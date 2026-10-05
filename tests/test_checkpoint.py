"""执行检查点：逐次登记、累计约束重算、冻结快照、完成转复核记录。

覆盖：
1. 小图独立枚举对拍：随机小图 × 全部合法（污染源, 保护区）划分 × 多组
   随机累计目标（已关闭 / 保持开启互斥分配），把目标随机切成 1~3 批
   逐次追加，每一步都与一次性累计求解一致，最终与暴力枚举所有可切断
   集合的独立实现对拍；不可执行的追加被拒绝且进度不变；
2. 逐次关闭与交班：创建 → 追加 → 读取进度 → 按读到的修订继续追加 →
   完成，全程 HTTP；
3. 约束冲突与原子性：修订过期、重复/交叉登记、未知管段（含后来修订才
   新增的 ID）、保持开启导致不可执行、非法负载、写入失败——进度与
   上一次完整复核结果都不变；
4. 冻结快照：方案修订、采用被另一计算替换后，已有检查点仍按创建时
   冻结的方案费用推进；新创建的检查点绑定新的采用；旧的一次性复核
   接口行为不变；
5. 完成：当前检查点转成可按 ID 读取的不可变复核记录，检查点转为
   COMPLETED 且不再接受追加/重复完成；新会话（重启后）读取一致。
"""

import random
from collections import deque

import pytest

from app import services
from app.db import SessionLocal
from app.errors import ApiError
from app.flow import solve_residual_min_cut
from app.models import Checkpoint, Review
from app.validation import validate_plan_payload

VALID_PLAN = {
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": [
        {"id": "p1", "from": "SRC1", "to": "MID", "cost": 4},
        {"id": "p2", "from": "SRC2", "to": "MID", "cost": 6},
        {"id": "p3", "from": "MID", "to": "SAFE1", "cost": 5},
        {"id": "p4", "from": "MID", "to": "SAFE2", "cost": 7},
    ],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"],
}
# 修订后：p4 费用 7 -> 1
REVISED_PLAN = {
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": [
        {"id": "p1", "from": "SRC1", "to": "MID", "cost": 4},
        {"id": "p2", "from": "SRC2", "to": "MID", "cost": 6},
        {"id": "p3", "from": "MID", "to": "SAFE1", "cost": 5},
        {"id": "p4", "from": "MID", "to": "SAFE2", "cost": 1},
    ],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"],
}
# 再修订一版：新增 p9（冻结版本之外的管段，追加不得引用）
EXTENDED_PLAN = {
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": VALID_PLAN["segments"]
    + [{"id": "p9", "from": "MID", "to": "SAFE2", "cost": 1}],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"],
}


# ---------- HTTP 辅助 ----------

def put(client, pid, plan):
    return client.put(f"/plans/{pid}", json=plan)


def compute(client, pid):
    return client.post(f"/plans/{pid}/computations").json()


def adopt(client, pid, cid):
    return client.post(f"/plans/{pid}/adopt", json={"computation_id": cid})


def adopt_plan(client, pid, plan=VALID_PLAN):
    put(client, pid, plan)
    cid = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid).status_code == 200
    return cid


def create_cp(client, pid):
    resp = client.post(f"/plans/{pid}/checkpoints")
    assert resp.status_code == 200
    return resp.json()


def get_cp(client, pid, cpid):
    return client.get(f"/plans/{pid}/checkpoints/{cpid}")


def append(client, pid, cpid, expected_revision, closed=None, keep_open=None):
    body = {"expected_revision": expected_revision}
    if closed is not None:
        body["closed_segments"] = closed
    if keep_open is not None:
        body["keep_open_segments"] = keep_open
    return client.post(f"/plans/{pid}/checkpoints/{cpid}/appends", json=body)


def complete(client, pid, cpid):
    return client.post(f"/plans/{pid}/checkpoints/{cpid}/complete")


def count_checkpoints(plan_id):
    db = SessionLocal()
    try:
        return db.query(Checkpoint).filter(Checkpoint.plan_id == plan_id).count()
    finally:
        db.close()


def count_reviews(plan_id):
    db = SessionLocal()
    try:
        return db.query(Review).filter(Review.plan_id == plan_id).count()
    finally:
        db.close()


def cut_isolates_sources(plan, cut_ids):
    """删除 cut_ids 管段后，任何污染源都不应能到达任何保护区。"""
    cut = set(cut_ids)
    adjacency = {}
    for seg in plan["segments"]:
        if seg["id"] in cut:
            continue
        adjacency.setdefault(seg["from"], []).append(seg["to"])
    seen = set(plan["sources"])
    queue = deque(plan["sources"])
    while queue:
        node = queue.popleft()
        for nxt in adjacency.get(node, []):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return seen.isdisjoint(plan["protections"])


# ---------- 1. 小图独立枚举对拍：逐次追加 == 一次性累计 == 暴力枚举 ----------

def brute_force_constrained_residual(plan, closed, keep_open):
    """枚举所有可切断管子集，独立求带"必须保持开启"约束的最优复核。

    与 test_review.py 中的独立实现一致：已关闭边先删除；保持开启边不允许
    放入切断集合；在全部最低费用可行集合的可达源侧交集上取唯一边界。
    """
    closed, keep_open = set(closed), set(keep_open)
    assert not closed & keep_open
    by_id = {seg["id"]: seg for seg in plan["segments"]}
    candidate_ids = [
        seg["id"]
        for seg in plan["segments"]
        if seg["id"] not in closed and seg["id"] not in keep_open
    ]

    def reachable_after_removal(removed):
        adjacency = {}
        for seg in plan["segments"]:
            if seg["id"] in removed:
                continue
            adjacency.setdefault(seg["from"], []).append(seg["to"])
        seen = set(plan["sources"])
        queue = deque(plan["sources"])
        while queue:
            node = queue.popleft()
            for nxt in adjacency.get(node, []):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return seen

    feasible = []
    best_cost = None
    for mask in range(1 << len(candidate_ids)):
        additional = {
            seg_id for i, seg_id in enumerate(candidate_ids) if mask & (1 << i)
        }
        reachable = reachable_after_removal(closed | additional)
        if reachable.isdisjoint(plan["protections"]):
            cost = sum(by_id[seg_id]["cost"] for seg_id in additional)
            feasible.append((additional, cost, reachable))
            if best_cost is None or cost < best_cost:
                best_cost = cost

    if best_cost is None:
        return None

    intersection_reachable = None
    for additional, cost, reachable in feasible:
        if cost != best_cost:
            continue
        intersection_reachable = (
            set(reachable)
            if intersection_reachable is None
            else intersection_reachable & reachable
        )

    side = sorted(zone for zone in plan["zones"] if zone in intersection_reachable)
    additional = sorted(
        seg["id"]
        for seg in plan["segments"]
        if seg["id"] in candidate_ids
        and seg["from"] in intersection_reachable
        and seg["to"] not in intersection_reachable
    )
    return side, additional, best_cost


def all_legal_partitions(zones):
    n = len(zones)
    full = (1 << n) - 1
    for src_mask in range(1, 1 << n):
        complement = full ^ src_mask
        sub = complement
        while sub:
            sources = [zones[i] for i in range(n) if src_mask >> i & 1]
            protections = [zones[i] for i in range(n) if sub >> i & 1]
            yield sources, protections
            sub = (sub - 1) & complement


def random_segments(rng, n):
    count = 2 * n + rng.randint(0, n)
    segments = []
    for i in range(count):
        frm, to = rng.randrange(n), rng.randrange(n)  # 允许自环与平行管段
        roll = rng.random()
        if roll < 0.2:
            cost = 0  # 零费用管段
        elif roll < 0.9:
            cost = rng.randint(1, 8)
        else:
            cost = 10**9
        segments.append({"id": f"e{i}", "from": f"Z{frm}", "to": f"Z{to}", "cost": cost})
    return segments


def split_into_batches(rng, closed_target, keep_target):
    """把累计目标随机切成 1~3 批（允许空批，覆盖空追加）。"""
    items = [(s, "closed") for s in closed_target]
    items += [(s, "keep") for s in keep_target]
    rng.shuffle(items)
    count = rng.randint(1, 3)
    batches = [(set(), set()) for _ in range(count)]
    for seg_id, kind in items:
        idx = rng.randrange(count)
        (batches[idx][0] if kind == "closed" else batches[idx][1]).add(seg_id)
    return batches


@pytest.mark.parametrize("seed", [0xC9E1, 0xC9E2])
def test_progressive_appends_match_one_shot_and_brute_force(seed):
    rng = random.Random(seed)
    n = 4
    zones = [f"Z{i}" for i in range(n)]
    segments = random_segments(rng, n)
    cost_by_id = {seg["id"]: seg["cost"] for seg in segments}
    seg_ids = [seg["id"] for seg in segments]

    for sources, protections in all_legal_partitions(zones):
        plan = {
            "zones": zones,
            "segments": segments,
            "sources": sources,
            "protections": protections,
        }
        canonical = validate_plan_payload(plan)
        for _ in range(4):
            # 随机互斥的累计目标
            closed_target, keep_target = set(), set()
            for seg_id in seg_ids:
                roll = rng.random()
                if roll < 0.35:
                    closed_target.add(seg_id)
                elif roll < 0.55:
                    keep_target.add(seg_id)
            batches = split_into_batches(rng, closed_target, keep_target)

            db = SessionLocal()
            try:
                services.save_plan(db, "cp-enum", canonical)
                computation = services.compute(db, "cp-enum")
                services.adopt(db, "cp-enum", computation.computation_id)
                cp = services.create_checkpoint(db, "cp-enum")
                cpid = cp.checkpoint_id

                revision = 1
                done_closed, done_keep = set(), set()
                failed_batch = None
                for batch_closed, batch_keep in batches:
                    try:
                        cp = services.append_checkpoint(
                            db, "cp-enum", cpid, revision,
                            sorted(batch_closed), sorted(batch_keep),
                        )
                    except ApiError as exc:
                        # 累计约束不可执行：明确拒绝且进度不变
                        assert exc.status_code == 422
                        assert exc.code == "REVIEW_NOT_EXECUTABLE"
                        failed_batch = (batch_closed, batch_keep)
                        break
                    assert cp.revision == revision + 1
                    revision = cp.revision
                    done_closed |= batch_closed
                    done_keep |= batch_keep
                    # 每一步都与一次性累计求解逐项一致
                    step = solve_residual_min_cut(plan, done_closed, done_keep)
                    assert cp.closed_segments == sorted(done_closed)
                    assert cp.keep_open_segments == sorted(done_keep)
                    assert cp.outcome["additional_segments"] == step[
                        "additional_segments"
                    ]
                    assert cp.outcome["additional_cost"] == step["additional_cost"]
                    assert cp.outcome["witness"] == step["witness"]

                if failed_batch is not None:
                    # 不可执行：进度停在上一修订，累计集合与复核结果不变
                    stuck = services.get_checkpoint_or_404(db, "cp-enum", cpid)
                    assert stuck.revision == revision
                    assert stuck.closed_segments == sorted(done_closed)
                    assert stuck.keep_open_segments == sorted(done_keep)
                    last = solve_residual_min_cut(plan, done_closed, done_keep)
                    assert stuck.outcome["additional_segments"] == last[
                        "additional_segments"
                    ]
                    assert stuck.outcome["witness"] == last["witness"]
                    # 独立确认该批累计目标确实不可执行
                    bad_closed = done_closed | failed_batch[0]
                    bad_keep = done_keep | failed_batch[1]
                    assert brute_force_constrained_residual(
                        plan, bad_closed, bad_keep
                    ) is None
                    continue

                # 最终状态与独立暴力枚举对拍
                expected = brute_force_constrained_residual(
                    plan, done_closed, done_keep
                )
                assert expected is not None
                side, additional, best_cost = expected
                assert cp.outcome["additional_segments"] == additional
                assert cp.outcome["additional_cost"] == best_cost
                assert cp.outcome["witness"]["source_zones"] == side
                merged = sorted(done_closed | set(additional))
                assert cp.outcome["witness"]["cut_segments"] == merged
                assert cp.outcome["witness"]["total_cost"] == sum(
                    cost_by_id[seg_id] for seg_id in merged
                )
                assert cut_isolates_sources(plan, merged)
            finally:
                db.close()


# ---------- 2. 逐次关闭与交班（HTTP 全流程） ----------

def test_checkpoint_progressive_flow_and_handover(client):
    pid = "cp-flow"
    cid = adopt_plan(client, pid)

    # 创建：绑定当前采用，初始进度为空，初始复核即冻结方案的最小隔断
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    assert cp["plan_id"] == pid
    assert cp["revision"] == 1
    assert cp["status"] == "OPEN"
    assert cp["plan_revision"] == 1
    assert cp["computation_id"] == cid
    assert cp["closed_segments"] == []
    assert cp["keep_open_segments"] == []
    assert cp["additional_segments"] == ["p1", "p2"]
    assert cp["additional_cost"] == 10
    assert cp["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }
    assert cp["review_id"] is None
    assert cp["created_at"] and cp["updated_at"]

    # 第一班：关闭 p1（剩余网络中 {p2}=6 远优于 {p3,p4}=12）
    resp = append(client, pid, cpid, 1, closed=["p1"])
    assert resp.status_code == 200
    progress = resp.json()
    assert progress["revision"] == 2
    assert progress["closed_segments"] == ["p1"]
    assert progress["keep_open_segments"] == []
    assert progress["additional_segments"] == ["p2"]
    assert progress["additional_cost"] == 6
    assert progress["witness"]["cut_segments"] == ["p1", "p2"]
    assert progress["witness"]["total_cost"] == 10

    # 交班：下一班先读取进度，再按读到的修订继续登记（p3 必须保持开启）
    handed_over = get_cp(client, pid, cpid)
    assert handed_over.status_code == 200
    assert handed_over.json() == progress
    resp = append(client, pid, cpid, handed_over.json()["revision"], keep_open=["p3"])
    assert resp.status_code == 200
    progress = resp.json()
    assert progress["revision"] == 3
    assert progress["closed_segments"] == ["p1"]
    assert progress["keep_open_segments"] == ["p3"]
    assert progress["additional_segments"] == ["p2"]
    assert progress["additional_cost"] == 6
    assert "p3" not in progress["witness"]["cut_segments"]

    # 再次交班后关闭 p2：现场已隔断，新增清单为空
    revision = get_cp(client, pid, cpid).json()["revision"]
    resp = append(client, pid, cpid, revision, closed=["p2"])
    assert resp.status_code == 200
    progress = resp.json()
    assert progress["revision"] == 4
    assert progress["closed_segments"] == ["p1", "p2"]
    assert progress["additional_segments"] == []
    assert progress["additional_cost"] == 0
    assert progress["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }
    assert cut_isolates_sources(VALID_PLAN, progress["witness"]["cut_segments"])
    assert count_checkpoints(pid) == 1


def test_checkpoint_created_from_current_adoption_binds_frozen_version(client):
    pid = "cp-create"
    cid = adopt_plan(client, pid)
    cp = create_cp(client, pid)
    assert cp["computation_id"] == cid
    assert cp["plan_revision"] == 1

    # 同一采用可创建多个检查点，各自独立推进
    cp2 = create_cp(client, pid)
    assert cp2["checkpoint_id"] != cp["checkpoint_id"]
    assert cp2["revision"] == 1
    resp = append(client, pid, cp["checkpoint_id"], 1, closed=["p1"])
    assert resp.status_code == 200
    assert get_cp(client, pid, cp2["checkpoint_id"]).json()["revision"] == 1
    assert count_checkpoints(pid) == 2


def test_empty_append_only_bumps_revision(client):
    """空追加（不登记任何管段）：修订推进，累计约束与复核结果不变。"""
    pid = "cp-empty-append"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    resp = client.post(
        f"/plans/{pid}/checkpoints/{cp['checkpoint_id']}/appends",
        json={"expected_revision": 1},
    )
    assert resp.status_code == 200
    progress = resp.json()
    assert progress["revision"] == 2
    assert progress["closed_segments"] == []
    assert progress["keep_open_segments"] == []
    assert progress["additional_segments"] == cp["additional_segments"]
    assert progress["witness"] == cp["witness"]


# ---------- 3. 约束冲突与原子性 ----------

def test_append_stale_revision_conflict_leaves_progress_unchanged(client):
    pid = "cp-stale"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    before = append(client, pid, cpid, 1, closed=["p1"]).json()

    resp = append(client, pid, cpid, 1, closed=["p2"])  # 过期修订
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "REVISION_CONFLICT"
    assert {d["code"] for d in error["details"]} == {"REVISION_MISMATCH"}
    assert {d["field"] for d in error["details"]} == {"expected_revision"}

    # 进度与上一次完整复核结果都不变
    assert get_cp(client, pid, cpid).json() == before

    # 重新读取后按当前修订提交：冲突可恢复
    resp = append(client, pid, cpid, before["revision"], closed=["p2"])
    assert resp.status_code == 200
    assert resp.json()["revision"] == before["revision"] + 1
    assert resp.json()["closed_segments"] == ["p1", "p2"]


def test_append_future_revision_is_also_conflict(client):
    pid = "cp-future-rev"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    resp = append(client, pid, cp["checkpoint_id"], 5, closed=["p1"])
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "REVISION_CONFLICT"
    assert get_cp(client, pid, cp["checkpoint_id"]).json() == cp


def test_append_duplicate_and_cross_registration_rejected(client):
    pid = "cp-dup-cross"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    append(client, pid, cpid, 1, closed=["p1"])
    append(client, pid, cpid, 2, keep_open=["p3"])
    before = get_cp(client, pid, cpid).json()
    assert before["revision"] == 3

    # 重复登记已关闭管段
    resp = append(client, pid, cpid, 3, closed=["p1"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"DUPLICATE_SEGMENT_ID"}
    # 重复登记保持开启管段
    resp = append(client, pid, cpid, 3, keep_open=["p3"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"DUPLICATE_SEGMENT_ID"}
    # 交叉：关闭已要求保持开启的管段
    resp = append(client, pid, cpid, 3, closed=["p3"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"CLOSED_KEEP_OPEN_OVERLAP"}
    # 交叉：要求保持开启已关闭的管段
    resp = append(client, pid, cpid, 3, keep_open=["p1"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"CLOSED_KEEP_OPEN_OVERLAP"}

    # 所有冲突都不改变进度与上一次完整复核结果
    assert get_cp(client, pid, cpid).json() == before


def test_append_unknown_segment_rejected_including_later_revisions(client):
    pid = "cp-unknown-seg"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]

    resp = append(client, pid, cpid, 1, closed=["nope"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"UNKNOWN_SEGMENT"}

    # 方案修订新增 p9：仍不属于检查点创建时冻结的方案
    assert put(client, pid, EXTENDED_PLAN).json()["revision"] == 2
    resp = append(client, pid, cpid, 1, closed=["p9"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"UNKNOWN_SEGMENT"}
    resp = append(client, pid, cpid, 1, keep_open=["p9"])
    assert resp.status_code == 422
    assert {
        d["code"] for d in resp.json()["error"]["details"]
    } == {"UNKNOWN_SEGMENT"}

    assert get_cp(client, pid, cpid).json() == cp


def test_append_infeasible_keep_open_leaves_progress_unchanged(client):
    """保持开启约束保留了污染源到保护区的路径：不可执行，进度不变。"""
    pid = "cp-infeasible"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    before = append(client, pid, cpid, 1, keep_open=["p1"]).json()
    assert before["revision"] == 2

    # p1 + p3 保持开启：SRC1 -> MID -> SAFE1 路径无法切断
    resp = append(client, pid, cpid, 2, keep_open=["p3"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "REVIEW_NOT_EXECUTABLE"
    assert {d["code"] for d in error["details"]} == {"REQUIRED_OPEN_PATH"}

    # 进度与上一次完整复核结果都不变
    assert get_cp(client, pid, cpid).json() == before
    # 换一批可执行的管段仍可继续推进
    resp = append(client, pid, cpid, 2, closed=["p2"])
    assert resp.status_code == 200
    assert resp.json()["revision"] == 3


INVALID_APPEND_BODIES = [
    ([], "INVALID_BODY"),
    ("nope", "INVALID_BODY"),
    ({}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": None}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": 0}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": -2}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": "1"}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": True}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": 1, "closed_segments": "p1"}, "INVALID_CLOSED_SEGMENTS_FIELD"),
    ({"expected_revision": 1, "closed_segments": None}, "INVALID_CLOSED_SEGMENTS_FIELD"),
    ({"expected_revision": 1, "closed_segments": [1]}, "INVALID_SEGMENT_ID"),
    ({"expected_revision": 1, "closed_segments": ["bad id"]}, "INVALID_SEGMENT_ID"),
    (
        {"expected_revision": 1, "closed_segments": ["p1", "p1"]},
        "DUPLICATE_SEGMENT_ID",
    ),
    (
        {"expected_revision": 1, "keep_open_segments": "p1"},
        "INVALID_KEEP_OPEN_SEGMENTS_FIELD",
    ),
    ({"expected_revision": 1, "keep_open_segments": [1]}, "INVALID_SEGMENT_ID"),
    (
        {"expected_revision": 1, "keep_open_segments": ["p3", "p3"]},
        "DUPLICATE_SEGMENT_ID",
    ),
    (
        {"expected_revision": 1, "closed_segments": ["p1"], "keep_open_segments": ["p1"]},
        "CLOSED_KEEP_OPEN_OVERLAP",
    ),
]


@pytest.mark.parametrize("body, code", INVALID_APPEND_BODIES)
def test_append_invalid_payload_rejected_without_progress(client, body, code):
    pid = "cp-invalid-append"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    resp = client.post(f"/plans/{pid}/checkpoints/{cp['checkpoint_id']}/appends",
                       json=body)
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert code in {d["code"] for d in error["details"]}
    assert get_cp(client, pid, cp["checkpoint_id"]).json() == cp


def test_append_malformed_json(client):
    pid = "cp-bad-json"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    resp = client.post(
        f"/plans/{pid}/checkpoints/{cp['checkpoint_id']}/appends",
        content=b"{broken",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_JSON"
    assert get_cp(client, pid, cp["checkpoint_id"]).json() == cp


def test_create_checkpoint_requires_adoption(client):
    pid = "cp-no-adoption"
    put(client, pid, VALID_PLAN)
    resp = client.post(f"/plans/{pid}/checkpoints")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "ADOPTION_NOT_FOUND"
    assert count_checkpoints(pid) == 0


def test_checkpoint_unknown_plan(client):
    resp = client.post("/plans/ghost/checkpoints")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"
    resp = client.get("/plans/ghost/checkpoints/anything")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"
    resp = client.post(
        "/plans/ghost/checkpoints/anything/appends", json={"expected_revision": 1}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"
    resp = client.post("/plans/ghost/checkpoints/anything/complete")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"


def test_checkpoint_not_found_and_cross_plan_isolation(client):
    adopt_plan(client, "cp-plan-a")
    adopt_plan(client, "cp-plan-b")
    cp = create_cp(client, "cp-plan-a")

    resp = get_cp(client, "cp-plan-a", "missing123")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    # 属于其他方案的检查点：404
    resp = get_cp(client, "cp-plan-b", cp["checkpoint_id"])
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    resp = append(client, "cp-plan-b", cp["checkpoint_id"], 1, closed=["p1"])
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    resp = complete(client, "cp-plan-b", cp["checkpoint_id"])
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    # 属主方案操作正常
    assert get_cp(client, "cp-plan-a", cp["checkpoint_id"]).status_code == 200


def test_db_write_failure_leaves_checkpoint_unchanged(client):
    """数据库写入失败：整体回滚并报 500，进度与上次复核结果都不变。"""
    pid = "cp-db-failure"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]

    db = SessionLocal()
    original_commit = db.commit

    def failing_commit(*args, **kwargs):
        raise RuntimeError("simulated database failure")

    db.commit = failing_commit
    try:
        with pytest.raises(ApiError) as excinfo:
            services.append_checkpoint(db, pid, cpid, 1, ["p1"], [])
        assert excinfo.value.status_code == 500
        assert excinfo.value.code == "INTERNAL_ERROR"
    finally:
        db.commit = original_commit
        db.close()

    # 进度与上一次完整复核结果都不变；随后可正常重试
    assert get_cp(client, pid, cpid).json() == cp
    resp = append(client, pid, cpid, 1, closed=["p1"])
    assert resp.status_code == 200
    assert resp.json()["revision"] == 2

    # 完成时写入失败：不留复核记录，检查点保持 OPEN
    db = SessionLocal()
    original_commit = db.commit
    db.commit = failing_commit
    try:
        with pytest.raises(ApiError) as excinfo:
            services.complete_checkpoint(db, pid, cpid)
        assert excinfo.value.status_code == 500
        assert excinfo.value.code == "INTERNAL_ERROR"
    finally:
        db.commit = original_commit
        db.close()

    assert count_reviews(pid) == 0
    progress = get_cp(client, pid, cpid).json()
    assert progress["status"] == "OPEN"
    assert progress["review_id"] is None
    assert complete(client, pid, cpid).status_code == 200
    assert count_reviews(pid) == 1


# ---------- 4. 冻结快照：方案修订与采用切换 ----------

def test_checkpoint_advances_on_frozen_snapshot_after_edit_and_adoption_switch(client):
    pid = "cp-frozen"
    cid1 = adopt_plan(client, pid)  # 第 1 版计算并采用
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]

    # 修订方案（p4 费用 7 -> 1）并采用另一计算
    assert put(client, pid, REVISED_PLAN).json()["revision"] == 2
    cid2 = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid2).status_code == 200
    assert client.get(f"/plans/{pid}/adoption").json()["computation_id"] == cid2

    # 已有检查点仍按创建时冻结的第 1 版推进：p4 费用仍是 7
    resp = append(client, pid, cpid, 1, closed=["p3"])
    assert resp.status_code == 200
    progress = resp.json()
    assert progress["plan_revision"] == 1
    assert progress["computation_id"] == cid1
    assert progress["additional_segments"] == ["p4"]
    assert progress["additional_cost"] == 7  # 若混入修订版费用会是 1
    assert progress["witness"]["total_cost"] == 12

    # 采用切换后新创建的检查点绑定新的采用（第 2 版）
    cp2 = create_cp(client, pid)
    assert cp2["plan_revision"] == 2
    assert cp2["computation_id"] == cid2
    resp = append(client, pid, cp2["checkpoint_id"], 1, closed=["p3"])
    assert resp.json()["additional_cost"] == 1

    # 两个检查点互不影响
    assert get_cp(client, pid, cpid).json()["revision"] == 2
    assert count_checkpoints(pid) == 2


def test_one_shot_review_interface_unchanged_by_checkpoints(client):
    """旧的一次性复核接口行为不变，且不受检查点影响。"""
    pid = "cp-legacy-review"
    cid = adopt_plan(client, pid)
    cp = create_cp(client, pid)
    append(client, pid, cp["checkpoint_id"], 1, closed=["p1"])

    # 一次性复核仍针对当前已采用结果，与检查点进度无关
    resp = client.post(f"/plans/{pid}/reviews", json={"closed_segments": ["p3"]})
    assert resp.status_code == 200
    record = resp.json()
    assert record["computation_id"] == cid
    assert record["closed_segments"] == ["p3"]
    assert record["additional_segments"] == ["p4"]
    assert "checkpoint_id" not in record
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record

    # 检查点进度不受一次性复核影响
    progress = get_cp(client, pid, cp["checkpoint_id"]).json()
    assert progress["revision"] == 2
    assert progress["closed_segments"] == ["p1"]
    assert count_reviews(pid) == 1


# ---------- 5. 完成：不可变复核记录与重启读取 ----------

def test_complete_freezes_immutable_review_record(client):
    pid = "cp-complete"
    cid = adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    append(client, pid, cpid, 1, closed=["p1"])
    append(client, pid, cpid, 2, keep_open=["p3"])
    progress = append(client, pid, cpid, 3, closed=["p2"]).json()

    plan_before = client.get(f"/plans/{pid}").json()
    adoption_before = client.get(f"/plans/{pid}/adoption").json()
    computation_before = client.get(f"/plans/{pid}/computations/{cid}").json()

    resp = complete(client, pid, cpid)
    assert resp.status_code == 200
    record = resp.json()
    assert record["review_id"]
    assert record["plan_id"] == pid
    assert record["plan_revision"] == 1
    assert record["computation_id"] == cid
    assert record["checkpoint_id"] == cpid
    assert record["closed_segments"] == ["p1", "p2"]
    assert record["keep_open_segments"] == ["p3"]
    assert record["additional_segments"] == []
    assert record["additional_cost"] == 0
    assert record["witness"] == progress["witness"]
    assert cut_isolates_sources(VALID_PLAN, record["witness"]["cut_segments"])

    # 复核记录可按 ID 读取（旧接口），与完成响应逐项一致
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record

    # 检查点转为 COMPLETED 并指向该复核记录，进度冻结在完成时状态
    done = get_cp(client, pid, cpid).json()
    assert done["status"] == "COMPLETED"
    assert done["review_id"] == record["review_id"]
    assert done["revision"] == progress["revision"]
    assert done["closed_segments"] == progress["closed_segments"]
    assert done["witness"] == progress["witness"]

    # 完成不修改方案、计算记录或采用快照
    assert client.get(f"/plans/{pid}").json() == plan_before
    assert client.get(f"/plans/{pid}/adoption").json() == adoption_before
    assert client.get(f"/plans/{pid}/computations/{cid}").json() == computation_before
    assert count_reviews(pid) == 1


def test_complete_is_terminal(client):
    pid = "cp-terminal"
    adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    record = complete(client, pid, cpid).json()
    done = get_cp(client, pid, cpid).json()

    # 完成后不再接受追加
    resp = append(client, pid, cpid, done["revision"], closed=["p1"])
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "CHECKPOINT_ALREADY_COMPLETED"
    # 也不能重复完成
    resp = complete(client, pid, cpid)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "CHECKPOINT_ALREADY_COMPLETED"

    # 状态与复核记录都保持不变
    assert get_cp(client, pid, cpid).json() == done
    assert count_reviews(pid) == 1
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.json() == record


def test_checkpoint_state_and_completion_survive_restart(client):
    """新会话（模拟重启）读取：进度、冻结版本与复核记录逐项一致。"""
    pid = "cp-restart"
    cid = adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    append(client, pid, cpid, 1, closed=["p1"])
    progress = append(client, pid, cpid, 2, keep_open=["p3"]).json()

    # 全新会话读取检查点：与最后响应逐项一致
    db = SessionLocal()
    try:
        row = services.get_checkpoint_or_404(db, pid, cpid)
        assert row.revision == progress["revision"]
        assert row.closed_segments == progress["closed_segments"]
        assert row.keep_open_segments == progress["keep_open_segments"]
        assert row.outcome["additional_segments"] == progress["additional_segments"]
        assert row.outcome["additional_cost"] == progress["additional_cost"]
        assert row.outcome["witness"] == progress["witness"]
        assert row.computation_id == cid
        assert row.plan_revision == 1
        assert row.snapshot["plan"] == VALID_PLAN
    finally:
        db.close()

    # 全新会话完成并读取复核记录
    db = SessionLocal()
    try:
        review = services.complete_checkpoint(db, pid, cpid)
        review_id = review.review_id
    finally:
        db.close()
    got = client.get(f"/plans/{pid}/reviews/{review_id}")
    assert got.status_code == 200
    record = got.json()
    assert record["checkpoint_id"] == cpid
    assert record["closed_segments"] == ["p1"]
    assert record["keep_open_segments"] == ["p3"]
    assert record["witness"] == progress["witness"]

    # 再开新会话：复核记录与检查点终态依然可读
    db = SessionLocal()
    try:
        assert services.get_review_or_404(db, pid, review_id).record == record
        done = services.get_checkpoint_or_404(db, pid, cpid)
        assert done.status == "COMPLETED"
        assert done.review_id == review_id
    finally:
        db.close()


def test_completed_review_survives_adoption_replacement(client):
    """完成后采用被替换：复核记录仍按 ID 可读，检查点终态不变。"""
    pid = "cp-complete-then-switch"
    cid1 = adopt_plan(client, pid)
    cp = create_cp(client, pid)
    cpid = cp["checkpoint_id"]
    append(client, pid, cpid, 1, closed=["p3"])
    record = complete(client, pid, cpid).json()
    assert record["additional_cost"] == 7  # 第 1 版冻结费用

    put(client, pid, REVISED_PLAN)
    cid2 = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid2).status_code == 200

    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record
    assert got.json()["computation_id"] == cid1
    done = get_cp(client, pid, cpid).json()
    assert done["status"] == "COMPLETED"
    assert done["computation_id"] == cid1
    assert done["additional_cost"] == 7
