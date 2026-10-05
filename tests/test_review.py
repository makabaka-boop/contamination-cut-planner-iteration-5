"""已采用结果的现场关闭复核：冻结版本、追加隔断、记录持久化与并发读取。

覆盖：
1. 小图独立枚举对拍：随机小图 × 全部合法（污染源, 保护区）划分 ×
   多组现场已关闭子集，并额外枚举每条边“已关闭 / 必须保持开启 /
   可追加切断”的全部状态组合，与暴力枚举所有可选切断集合的结果对拍，
   并验证合并后的隔断见证确实切断冻结方案中的全部污染路径；
2. 非建议管段先被关闭：复核在剩余网络上重新求最优，而不是机械补齐
   原建议清单；
3. 零费用边、必须保持开启边与“若已隔断则新增清单为空”；
4. 保持开启约束使方案不可执行时，返回不可执行且不写记录；
5. 方案修订后复核：只能引用采用时冻结的方案版本（后来修订的同名
   管段费用不得混入），且复核不修改原方案、计算记录与采用快照；
6. 复核记录冻结两类输入与结果，按 ID 在新会话（重启后）读取一致，
   多线程并发读取也一致；
7. 未知/重复管段、两类约束重叠、无采用结果、数据库写入失败都不留下
   半条复核记录。
"""

import random
import threading
from collections import deque

import pytest

from app import services
from app.db import SessionLocal
from app.errors import ApiError
from app.flow import ResidualCutInfeasible, solve_residual_min_cut
from app.models import Review

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
# 再修订一版：新增 p9（冻结版本之外的管段，复核不得引用）
EXTENDED_PLAN = {
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": VALID_PLAN["segments"]
    + [{"id": "p9", "from": "MID", "to": "SAFE2", "cost": 1}],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"],
}

ZERO_PLAN = {
    "zones": ["S", "A", "T"],
    "segments": [
        {"id": "z", "from": "S", "to": "A", "cost": 0},
        {"id": "w", "from": "A", "to": "T", "cost": 5},
    ],
    "sources": ["S"],
    "protections": ["T"],
}


# ---------- HTTP 辅助 ----------

def put(client, pid, plan):
    return client.put(f"/plans/{pid}", json=plan)


def compute(client, pid):
    return client.post(f"/plans/{pid}/computations").json()


def adopt(client, pid, cid):
    return client.post(f"/plans/{pid}/adopt", json={"computation_id": cid})


def review(client, pid, closed, keep_open=None):
    body = {"closed_segments": closed}
    if keep_open is not None:
        body["keep_open_segments"] = keep_open
    return client.post(f"/plans/{pid}/reviews", json=body)


def adopt_plan(client, pid, plan=VALID_PLAN):
    put(client, pid, plan)
    cid = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid).status_code == 200
    return cid


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


# ---------- 1. 独立枚举对拍 ----------

def brute_force_residual(plan, closed):
    """独立暴力枚举：移除 closed 后的剩余网络上枚举全部源侧集合。

    在所有最低费用割中取所有最优掩码的交集（最小源侧），返回
    (升序源侧, 升序新增切断管段, 最低追加费用)。
    """
    zones = plan["zones"]
    remaining = [seg for seg in plan["segments"] if seg["id"] not in closed]
    n = len(zones)
    index = {zone: i for i, zone in enumerate(zones)}
    src_mask = 0
    for zone in plan["sources"]:
        src_mask |= 1 << index[zone]
    prot_mask = 0
    for zone in plan["protections"]:
        prot_mask |= 1 << index[zone]

    best_cost = None
    intersection = 0
    for mask in range(1 << n):
        if mask & src_mask != src_mask or mask & prot_mask:
            continue

        def on_source(zone):
            return bool(mask & (1 << index[zone]))

        cost = sum(
            seg["cost"]
            for seg in remaining
            if on_source(seg["from"]) and not on_source(seg["to"])
        )
        if best_cost is None or cost < best_cost:
            best_cost, intersection = cost, mask
        elif cost == best_cost:
            intersection &= mask
    assert best_cost is not None

    side = sorted(zone for zone in zones if intersection & (1 << index[zone]))
    additional = sorted(
        seg["id"]
        for seg in remaining
        if intersection & (1 << index[seg["from"]])
        and not intersection & (1 << index[seg["to"]])
    )
    return side, additional, best_cost


def brute_force_constrained_residual(plan, closed, keep_open):
    """枚举所有可切断管子集，独立求带“必须保持开启”约束的最优复核。

    已关闭边先删除；保持开启边不允许放入切断集合；其余每条边都可选关闭。
    用 BFS 枚举每个候选集合是否可隔断，并在全部最低费用可行集合的可达源侧
    交集上取唯一边界，复刻“最低追加费用 + 最小源侧 + 排序”的裁决。
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

    minimum_sets = [
        (additional, reachable)
        for additional, cost, reachable in feasible
        if cost == best_cost
    ]
    intersection_reachable = None
    for _, reachable in minimum_sets:
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


def closed_subsets(rng, segments):
    """空集、全集、全部单元素/双元素子集与若干随机子集。"""
    ids = [seg["id"] for seg in segments]
    subsets = [set(), set(ids)]
    subsets.extend({seg_id} for seg_id in ids)
    subsets.extend(
        {a, b} for i, a in enumerate(ids) for b in ids[i + 1:]
    )
    for _ in range(24):
        subsets.append({seg_id for seg_id in ids if rng.random() < 0.5})
    return subsets


@pytest.mark.parametrize("seed", [0x5E71E301, 0x5E71E302])
def test_residual_review_matches_brute_force(seed):
    rng = random.Random(seed)
    n = 4
    zones = [f"Z{i}" for i in range(n)]
    segments = random_segments(rng, n)
    cost_by_id = {seg["id"]: seg["cost"] for seg in segments}

    for sources, protections in all_legal_partitions(zones):
        plan = {
            "zones": zones,
            "segments": segments,
            "sources": sources,
            "protections": protections,
        }
        for closed in closed_subsets(rng, segments):
            outcome = solve_residual_min_cut(plan, closed)
            side, additional, best_cost = brute_force_residual(plan, closed)

            # 新增建议与追加费用等于独立枚举的最优解（源侧最小裁决一致）
            assert outcome["additional_segments"] == additional, (sources, closed)
            assert outcome["additional_cost"] == best_cost, (sources, closed)
            assert outcome["witness"]["source_zones"] == side, (sources, closed)
            assert outcome["closed_segments"] == sorted(closed)

            # 合并见证 = 现场已关闭 ∪ 新增建议，费用按（冻结）方案求和
            merged = sorted(set(closed) | set(additional))
            assert outcome["witness"]["cut_segments"] == merged, (sources, closed)
            assert outcome["witness"]["total_cost"] == sum(
                cost_by_id[seg_id] for seg_id in merged
            )
            # 新增清单与现场已关闭互不相交
            assert not set(additional) & closed
            # 合并清单确实切断冻结方案中的全部污染路径
            assert cut_isolates_sources(plan, merged)


def test_constrained_residual_review_matches_brute_force():
    """小图枚举每个“已关闭 / 保持开启 / 可追加切断”状态并与算法对拍。"""
    zones = ["Z0", "Z1", "Z2", "Z3"]
    segments = [
        {"id": "e0", "from": "Z0", "to": "Z1", "cost": 0},
        {"id": "e1", "from": "Z1", "to": "Z2", "cost": 3},
        {"id": "e2", "from": "Z2", "to": "Z3", "cost": 2},
        {"id": "e3", "from": "Z0", "to": "Z3", "cost": 5},
        {"id": "e4", "from": "Z3", "to": "Z1", "cost": 1},
    ]
    cost_by_id = {seg["id"]: seg["cost"] for seg in segments}
    states = ("remaining", "closed", "keep_open")

    for sources, protections in all_legal_partitions(zones):
        plan = {
            "zones": zones,
            "segments": segments,
            "sources": sources,
            "protections": protections,
        }
        for state_mask in range(len(states) ** len(segments)):
            closed, keep_open = set(), set()
            value = state_mask
            for i in range(len(segments)):
                state = states[value % len(states)]
                value //= len(states)
                if state == "closed":
                    closed.add(segments[i]["id"])
                elif state == "keep_open":
                    keep_open.add(segments[i]["id"])

            expected = brute_force_constrained_residual(plan, closed, keep_open)
            if expected is None:
                with pytest.raises(ResidualCutInfeasible):
                    solve_residual_min_cut(plan, closed, keep_open)
                continue

            side, additional, best_cost = expected
            outcome = solve_residual_min_cut(plan, closed, keep_open)
            assert outcome["closed_segments"] == sorted(closed)
            assert outcome["keep_open_segments"] == sorted(keep_open)
            assert outcome["additional_segments"] == additional, (
                sources,
                protections,
                closed,
                keep_open,
            )
            assert outcome["additional_cost"] == best_cost
            assert outcome["witness"]["source_zones"] == side
            merged = sorted(closed | set(additional))
            assert outcome["witness"]["cut_segments"] == merged
            assert outcome["witness"]["total_cost"] == sum(
                cost_by_id[seg_id] for seg_id in merged
            )
            assert not (set(additional) & (closed | keep_open))
            assert cut_isolates_sources(plan, merged)


# ---------- 2/3. 接口行为：新增建议、零费用、已隔断 ----------

def test_review_recommends_remaining_cut(client):
    pid = "review-basic"
    cid = adopt_plan(client, pid)
    # 现场先关闭 p1（费用 4）：剩余网络中 {p2}=6 远优于 {p3,p4}=12
    resp = review(client, pid, ["p1"])
    assert resp.status_code == 200
    record = resp.json()
    assert record["review_id"]
    assert record["created_at"]
    assert record["plan_id"] == pid
    assert record["plan_revision"] == 1
    assert record["computation_id"] == cid
    assert record["closed_segments"] == ["p1"]
    assert record["additional_segments"] == ["p2"]
    assert record["additional_cost"] == 6
    assert record["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }

    # 按 ID 在新会话读取：与创建响应逐项一致
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record
    assert count_reviews(pid) == 1


def test_review_when_non_recommended_segment_closed_first(client):
    """非建议管段先被关闭：在剩余网络上重新求最优，而非补齐原清单。

    冻结方案中原建议清单是 {p1,p2}；现场先关闭不在清单内的 p3 后，
    剩余网络中 {p4}=7 优于 {p1,p2}=10，新增建议必须是 p4。
    """
    pid = "review-non-recommended"
    adopt_plan(client, pid)
    record = review(client, pid, ["p3"]).json()
    assert record["closed_segments"] == ["p3"]
    assert record["additional_segments"] == ["p4"]
    assert record["additional_cost"] == 7
    assert record["witness"]["cut_segments"] == ["p3", "p4"]
    assert record["witness"]["total_cost"] == 12
    assert cut_isolates_sources(VALID_PLAN, record["witness"]["cut_segments"])


def test_review_without_any_closed_segment(client):
    """一个管段都没关：复核退化为原方案的最小隔断。"""
    pid = "review-empty"
    adopt_plan(client, pid)
    record = review(client, pid, []).json()
    assert record["closed_segments"] == []
    assert record["additional_segments"] == ["p1", "p2"]
    assert record["additional_cost"] == 10
    assert record["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }


def test_review_zero_cost_edge(client):
    pid = "review-zero"
    adopt_plan(client, pid, ZERO_PLAN)
    record = review(client, pid, []).json()
    assert record["additional_segments"] == ["z"]
    assert record["additional_cost"] == 0
    assert record["witness"]["cut_segments"] == ["z"]
    assert record["witness"]["total_cost"] == 0
    assert cut_isolates_sources(ZERO_PLAN, ["z"])


def test_review_already_isolated_gives_empty_additional(client):
    """已隔断时新增清单为空、追加费用为 0，见证只含现场已关闭管段。"""
    pid = "review-isolated"
    adopt_plan(client, pid)
    record = review(client, pid, ["p1", "p2"]).json()
    assert record["additional_segments"] == []
    assert record["additional_cost"] == 0
    assert record["witness"]["cut_segments"] == ["p1", "p2"]
    assert record["witness"]["total_cost"] == 10
    assert cut_isolates_sources(VALID_PLAN, ["p1", "p2"])

    # 零费用方案关闭唯一隔断管段后同样已隔断
    pid2 = "review-isolated-zero"
    adopt_plan(client, pid2, ZERO_PLAN)
    record = review(client, pid2, ["z"]).json()
    assert record["additional_segments"] == []
    assert record["additional_cost"] == 0
    assert record["witness"]["cut_segments"] == ["z"]
    assert record["witness"]["source_zones"] == ["S"]


def test_review_keep_open_forces_min_cost_additional_cut(client):
    """必须保持开启的边不可切断，算法只能在其余边中选最低追加隔断。"""
    pid = "review-keep-open"
    cid = adopt_plan(client, pid)
    resp = review(client, pid, [], keep_open=["p1"])
    assert resp.status_code == 200
    record = resp.json()
    assert record["computation_id"] == cid
    assert record["closed_segments"] == []
    assert record["keep_open_segments"] == ["p1"]
    assert record["additional_segments"] == ["p3", "p4"]
    assert record["additional_cost"] == 12
    assert record["witness"] == {
        "source_zones": ["MID", "SRC1", "SRC2"],
        "cut_segments": ["p3", "p4"],
        "total_cost": 12,
    }
    assert cut_isolates_sources(VALID_PLAN, record["witness"]["cut_segments"])
    assert "p1" not in record["witness"]["cut_segments"]


def test_review_keep_open_zero_cost_edge_picks_positive_alternative(client):
    pid = "review-keep-open-zero"
    adopt_plan(client, pid, ZERO_PLAN)
    resp = review(client, pid, [], keep_open=["z"])
    assert resp.status_code == 200
    record = resp.json()
    assert record["keep_open_segments"] == ["z"]
    assert record["additional_segments"] == ["w"]
    assert record["additional_cost"] == 5
    assert record["witness"] == {
        "source_zones": ["S", "A"],
        "cut_segments": ["w"],
        "total_cost": 5,
    }
    assert cut_isolates_sources(ZERO_PLAN, ["w"])


def test_review_keep_open_does_not_duplicate_already_isolated_constraint(client):
    pid = "review-keep-open-isolated"
    adopt_plan(client, pid)
    record = review(client, pid, ["p1", "p2"], keep_open=["p3"]).json()
    assert record["keep_open_segments"] == ["p3"]
    assert record["additional_segments"] == []
    assert record["additional_cost"] == 0
    assert record["witness"]["cut_segments"] == ["p1", "p2"]


def test_review_infeasible_when_required_open_path_remains(client):
    """保持开启约束保留源到保护区路径：明确不可执行，且不写复核记录。"""
    pid = "review-infeasible"
    adopt_plan(client, pid)
    resp = review(client, pid, [], keep_open=["p1", "p3"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "REVIEW_NOT_EXECUTABLE"
    assert {d["code"] for d in error["details"]} == {"REQUIRED_OPEN_PATH"}
    assert count_reviews(pid) == 0

    # 即使关闭其他路径，保持开启的路径仍存在时仍不可执行
    resp = review(client, pid, ["p2", "p4"], keep_open=["p1", "p3"])
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "REVIEW_NOT_EXECUTABLE"
    assert count_reviews(pid) == 0


# ---------- 4. 方案修订后复核：冻结版本 ----------

def test_review_after_revision_uses_frozen_plan_and_changes_nothing(client):
    pid = "review-frozen"
    cid = adopt_plan(client, pid)  # 第 1 版计算并采用
    # 修订方案：p4 费用 7 -> 1（同名管段的新费用不得混入复核）
    assert put(client, pid, REVISED_PLAN).json()["revision"] == 2

    adoption_before = client.get(f"/plans/{pid}/adoption").json()
    computation_before = client.get(
        f"/plans/{pid}/computations/{cid}"
    ).json()

    # 现场已关闭 p3：冻结第 1 版中 p4 费用仍是 7 → 追加 p4 费用为 7；
    # 若错误混入修订版费用，追加费用会是 1
    record = review(client, pid, ["p3"]).json()
    assert record["plan_revision"] == 1
    assert record["computation_id"] == cid
    assert record["additional_segments"] == ["p4"]
    assert record["additional_cost"] == 7
    assert record["witness"]["total_cost"] == 12
    assert cut_isolates_sources(VALID_PLAN, record["witness"]["cut_segments"])

    # 复核不得修改原方案、计算记录或采用快照
    current = client.get(f"/plans/{pid}").json()
    assert current["revision"] == 2
    assert current["plan"] == REVISED_PLAN
    assert client.get(f"/plans/{pid}/adoption").json() == adoption_before
    assert (
        client.get(f"/plans/{pid}/computations/{cid}").json()
        == computation_before
    )


def test_review_keep_open_after_revision_uses_frozen_costs(client):
    """保持开启约束下，追加边费用仍只能来自采用时冻结的版本。"""
    pid = "review-keep-open-frozen"
    cid = adopt_plan(client, pid)
    put(client, pid, REVISED_PLAN)  # p4: 7 -> 1

    record = review(client, pid, [], keep_open=["p1"]).json()
    assert record["plan_revision"] == 1
    assert record["computation_id"] == cid
    assert record["keep_open_segments"] == ["p1"]
    assert record["additional_segments"] == ["p3", "p4"]
    assert record["additional_cost"] == 12
    assert record["witness"]["total_cost"] == 12


def test_review_cannot_reference_segment_added_by_later_revision(client):
    """后来修订才新增的管段 p9 不属于冻结方案：未知管段，拒绝且不留记录。"""
    pid = "review-later-segment"
    adopt_plan(client, pid)
    put(client, pid, EXTENDED_PLAN)  # 第 2 版新增 p9
    resp = review(client, pid, ["p9"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert "UNKNOWN_SEGMENT" in {d["code"] for d in error["details"]}
    assert count_reviews(pid) == 0
    # 采用快照仍是第 1 版
    adoption = client.get(f"/plans/{pid}/adoption").json()
    assert adoption["plan_revision"] == 1
    assert all(seg["id"] != "p9" for seg in adoption["plan"]["segments"])


def test_review_history_survives_adoption_replacement(client):
    """采用被替换后，旧复核记录仍按 ID 可读；新复核针对新冻结版本，
    并列费用时沿用源侧最小裁决。"""
    pid = "review-history"
    cid1 = adopt_plan(client, pid)
    r1 = review(client, pid, ["p1"]).json()

    put(client, pid, REVISED_PLAN)
    cid2 = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid2).status_code == 200

    # 第 2 版关闭 p1：{p2}=6 与 {p3,p4}=6 并列，源侧最小取 {p2}
    r2 = review(client, pid, ["p1"]).json()
    assert r2["plan_revision"] == 2
    assert r2["computation_id"] == cid2
    assert r2["additional_segments"] == ["p2"]
    assert r2["additional_cost"] == 6
    assert r2["witness"]["cut_segments"] == ["p1", "p2"]

    # 旧记录冻结在第 1 版，仍然完整可读
    got1 = client.get(f"/plans/{pid}/reviews/{r1['review_id']}")
    assert got1.status_code == 200
    assert got1.json() == r1
    assert got1.json()["computation_id"] == cid1
    got2 = client.get(f"/plans/{pid}/reviews/{r2['review_id']}")
    assert got2.json() == r2
    assert count_reviews(pid) == 2


# ---------- 5. 并发读取 ----------

def test_concurrent_reads_return_identical_record(client):
    pid = "review-concurrent-read"
    cid = adopt_plan(client, pid)
    record = review(client, pid, ["p3"]).json()
    review_url = f"/plans/{pid}/reviews/{record['review_id']}"
    mismatches = []

    def reader():
        try:
            for _ in range(25):
                got = client.get(review_url)
                if got.status_code != 200 or got.json() != record:
                    mismatches.append(("review", got.status_code, got.text))
                adoption = client.get(f"/plans/{pid}/adoption")
                if adoption.status_code != 200:
                    mismatches.append(("adoption", adoption.status_code))
                elif adoption.json()["computation_id"] != cid:
                    mismatches.append(("adoption-content",))
                plan = client.get(f"/plans/{pid}")
                if plan.status_code != 200 or plan.json()["revision"] != 1:
                    mismatches.append(("plan", plan.status_code))
        except Exception as exc:  # 任何线程异常都让测试显式失败
            mismatches.append(("exception", repr(exc)))

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "reader thread hung"
    assert mismatches == []
    assert count_reviews(pid) == 1


# ---------- 6. 错误与原子性 ----------

def test_review_requires_adoption(client):
    pid = "review-no-adoption"
    put(client, pid, VALID_PLAN)
    resp = review(client, pid, ["p1"])
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "ADOPTION_NOT_FOUND"
    assert count_reviews(pid) == 0


def test_review_unknown_plan(client):
    resp = review(client, "ghost", ["p1"])
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"


def test_review_unknown_segment_rejected_without_record(client):
    pid = "review-unknown-seg"
    adopt_plan(client, pid)
    resp = review(client, pid, ["p1", "nope"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert {d["code"] for d in error["details"]} == {"UNKNOWN_SEGMENT"}
    assert count_reviews(pid) == 0


def test_review_unknown_keep_open_segment_rejected_without_record(client):
    pid = "review-unknown-keep-open"
    adopt_plan(client, pid)
    put(client, pid, EXTENDED_PLAN)  # 第 2 版新增 p9，但当前采用仍冻结第 1 版
    resp = review(client, pid, ["p1"], keep_open=["p9"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert {d["code"] for d in error["details"]} == {"UNKNOWN_SEGMENT"}
    assert count_reviews(pid) == 0


def test_review_closed_and_keep_open_must_be_disjoint(client):
    pid = "review-constraint-overlap"
    adopt_plan(client, pid)
    resp = review(client, pid, ["p1", "p2"], keep_open=["p2", "p3"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert {
        d["code"] for d in error["details"]
    } == {"CLOSED_KEEP_OPEN_OVERLAP"}
    assert count_reviews(pid) == 0


def test_review_empty_keep_open_list_is_frozen_with_legacy_compatibility(client):
    pid = "review-keep-open-compatibility"
    adopt_plan(client, pid)

    legacy = client.post(
        f"/plans/{pid}/reviews", json={"closed_segments": ["p1"]}
    ).json()
    assert "keep_open_segments" not in legacy
    got_legacy = client.get(
        f"/plans/{pid}/reviews/{legacy['review_id']}"
    ).json()
    assert "keep_open_segments" not in got_legacy

    explicit_empty = client.post(
        f"/plans/{pid}/reviews",
        json={"closed_segments": ["p1"], "keep_open_segments": []},
    ).json()
    assert explicit_empty["keep_open_segments"] == []
    got_empty = client.get(
        f"/plans/{pid}/reviews/{explicit_empty['review_id']}"
    ).json()
    assert got_empty == explicit_empty
    assert count_reviews(pid) == 2


def test_review_duplicate_segment_rejected_without_record(client):
    pid = "review-dup"
    adopt_plan(client, pid)
    resp = review(client, pid, ["p1", "p1"])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert {d["code"] for d in error["details"]} == {"DUPLICATE_SEGMENT_ID"}
    assert count_reviews(pid) == 0


INVALID_BODIES = [
    ([], "INVALID_BODY"),
    ("nope", "INVALID_BODY"),
    ({}, "INVALID_CLOSED_SEGMENTS_FIELD"),
    ({"closed_segments": "p1"}, "INVALID_CLOSED_SEGMENTS_FIELD"),
    ({"closed_segments": None}, "INVALID_CLOSED_SEGMENTS_FIELD"),
    ({"closed_segments": [1]}, "INVALID_SEGMENT_ID"),
    ({"closed_segments": ["bad id"]}, "INVALID_SEGMENT_ID"),
    (
        {"closed_segments": [], "keep_open_segments": "p1"},
        "INVALID_KEEP_OPEN_SEGMENTS_FIELD",
    ),
    (
        {"closed_segments": [], "keep_open_segments": None},
        "INVALID_KEEP_OPEN_SEGMENTS_FIELD",
    ),
    (
        {"closed_segments": [], "keep_open_segments": [1]},
        "INVALID_SEGMENT_ID",
    ),
    (
        {"closed_segments": [], "keep_open_segments": ["p1", "p1"]},
        "DUPLICATE_SEGMENT_ID",
    ),
]


@pytest.mark.parametrize("body, code", INVALID_BODIES)
def test_review_invalid_payload_rejected_without_record(client, body, code):
    pid = "review-invalid"
    adopt_plan(client, pid)
    resp = client.post(f"/plans/{pid}/reviews", json=body)
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert code in {d["code"] for d in error["details"]}
    assert count_reviews(pid) == 0


def test_review_malformed_json(client):
    pid = "review-bad-json"
    adopt_plan(client, pid)
    resp = client.post(
        f"/plans/{pid}/reviews",
        content=b"{broken",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_JSON"
    assert count_reviews(pid) == 0


def test_get_unknown_review(client):
    pid = "review-get-unknown"
    adopt_plan(client, pid)
    resp = client.get(f"/plans/{pid}/reviews/missing123")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "REVIEW_NOT_FOUND"


def test_get_review_of_other_plan_is_404(client):
    adopt_plan(client, "review-plan-a")
    adopt_plan(client, "review-plan-b")
    record = review(client, "review-plan-a", ["p1"]).json()
    resp = client.get(f"/plans/review-plan-b/reviews/{record['review_id']}")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "REVIEW_NOT_FOUND"
    # 属主方案读取正常
    ok = client.get(f"/plans/review-plan-a/reviews/{record['review_id']}")
    assert ok.status_code == 200
    assert ok.json() == record


def test_get_review_unknown_plan(client):
    resp = client.get("/plans/ghost/reviews/anything")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "PLAN_NOT_FOUND"


def test_db_write_failure_leaves_no_partial_review_record(client):
    """数据库写入失败：服务整体回滚并报 500，不留下半条复核记录。"""
    pid = "review-db-failure"
    adopt_plan(client, pid)

    db = SessionLocal()
    original_commit = db.commit

    def failing_commit(*args, **kwargs):
        raise RuntimeError("simulated database failure")

    db.commit = failing_commit
    try:
        with pytest.raises(ApiError) as excinfo:
            services.review(db, pid, ["p2"], ["p1"])
        assert excinfo.value.status_code == 500
        assert excinfo.value.code == "INTERNAL_ERROR"
    finally:
        db.commit = original_commit
        db.close()

    assert count_reviews(pid) == 0
    # 失败之后数据库仍可正常工作：重试复核成功
    resp = review(client, pid, ["p1"])
    assert resp.status_code == 200
    assert count_reviews(pid) == 1
