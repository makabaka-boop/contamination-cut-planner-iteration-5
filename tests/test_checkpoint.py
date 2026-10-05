"""逐班执行检查点：累计追加、乐观修订、冻结版本、完成与旧复核兼容。"""

from collections import deque
from itertools import product

import pytest

from app import services
from app.db import SessionLocal
from app.errors import ApiError
from app.models import ExecutionCheckpoint

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


def create_checkpoint(client, pid):
    return client.post(f"/plans/{pid}/execution-checkpoints")


def get_checkpoint(client, pid, chk):
    return client.get(f"/plans/{pid}/execution-checkpoints/{chk}")


def append_checkpoint(client, pid, chk, expected, closed=None, keep_open=None):
    body = {"expected_revision": expected}
    if closed is not None:
        body["closed_segments"] = closed
    if keep_open is not None:
        body["keep_open_segments"] = keep_open
    return client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/append", json=body
    )


def complete_checkpoint(client, pid, chk):
    return client.post(f"/plans/{pid}/execution-checkpoints/{chk}/complete")


def legacy_review(client, pid, closed, keep_open=None):
    body = {"closed_segments": closed}
    if keep_open is not None:
        body["keep_open_segments"] = keep_open
    return client.post(f"/plans/{pid}/reviews", json=body)


def cut_isolates_sources(plan, cut_ids):
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


def brute_force_constrained(plan, closed, keep_open):
    """枚举可切断管子集，返回 None 或 (源侧交集, 新增清单, 最低费用)。"""
    closed, keep_open = set(closed), set(keep_open)
    by_id = {seg["id"]: seg for seg in plan["segments"]}
    candidates = [
        seg["id"]
        for seg in plan["segments"]
        if seg["id"] not in closed and seg["id"] not in keep_open
    ]

    def reachable(removed):
        adjacency = {}
        for seg in plan["segments"]:
            if seg["id"] in removed:
                continue
            adjacency.setdefault(seg["from"], []).append(seg["to"])
        seen = set(plan["sources"])
        stack = list(plan["sources"])
        while stack:
            node = stack.pop()
            for nxt in adjacency.get(node, []):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    feasible = []
    best = None
    for mask in range(1 << len(candidates)):
        extra = {
            seg_id for i, seg_id in enumerate(candidates) if mask & (1 << i)
        }
        reachable_nodes = reachable(closed | extra)
        if reachable_nodes.isdisjoint(plan["protections"]):
            cost = sum(by_id[seg_id]["cost"] for seg_id in extra)
            feasible.append((extra, cost, reachable_nodes))
            if best is None or cost < best:
                best = cost
    if best is None:
        return None

    intersection = None
    for extra, cost, reachable_nodes in feasible:
        if cost == best:
            intersection = (
                set(reachable_nodes)
                if intersection is None
                else intersection & reachable_nodes
            )
    additional = sorted(
        seg_id for seg_id in candidates
        if by_id[seg_id]["from"] in intersection
        and by_id[seg_id]["to"] not in intersection
    )
    return sorted(intersection), additional, best


# ---------- 小图状态枚举：每次创建后逐次追加 ----------


SMALL_PLAN_TEMPLATE = {
    "zones": ["Z0", "Z1", "Z2"],
    "segments": [
        {"id": "e0", "from": "Z0", "to": "Z1", "cost": 2},
        {"id": "e1", "from": "Z1", "to": "Z2", "cost": 3},
        {"id": "e2", "from": "Z0", "to": "Z2", "cost": 5},
    ],
    "sources": ["Z0"],
    "protections": ["Z2"],
}

def all_legal_partitions(zones):
    n = len(zones)
    full = (1 << n) - 1
    result = []
    for src_mask in range(1, 1 << n):
        complement = full ^ src_mask
        sub = complement
        while sub:
            sources = [zones[i] for i in range(n) if src_mask >> i & 1]
            protections = [zones[i] for i in range(n) if sub >> i & 1]
            result.append((sources, protections))
            sub = (sub - 1) & complement
    return result


PARTITIONS = all_legal_partitions(["Z0", "Z1", "Z2"])


@pytest.mark.parametrize("sources,protections", PARTITIONS)
def test_checkpoint_every_small_state_matches_enumeration(sources, protections):
    plan = dict(SMALL_PLAN_TEMPLATE, sources=sources, protections=protections)
    pid = f"enum-{'-'.join(sources)}-{'-'.join(protections)}"

    db = SessionLocal()
    try:
        from app.validation import validate_plan_payload

        services.save_plan(db, pid, validate_plan_payload(plan))
        computation = services.compute(db, pid)
        services.adopt(db, pid, computation.computation_id)
    finally:
        db.close()

    ids = [seg["id"] for seg in plan["segments"]]
    for state_index, states in enumerate(
        product(("remaining", "closed", "keep_open"), repeat=3)
    ):
        closed = [seg_id for seg_id, state in zip(ids, states) if state == "closed"]
        keep_open = [
            seg_id for seg_id, state in zip(ids, states) if state == "keep_open"
        ]
        expected = brute_force_constrained(plan, closed, keep_open)

        db = SessionLocal()
        try:
            chk = services.create_checkpoint(db, pid)
            checkpoint_id = chk.checkpoint_id
            assert chk.revision == 1

            expected_revision = 1
            if closed:
                chk = services.append_checkpoint(
                    db, pid, checkpoint_id, expected_revision, closed, []
                )
                expected_revision = chk.revision

            if keep_open:
                if expected is None:
                    with pytest.raises(ApiError) as excinfo:
                        services.append_checkpoint(
                            db,
                            pid,
                            checkpoint_id,
                            expected_revision,
                            [],
                            keep_open,
                        )
                    assert excinfo.value.status_code == 422
                    assert excinfo.value.code == "REVIEW_NOT_EXECUTABLE"
                    continue
                chk = services.append_checkpoint(
                    db,
                    pid,
                    checkpoint_id,
                    expected_revision,
                    [],
                    keep_open,
                )
                expected_revision = chk.revision
            else:
                assert expected is not None

            stored = services.get_checkpoint_or_404(db, pid, checkpoint_id)
            side, additional, cost = expected
            assert stored.revision == expected_revision
            assert stored.closed_segments == sorted(closed)
            assert stored.keep_open_segments == sorted(keep_open)
            assert stored.outcome["additional_segments"] == additional
            assert stored.outcome["additional_cost"] == cost
            assert stored.outcome["witness"]["source_zones"] == side
            merged = sorted(set(closed) | set(additional))
            assert stored.outcome["witness"]["cut_segments"] == merged
            assert cut_isolates_sources(plan, merged)
        finally:
            db.close()


# ---------- HTTP 逐次关闭与完成 ----------


def test_checkpoint_is_created_from_current_adoption(client):
    pid = "checkpoint-create"
    cid = adopt_plan(client, pid)
    resp = create_checkpoint(client, pid)
    assert resp.status_code == 200
    body = resp.json()
    assert body["checkpoint_id"]
    assert body["status"] == "ACTIVE"
    assert body["revision"] == 1
    assert body["plan_id"] == pid
    assert body["plan_revision"] == 1
    assert body["computation_id"] == cid
    assert body["closed_segments"] == []
    assert body["keep_open_segments"] == []
    assert body["additional_segments"] == ["p1", "p2"]
    assert body["additional_cost"] == 10
    assert get_checkpoint(client, pid, body["checkpoint_id"]).json() == body


def test_successive_appends_accumulate_and_bump_revision(client):
    pid = "checkpoint-sequence"
    cid = adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]

    first = append_checkpoint(client, pid, chk, 1, closed=["p1"])
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["revision"] == 2
    assert first_body["closed_segments"] == ["p1"]
    assert first_body["keep_open_segments"] == []
    assert first_body["additional_segments"] == ["p2"]
    assert first_body["additional_cost"] == 6
    assert first_body["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }

    second = append_checkpoint(client, pid, chk, 2, closed=["p2"])
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["revision"] == 3
    assert second_body["closed_segments"] == ["p1", "p2"]
    assert second_body["additional_segments"] == []
    assert second_body["additional_cost"] == 0
    assert second_body["witness"] == {
        "source_zones": ["SRC1", "SRC2"],
        "cut_segments": ["p1", "p2"],
        "total_cost": 10,
    }
    assert get_checkpoint(client, pid, chk).json() == second_body
    assert second_body["computation_id"] == cid


def test_checkpoint_completion_creates_immutable_review_with_same_id(client):
    pid = "checkpoint-complete"
    cid = adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    assert append_checkpoint(client, pid, chk, 1, closed=["p1", "p2"]).status_code == 200

    completed = complete_checkpoint(client, pid, chk)
    assert completed.status_code == 200
    body = completed.json()
    assert body["status"] == "COMPLETED"
    assert body["revision"] == 2
    assert body["review_id"] == chk
    assert body["completed_at"]

    review = client.get(f"/plans/{pid}/reviews/{chk}")
    assert review.status_code == 200
    record = review.json()
    assert record == {
        "review_id": chk,
        "plan_id": pid,
        "plan_revision": 1,
        "computation_id": cid,
        "created_at": body["completed_at"],
        "closed_segments": ["p1", "p2"],
        "keep_open_segments": [],
        "additional_segments": [],
        "additional_cost": 0,
        "witness": {
            "source_zones": ["SRC1", "SRC2"],
            "cut_segments": ["p1", "p2"],
            "total_cost": 10,
        },
    }
    assert get_checkpoint(client, pid, chk).json() == body

    # 不可变：完成后不能继续追加或再次完成。
    again = append_checkpoint(client, pid, chk, 2, closed=["p3"])
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "CHECKPOINT_COMPLETED"
    repeat = complete_checkpoint(client, pid, chk)
    assert repeat.status_code == 409
    assert repeat.json()["error"]["code"] == "CHECKPOINT_COMPLETED"


def test_can_freeze_partial_progress_when_completing_checkpoint(client):
    pid = "checkpoint-partial-complete"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    append_checkpoint(client, pid, chk, 1, closed=["p1"])

    resp = complete_checkpoint(client, pid, chk)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "COMPLETED"
    assert body["revision"] == 2
    assert body["additional_segments"] == ["p2"]
    assert body["additional_cost"] == 6

    record = client.get(f"/plans/{pid}/reviews/{chk}")
    assert record.status_code == 200
    assert record.json()["additional_segments"] == ["p2"]
    assert record.json()["witness"]["cut_segments"] == ["p1", "p2"]


# ---------- 修订冲突、累计约束和不可执行 ----------


def test_stale_expected_revision_rejected_without_progress_change(client):
    pid = "checkpoint-stale"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    assert append_checkpoint(client, pid, chk, 1, closed=["p1"]).status_code == 200

    resp = append_checkpoint(client, pid, chk, 1, closed=["p2"])
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "CHECKPOINT_REVISION_CONFLICT"
    assert {d["code"] for d in error["details"]} == {
        "CHECKPOINT_REVISION_MISMATCH"
    }
    current = get_checkpoint(client, pid, chk).json()
    assert current["revision"] == 2
    assert current["closed_segments"] == ["p1"]
    assert current["additional_segments"] == ["p2"]


def test_cumulative_sets_can_only_add_and_cannot_cross(client):
    pid = "checkpoint-disjoint"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    assert append_checkpoint(
        client, pid, chk, 1, closed=["p1"], keep_open=["p2"]
    ).status_code == 200

    duplicate_closed = append_checkpoint(
        client, pid, chk, 2, closed=["p1"]
    )
    assert duplicate_closed.status_code == 422
    assert duplicate_closed.json()["error"]["details"][0]["code"] == (
        "SEGMENT_ALREADY_CLOSED"
    )

    duplicate_open = append_checkpoint(
        client, pid, chk, 2, keep_open=["p2"]
    )
    assert duplicate_open.status_code == 422
    assert duplicate_open.json()["error"]["details"][0]["code"] == (
        "SEGMENT_ALREADY_KEEP_OPEN"
    )

    cross = append_checkpoint(client, pid, chk, 2, keep_open=["p1"])
    assert cross.status_code == 422
    assert cross.json()["error"]["details"][0]["code"] == (
        "CLOSED_KEEP_OPEN_OVERLAP"
    )

    current = get_checkpoint(client, pid, chk).json()
    assert current["revision"] == 2
    assert current["closed_segments"] == ["p1"]
    assert current["keep_open_segments"] == ["p2"]


def test_infeasible_keep_open_append_leaves_previous_result_unchanged(client):
    pid = "checkpoint-infeasible"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    append_checkpoint(client, pid, chk, 1, closed=["p2"])
    before = get_checkpoint(client, pid, chk).json()

    resp = append_checkpoint(
        client, pid, chk, 2, keep_open=["p1", "p3"]
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "REVIEW_NOT_EXECUTABLE"
    assert get_checkpoint(client, pid, chk).json() == before


def test_unknown_segment_append_leaves_checkpoint_unchanged(client):
    pid = "checkpoint-unknown"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    resp = append_checkpoint(client, pid, chk, 1, closed=["nope"])
    assert resp.status_code == 422
    assert "UNKNOWN_SEGMENT" in {
        d["code"] for d in resp.json()["error"]["details"]
    }
    current = get_checkpoint(client, pid, chk).json()
    assert current["revision"] == 1
    assert current["closed_segments"] == []


INVALID_APPEND_BODIES = [
    ({}, "MISSING_EXPECTED_REVISION"),
    ({"expected_revision": None}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": 0}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": "1"}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": True}, "INVALID_EXPECTED_REVISION"),
    ({"expected_revision": 1}, "NO_SEGMENTS_ADDED"),
    (
        {"expected_revision": 1, "closed_segments": None},
        "INVALID_CLOSED_SEGMENTS_FIELD",
    ),
    (
        {"expected_revision": 1, "keep_open_segments": {}},
        "INVALID_KEEP_OPEN_SEGMENTS_FIELD",
    ),
    (
        {"expected_revision": 1, "closed_segments": ["p1", "p1"]},
        "DUPLICATE_SEGMENT_ID",
    ),
    (
        {
            "expected_revision": 1,
            "closed_segments": ["p1"],
            "keep_open_segments": ["p1"],
        },
        "CLOSED_KEEP_OPEN_OVERLAP",
    ),
]


@pytest.mark.parametrize("body,code", INVALID_APPEND_BODIES)
def test_invalid_append_payload(client, body, code):
    pid = "checkpoint-invalid-body"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    resp = client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/append", json=body
    )
    assert resp.status_code == 422
    assert code in {d["code"] for d in resp.json()["error"]["details"]}
    assert get_checkpoint(client, pid, chk).json()["revision"] == 1


def test_checkpoint_and_append_errors_for_missing_resources(client):
    assert create_checkpoint(client, "ghost").status_code == 404

    pid = "checkpoint-missing"
    put(client, pid, VALID_PLAN)
    assert create_checkpoint(client, pid).status_code == 404

    cid = adopt_plan(client, f"{pid}-adopted")
    chk = create_checkpoint(client, f"{pid}-adopted").json()["checkpoint_id"]
    resp = get_checkpoint(client, f"{pid}-adopted", "missing")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    assert cid


def test_checkpoint_of_other_plan_is_404(client):
    adopt_plan(client, "chk-plan-a")
    adopt_plan(client, "chk-plan-b")
    chk = create_checkpoint(client, "chk-plan-a").json()["checkpoint_id"]
    resp = get_checkpoint(client, "chk-plan-b", chk)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHECKPOINT_NOT_FOUND"
    assert get_checkpoint(client, "chk-plan-a", chk).status_code == 200


# ---------- 冻结快照和旧接口 ----------


def test_checkpoint_continues_on_creation_snapshot_after_edit_and_adoption_switch(
    client,
):
    pid = "checkpoint-freeze"
    c1 = adopt_plan(client, pid, VALID_PLAN)
    old_chk = create_checkpoint(client, pid).json()["checkpoint_id"]

    # 第 2 版 p4 费用变为 1；采用新计算后，旧检查点仍绑定第 1 版/c1。
    assert put(client, pid, REVISED_PLAN).json()["revision"] == 2
    c2 = compute(client, pid)["computation_id"]
    assert adopt(client, pid, c2).status_code == 200

    old_after = append_checkpoint(client, pid, old_chk, 1, closed=["p3"])
    assert old_after.status_code == 200
    old_body = old_after.json()
    assert old_body["plan_revision"] == 1
    assert old_body["computation_id"] == c1
    assert old_body["additional_segments"] == ["p4"]
    assert old_body["additional_cost"] == 7
    assert old_body["witness"]["total_cost"] == 12

    new_chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    new_body = append_checkpoint(
        client, pid, new_chk, 1, closed=["p3"]
    ).json()
    assert new_body["plan_revision"] == 2
    assert new_body["computation_id"] == c2
    assert new_body["additional_segments"] == ["p4"]
    assert new_body["additional_cost"] == 1


def test_legacy_one_shot_review_interface_is_unchanged(client):
    pid = "checkpoint-legacy"
    cid = adopt_plan(client, pid)
    record = legacy_review(client, pid, ["p1"]).json()
    assert record["review_id"]
    assert record["computation_id"] == cid
    assert "checkpoint_id" not in record
    assert "status" not in record
    assert "revision" not in record
    assert record["closed_segments"] == ["p1"]
    assert record["additional_segments"] == ["p2"]
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record

    # 一次性复核不产生执行检查点；检查点完成也复用 reviews，不改变旧记录。
    db = SessionLocal()
    try:
        assert db.query(ExecutionCheckpoint).filter(
            ExecutionCheckpoint.plan_id == pid
        ).count() == 0
    finally:
        db.close()


def test_checkpoint_write_failure_rolls_back_progress(client):
    pid = "checkpoint-write-failure"
    adopt_plan(client, pid)
    db = SessionLocal()
    chk_id = services.create_checkpoint(db, pid).checkpoint_id
    db.close()

    db = SessionLocal()
    original_commit = db.commit

    def failing_commit(*args, **kwargs):
        raise RuntimeError("simulated database failure")

    db.commit = failing_commit
    try:
        with pytest.raises(ApiError) as excinfo:
            services.append_checkpoint(db, pid, chk_id, 1, ["p1"], ["p3"])
        assert excinfo.value.status_code == 500
        assert excinfo.value.code == "INTERNAL_ERROR"
    finally:
        db.commit = original_commit
        db.close()

    body = get_checkpoint(client, pid, chk_id).json()
    assert body["revision"] == 1
    assert body["closed_segments"] == []
    assert body["keep_open_segments"] == []
    assert append_checkpoint(client, pid, chk_id, 1, closed=["p1"]).status_code == 200


def test_checkpoint_progress_survives_new_database_session(client):
    pid = "checkpoint-restart-session"
    adopt_plan(client, pid)
    chk = create_checkpoint(client, pid).json()["checkpoint_id"]
    append_checkpoint(client, pid, chk, 1, closed=["p1"], keep_open=["p3"])

    db = SessionLocal()
    try:
        row = services.get_checkpoint_or_404(db, pid, chk)
        view = services._checkpoint_view(row)
        assert view["revision"] == 2
        assert view["closed_segments"] == ["p1"]
        assert view["keep_open_segments"] == ["p3"]
    finally:
        db.close()
