"""真实 PostgreSQL 上的执行检查点交错提交、采用切换与重启读取验收。"""

import threading

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app import services
from app.config import settings
from app.db import SessionLocal
from app.errors import ApiError
from app.models import ExecutionCheckpoint, Review

pytestmark = pytest.mark.skipif(
    not settings.database_url.startswith("postgresql"),
    reason="检查点交错提交依赖 PostgreSQL 行锁",
)

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


def prepare_adopted_plan(client, pid, plan=VALID_PLAN):
    put(client, pid, plan)
    cid = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid).status_code == 200
    return cid


def wait_for_lock_waiter(timeout=10.0):
    import time

    deadline = time.monotonic() + timeout
    watcher = SessionLocal()
    sql = text(
        """
        SELECT 1
        FROM pg_stat_activity a
        WHERE a.datname = current_database()
          AND a.pid <> pg_backend_pid()
          AND a.wait_event_type = 'Lock'
          AND EXISTS (
            SELECT 1
            FROM pg_locks w
            JOIN pg_locks h
              ON h.locktype = 'transactionid'
             AND h.transactionid = w.transactionid
             AND h.granted AND h.pid <> w.pid
            WHERE w.pid = a.pid AND NOT w.granted
              AND w.locktype = 'transactionid'
          )
        LIMIT 1
        """
    )
    try:
        while time.monotonic() < deadline:
            if watcher.execute(sql).first() is not None:
                return True
            time.sleep(0.02)
        return False
    finally:
        watcher.close()


def append_with_commit_gate(pid, checkpoint_id, revision, closed=None,
                            keep_open=None):
    """领先者：追加事务到达提交点后保持行锁，直到 gate 放行。"""
    gate = threading.Event()
    reached = threading.Event()
    outcome = {}

    def run():
        db = SessionLocal()
        original_commit = db.commit

        def gated_commit(*args, **kwargs):
            reached.set()
            gate.wait(timeout=15)
            return original_commit(*args, **kwargs)

        db.commit = gated_commit
        try:
            row = services.append_checkpoint(
                db,
                pid,
                checkpoint_id,
                revision,
                closed or [],
                keep_open or [],
            )
            outcome["status"] = 200
            outcome["view"] = services._checkpoint_view(row)
        except ApiError as exc:
            outcome["status"] = exc.status_code
            outcome["code"] = exc.code
            outcome["details"] = exc.details
        except Exception as exc:
            outcome["status"] = 500
            outcome["code"] = "INTERNAL_ERROR"
            outcome["message"] = repr(exc)
        finally:
            db.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, gate, reached, outcome


def append_in_thread(pid, checkpoint_id, revision, closed=None,
                     keep_open=None):
    outcome = {}

    def run():
        db = SessionLocal()
        try:
            row = services.append_checkpoint(
                db,
                pid,
                checkpoint_id,
                revision,
                closed or [],
                keep_open or [],
            )
            outcome["status"] = 200
            outcome["view"] = services._checkpoint_view(row)
        except ApiError as exc:
            outcome["status"] = exc.status_code
            outcome["code"] = exc.code
            outcome["details"] = exc.details
        except Exception as exc:
            outcome["status"] = 500
            outcome["code"] = "INTERNAL_ERROR"
            outcome["message"] = repr(exc)
        finally:
            db.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def test_concurrent_appends_to_same_checkpoint_one_stale_conflict(client):
    pid = "pg-checkpoint-stale"
    prepare_adopted_plan(client, pid)
    chk = client.post(f"/plans/{pid}/execution-checkpoints").json()["checkpoint_id"]

    leader, gate, reached, leader_out = append_with_commit_gate(
        pid, chk, 1, closed=["p1"]
    )
    assert reached.wait(timeout=10)
    follower, follower_out = append_in_thread(pid, chk, 1, closed=["p2"])
    assert wait_for_lock_waiter()
    gate.set()
    leader.join(timeout=15)
    follower.join(timeout=15)

    assert leader_out["status"] == 200, leader_out
    assert follower_out["status"] == 409, follower_out
    assert follower_out["code"] == "CHECKPOINT_REVISION_CONFLICT"
    assert [d["code"] for d in follower_out["details"]] == [
        "CHECKPOINT_REVISION_MISMATCH"
    ]

    body = client.get(f"/plans/{pid}/execution-checkpoints/{chk}").json()
    assert body == leader_out["view"]
    assert body["revision"] == 2
    assert body["closed_segments"] == ["p1"]
    assert body["additional_segments"] == ["p2"]


def test_committed_constraint_conflict_rolls_back_follower_progress(client):
    """PostgreSQL 顺序交班：新追加命中累计集合重复时返回 422 且进度不变。

    基于同一旧修订的两个并发请求在行锁释放后首先会被判为修订过期（409）；
    调用方重读新修订后，若仍提交上一班已登记的管段，才进入累计约束冲突。
    """
    pid = "pg-checkpoint-constraint"
    prepare_adopted_plan(client, pid)
    chk = client.post(f"/plans/{pid}/execution-checkpoints").json()["checkpoint_id"]

    first = client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/append",
        json={"expected_revision": 1, "keep_open_segments": ["p2"]},
    )
    assert first.status_code == 200
    follower, follower_out = append_in_thread(pid, chk, 2, keep_open=["p2"])
    follower.join(timeout=15)

    assert follower_out["status"] == 422
    assert follower_out["details"][0]["code"] == "SEGMENT_ALREADY_KEEP_OPEN"
    body = client.get(f"/plans/{pid}/execution-checkpoints/{chk}").json()
    assert body["revision"] == 2
    assert body["closed_segments"] == []
    assert body["keep_open_segments"] == ["p2"]


def test_committed_infeasible_append_leaves_last_complete_result(client):
    pid = "pg-checkpoint-infeasible"
    prepare_adopted_plan(client, pid)
    chk = client.post(f"/plans/{pid}/execution-checkpoints").json()["checkpoint_id"]

    first = client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/append",
        json={"expected_revision": 1, "closed_segments": ["p2"]},
    )
    assert first.status_code == 200
    before = first.json()
    follower, follower_out = append_in_thread(
        pid, chk, 2, keep_open=["p1", "p3"]
    )
    follower.join(timeout=15)

    assert follower_out["status"] == 422
    assert follower_out["code"] == "REVIEW_NOT_EXECUTABLE"
    body = client.get(f"/plans/{pid}/execution-checkpoints/{chk}").json()
    assert body == before
    assert body["revision"] == 2
    assert body["closed_segments"] == ["p2"]
    assert body["keep_open_segments"] == []


def test_adoption_switch_interleaves_without_freezing_wrong_snapshot(client):
    pid = "pg-checkpoint-switch"
    c1 = prepare_adopted_plan(client, pid, VALID_PLAN)
    chk = client.post(f"/plans/{pid}/execution-checkpoints").json()["checkpoint_id"]

    put(client, pid, REVISED_PLAN)
    c2 = compute(client, pid)["computation_id"]

    create_ready = threading.Event()
    create_gate = threading.Event()
    create_out = {}

    def create_with_gate():
        db = SessionLocal()
        original_commit = db.commit

        def gated_commit(*args, **kwargs):
            create_ready.set()
            create_gate.wait(timeout=15)
            return original_commit(*args, **kwargs)

        db.commit = gated_commit
        try:
            row = services.create_checkpoint(db, pid)
            create_out["status"] = 200
            create_out["view"] = services._checkpoint_view(row)
        except ApiError as exc:
            create_out["status"] = exc.status_code
            create_out["code"] = exc.code
        finally:
            db.close()

    creator = threading.Thread(target=create_with_gate)
    creator.start()
    assert create_ready.wait(timeout=10)

    # 创建检查点已持有 plans 行锁；采用切换等待，不存在死锁。
    adoption_out = {}

    def adopt_c2():
        db = SessionLocal()
        try:
            adoption = services.adopt(db, pid, c2)
            adoption_out["status"] = 200
            adoption_out["snapshot"] = adoption.snapshot
        except ApiError as exc:
            adoption_out["status"] = exc.status_code
            adoption_out["code"] = exc.code
        finally:
            db.close()

    adopter = threading.Thread(target=adopt_c2)
    adopter.start()
    assert wait_for_lock_waiter()
    create_gate.set()
    creator.join(timeout=15)
    adopter.join(timeout=15)

    assert create_out["status"] == 200, create_out
    assert adoption_out["status"] == 200, adoption_out
    # 创建检查点在采用切换前进入临界区，永久冻结 c1/第 1 版。
    assert create_out["view"]["computation_id"] == c1
    assert create_out["view"]["plan_revision"] == 1

    resp = client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/append",
        json={"expected_revision": 1, "closed_segments": ["p3"]},
    )
    assert resp.status_code == 200
    old_body = resp.json()
    assert old_body["computation_id"] == c1
    assert old_body["additional_cost"] == 7

    new_chk = client.post(f"/plans/{pid}/execution-checkpoints").json()
    assert new_chk["computation_id"] == c2
    assert new_chk["plan_revision"] == 2


def test_completed_checkpoint_reads_after_new_connection_and_restart(client):
    """用全新会话/连接模拟交班重启：进度与不可变复核记录都能读取。"""
    pid = "pg-checkpoint-restart"
    prepare_adopted_plan(client, pid)
    chk = client.post(f"/plans/{pid}/execution-checkpoints").json()["checkpoint_id"]
    assert (
        client.post(
            f"/plans/{pid}/execution-checkpoints/{chk}/append",
            json={"expected_revision": 1, "closed_segments": ["p1", "p2"]},
        ).status_code
        == 200
    )
    assert client.post(
        f"/plans/{pid}/execution-checkpoints/{chk}/complete"
    ).status_code == 200

    # 创建一个全新的 engine/session 工厂（连接池也重新建立），模拟服务
    # 交班后重启进程再按 ID 读取，而不是复用 SessionLocal 的缓存连接。
    restart_engine = create_engine(settings.database_url, pool_pre_ping=True)
    RestartSession = sessionmaker(
        bind=restart_engine, autoflush=False, expire_on_commit=False
    )
    try:
        db = RestartSession()
        try:
            checkpoint = services.get_checkpoint_or_404(db, pid, chk)
            review = services.get_review_or_404(db, pid, chk)
            assert checkpoint.status == "COMPLETED"
            assert services._checkpoint_view(checkpoint)["review_id"] == chk
            assert review.review_id == chk
            assert review.record["witness"]["cut_segments"] == ["p1", "p2"]
            assert db.query(ExecutionCheckpoint).filter(
                ExecutionCheckpoint.plan_id == pid
            ).count() == 1
            assert db.query(Review).filter(Review.plan_id == pid).count() == 1
        finally:
            db.close()
    finally:
        restart_engine.dispose()
