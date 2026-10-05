"""真实 PostgreSQL 上的执行检查点并发验收测试。

场景与期望：
- 两个并发追加携带同一预期修订号：恰一胜（200，修订唯一推进）一负
  （409 REVISION_CONFLICT / REVISION_MISMATCH），落败追加不改变进度，
  重新读取后可按新修订重试；
- 追加与完成并发：两种领先顺序都确定——完成领先时追加得到 409
  CHECKPOINT_ALREADY_COMPLETED 且复核记录冻结追加前状态；追加领先时
  完成成功且复核记录冻结追加后的最终状态；
- 两个并发完成：恰一胜一负（409 CHECKPOINT_ALREADY_COMPLETED），
  只产生一条复核记录；
- 多会话交错提交覆盖逐次关闭、约束冲突、采用切换与重启读取。

并发的真实性由 PostgreSQL 行锁保证（与并发采用验收同样的做法）：领先者
的条件 UPDATE 已执行（持有 checkpoints 行锁）但在提交点被 gate 挂住，
确认落后者已阻塞在同一行的锁竞争上（pg_locks 中出现未授予的锁等待）
后再放行提交，从而两次操作确定地在锁上相遇，杜绝"线程时序碰巧串行化"
造成的假阳性。

运行：
    TEST_DATABASE_URL=postgresql+psycopg2://cleanroom@/cleanroom?host=/tmp \
        pytest tests/test_checkpoint_pg.py
"""

import threading

import pytest
from sqlalchemy import text

from app import services
from app.config import settings
from app.db import SessionLocal
from app.errors import ApiError
from app.models import Checkpoint, Review

pytestmark = pytest.mark.skipif(
    not settings.database_url.startswith("postgresql"),
    reason="检查点并发依赖 PostgreSQL 行锁与条件 UPDATE 的重评估",
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


def create_checkpoint(pid):
    db = SessionLocal()
    try:
        return services.create_checkpoint(db, pid).checkpoint_id
    finally:
        db.close()


def get_checkpoint_row(pid, cpid):
    db = SessionLocal()
    try:
        return services.get_checkpoint_or_404(db, pid, cpid)
    finally:
        db.close()


def count_reviews(pid):
    db = SessionLocal()
    try:
        return db.query(Review).filter(Review.plan_id == pid).count()
    finally:
        db.close()


def _wait_for_lock_waiter(timeout=10.0):
    """等待直到另一个会话阻塞在他人持有的行锁/事务锁上。"""
    import time

    deadline = time.monotonic() + timeout
    watcher = SessionLocal()
    try:
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
        while time.monotonic() < deadline:
            if watcher.execute(sql).first() is not None:
                return True
            time.sleep(0.02)
        return False
    finally:
        watcher.close()


def _run_in_thread(fn, gate=None):
    """在独立线程/会话中执行服务调用，返回 (线程, 结果字典, 就绪事件)。

    gate 非空时，第一次提交前先置就绪事件并等待 gate 放行，使调用方
    能确认该事务已持锁未提交，制造确定的锁重叠。
    """
    outcome = {}
    reached = threading.Event()

    def run():
        db = SessionLocal()
        if gate is not None:
            original_commit = db.commit

            def gated_commit(*args, **kwargs):
                if not reached.is_set():
                    reached.set()
                    gate.wait(timeout=15)
                return original_commit(*args, **kwargs)

            db.commit = gated_commit
        try:
            outcome["result"] = fn(db)
            outcome["status"] = 200
        except ApiError as exc:
            outcome["status"] = exc.status_code
            outcome["code"] = exc.code
            outcome["details"] = [d["code"] for d in exc.details]
        except Exception as exc:  # 任何非预期错误都让测试显式失败
            outcome["status"] = 500
            outcome["code"] = "INTERNAL_ERROR"
            outcome["message"] = repr(exc)
        finally:
            db.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome, reached


def _append_call(pid, cpid, expected_revision, closed, keep_open):
    def call(db):
        return services.append_checkpoint(
            db, pid, cpid, expected_revision, closed, keep_open
        )

    return call


def _complete_call(pid, cpid):
    def call(db):
        return services.complete_checkpoint(db, pid, cpid)

    return call


def test_concurrent_appends_same_revision_one_wins(client):
    """两个追加都基于修订 1：恰一胜（修订推进为 2）一负（409），
    落败追加不改变进度，重新读取后可按新修订重试成功。"""
    pid = "cp-pg-append-race"
    adopt_plan(client, pid)
    cpid = create_checkpoint(pid)

    gate = threading.Event()
    leader, lout, leader_ready = _run_in_thread(
        _append_call(pid, cpid, 1, ["p1"], []), gate=gate
    )
    assert leader_ready.wait(timeout=10), "leader never reached commit point"

    follower, fout, _ = _run_in_thread(_append_call(pid, cpid, 1, ["p2"], []))
    assert _wait_for_lock_waiter(), "follower never blocked on the checkpoint row lock"

    gate.set()
    leader.join(timeout=15)
    follower.join(timeout=15)
    assert not leader.is_alive() and not follower.is_alive(), "worker thread hung"

    # ---- 恰一胜一负：不允许两个 200，也不允许 500 ----
    assert lout["status"] == 200, lout
    assert fout["status"] == 409, fout
    assert fout["code"] == "REVISION_CONFLICT", fout
    assert fout["details"] == ["REVISION_MISMATCH"], fout

    # ---- 胜者响应即最终进度：修订 2、只登记了 p1 ----
    winner = lout["result"]
    assert winner.revision == 2
    assert winner.closed_segments == ["p1"]
    assert winner.keep_open_segments == []
    assert winner.outcome["additional_segments"] == ["p2"]
    assert winner.outcome["additional_cost"] == 6
    final = get_checkpoint_row(pid, cpid)
    assert final.revision == 2
    assert final.closed_segments == ["p1"]
    assert final.outcome == winner.outcome

    # ---- 落败者重新读取后按新修订重试：进度继续推进 ----
    db = SessionLocal()
    try:
        retried = services.append_checkpoint(db, pid, cpid, 2, ["p2"], [])
        assert retried.revision == 3
        assert retried.closed_segments == ["p1", "p2"]
        assert retried.outcome["additional_segments"] == []
        assert retried.outcome["additional_cost"] == 0
    finally:
        db.close()


def test_concurrent_append_and_complete_complete_wins(client):
    """完成领先：追加得到 409 CHECKPOINT_ALREADY_COMPLETED，复核记录
    冻结追加前的进度，检查点终态不含落败追加的内容。"""
    pid = "cp-pg-complete-first"
    adopt_plan(client, pid)
    cpid = create_checkpoint(pid)
    db = SessionLocal()
    try:
        services.append_checkpoint(db, pid, cpid, 1, ["p1"], [])
    finally:
        db.close()

    gate = threading.Event()
    leader, lout, leader_ready = _run_in_thread(_complete_call(pid, cpid), gate=gate)
    assert leader_ready.wait(timeout=10), "leader never reached commit point"

    follower, fout, _ = _run_in_thread(_append_call(pid, cpid, 2, ["p2"], []))
    assert _wait_for_lock_waiter(), "follower never blocked on the checkpoint row lock"

    gate.set()
    leader.join(timeout=15)
    follower.join(timeout=15)

    assert lout["status"] == 200, lout
    assert fout["status"] == 409, fout
    assert fout["code"] == "CHECKPOINT_ALREADY_COMPLETED", fout

    # 复核记录冻结追加前状态：只关闭 p1
    record = lout["result"].record
    assert record["closed_segments"] == ["p1"]
    assert record["additional_segments"] == ["p2"]
    assert record["checkpoint_id"] == cpid
    assert count_reviews(pid) == 1

    # 检查点终态：COMPLETED、修订 2、不含落败追加的 p2
    final = get_checkpoint_row(pid, cpid)
    assert final.status == "COMPLETED"
    assert final.revision == 2
    assert final.closed_segments == ["p1"]
    assert final.review_id == record["review_id"]

    # 复核记录可按 ID 读取
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record


def test_concurrent_append_and_complete_append_wins(client):
    """追加领先：完成成功，复核记录必须冻结追加后的最终进度
    （完成在持锁后重读检查点，而不是使用加锁前读到的旧进度）。"""
    pid = "cp-pg-append-first"
    adopt_plan(client, pid)
    cpid = create_checkpoint(pid)

    gate = threading.Event()
    leader, lout, leader_ready = _run_in_thread(
        _append_call(pid, cpid, 1, ["p1"], []), gate=gate
    )
    assert leader_ready.wait(timeout=10), "leader never reached commit point"

    follower, fout, _ = _run_in_thread(_complete_call(pid, cpid))
    assert _wait_for_lock_waiter(), "follower never blocked on the checkpoint row lock"

    gate.set()
    leader.join(timeout=15)
    follower.join(timeout=15)

    assert lout["status"] == 200, lout
    assert fout["status"] == 200, fout

    # 复核记录冻结追加后的最终状态（修订 2 的进度）
    record = fout["result"].record
    assert record["closed_segments"] == ["p1"]
    assert record["additional_segments"] == ["p2"]
    assert record["additional_cost"] == 6
    assert count_reviews(pid) == 1

    final = get_checkpoint_row(pid, cpid)
    assert final.status == "COMPLETED"
    assert final.revision == 2
    assert final.review_id == record["review_id"]

    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record


def test_concurrent_completes_one_wins(client):
    """两个并发完成：恰一胜一负，只产生一条复核记录。"""
    pid = "cp-pg-double-complete"
    adopt_plan(client, pid)
    cpid = create_checkpoint(pid)

    gate = threading.Event()
    leader, lout, leader_ready = _run_in_thread(_complete_call(pid, cpid), gate=gate)
    assert leader_ready.wait(timeout=10), "leader never reached commit point"

    follower, fout, _ = _run_in_thread(_complete_call(pid, cpid))
    assert _wait_for_lock_waiter(), "follower never blocked on the checkpoint row lock"

    gate.set()
    leader.join(timeout=15)
    follower.join(timeout=15)

    assert lout["status"] == 200, lout
    assert fout["status"] == 409, fout
    assert fout["code"] == "CHECKPOINT_ALREADY_COMPLETED", fout
    assert count_reviews(pid) == 1

    final = get_checkpoint_row(pid, cpid)
    assert final.status == "COMPLETED"
    assert final.review_id == lout["result"].review_id


def test_interleaved_sessions_progress_conflicts_switch_and_restart(client):
    """多会话交错提交：逐次关闭、约束冲突、采用切换与重启读取。"""
    pid = "cp-pg-interleaved"
    cid1 = adopt_plan(client, pid)
    cpid = create_checkpoint(pid)

    # 会话 A：关闭 p1（修订 1 -> 2）
    db = SessionLocal()
    try:
        cp = services.append_checkpoint(db, pid, cpid, 1, ["p1"], [])
        assert cp.revision == 2
    finally:
        db.close()

    # 采用切换：修订方案（p4: 7 -> 1）并采用另一计算
    put(client, pid, REVISED_PLAN)
    cid2 = compute(client, pid)["computation_id"]
    assert adopt(client, pid, cid2).status_code == 200

    # 会话 B（全新会话，模拟交班/重启）：读取进度后继续登记保持开启
    db = SessionLocal()
    try:
        cp = services.get_checkpoint_or_404(db, pid, cpid)
        assert cp.revision == 2
        cp = services.append_checkpoint(db, pid, cpid, cp.revision, [], ["p3"])
        assert cp.revision == 3
        assert cp.keep_open_segments == ["p3"]
    finally:
        db.close()

    # 会话 C：约束冲突交错——过期修订、重复登记、不可执行都不改变进度
    db = SessionLocal()
    try:
        with pytest.raises(ApiError) as stale:
            services.append_checkpoint(db, pid, cpid, 1, ["p2"], [])
        assert stale.value.status_code == 409
        assert stale.value.code == "REVISION_CONFLICT"
        with pytest.raises(ApiError) as dup:
            services.append_checkpoint(db, pid, cpid, 3, ["p1"], [])
        assert dup.value.status_code == 422
        with pytest.raises(ApiError) as infeasible:
            # p2 保持开启后 SRC2 -> MID -> SAFE1 全为保持开启边：不可执行
            services.append_checkpoint(db, pid, cpid, 3, [], ["p2"])
        assert infeasible.value.status_code == 422
        assert infeasible.value.code == "REVIEW_NOT_EXECUTABLE"
        cp = services.get_checkpoint_or_404(db, pid, cpid)
        assert cp.revision == 3
        assert cp.closed_segments == ["p1"]
        assert cp.keep_open_segments == ["p3"]
        assert cp.outcome["additional_segments"] == ["p2"]
    finally:
        db.close()

    # 会话 D：按冻结的第 1 版继续推进（p4 费用仍是 7）并完成
    db = SessionLocal()
    try:
        cp = services.append_checkpoint(db, pid, cpid, 3, ["p2"], [])
        assert cp.revision == 4
        assert cp.computation_id == cid1
        assert cp.plan_revision == 1
        assert cp.outcome["additional_segments"] == []
        review = services.complete_checkpoint(db, pid, cpid)
        record = review.record
        assert record["closed_segments"] == ["p1", "p2"]
        assert record["keep_open_segments"] == ["p3"]
        assert record["computation_id"] == cid1
        assert record["witness"]["total_cost"] == 10  # 冻结的第 1 版费用
    finally:
        db.close()

    # 重启读取：全新会话按 ID 读回复核记录，检查点终态一致
    db = SessionLocal()
    try:
        reread = services.get_review_or_404(db, pid, record["review_id"])
        assert reread.record == record
        done = services.get_checkpoint_or_404(db, pid, cpid)
        assert done.status == "COMPLETED"
        assert done.review_id == record["review_id"]
        assert done.revision == 4
    finally:
        db.close()

    # 当前采用已切换到第 2 版，但不影响已完成的检查点与复核记录
    assert client.get(f"/plans/{pid}/adoption").json()["computation_id"] == cid2
    got = client.get(f"/plans/{pid}/reviews/{record['review_id']}")
    assert got.status_code == 200
    assert got.json() == record


@pytest.mark.parametrize("round_no", range(3))
def test_parallel_append_fuzz(client, round_no):
    """多轮无协调并行追加同一修订：任何交错下恰一胜一负，最终状态自洽。"""
    pid = f"cp-pg-fuzz-{round_no}"
    adopt_plan(client, pid)
    cpid = create_checkpoint(pid)

    barrier = threading.Barrier(2)
    outcomes = {}

    def worker(name, closed):
        barrier.wait()
        db = SessionLocal()
        try:
            cp = services.append_checkpoint(db, pid, cpid, 1, closed, [])
            outcomes[name] = ("ok", cp.revision, cp.closed_segments)
        except ApiError as exc:
            outcomes[name] = (exc.code, None, None)
        except Exception as exc:
            outcomes[name] = ("INTERNAL_ERROR:" + repr(exc), None, None)
        finally:
            db.close()

    t1 = threading.Thread(target=worker, args=("a", ["p1"]))
    t2 = threading.Thread(target=worker, args=("b", ["p2"]))
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)
    assert set(outcomes) == {"a", "b"}

    codes = {name: value[0] for name, value in outcomes.items()}
    # 只可能是成功或修订冲突；绝不能出现 500
    assert set(codes.values()) <= {"ok", "REVISION_CONFLICT"}, codes
    assert "ok" in codes.values(), codes

    # 最终进度恰为胜者的追加，修订为 2
    final = get_checkpoint_row(pid, cpid)
    assert final.revision == 2
    winner_closed = [v[2] for v in outcomes.values() if v[0] == "ok"]
    assert len(winner_closed) == 1
    assert final.closed_segments == winner_closed[0]
    assert final.closed_segments in (["p1"], ["p2"])
