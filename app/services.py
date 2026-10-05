"""业务逻辑：方案保存、最小割计算、采用与已采用结果查询。

事务约定：
- 方案校验通过后才写库，非法整版不会改写当前方案；
- 方案保存保证"每次被接受的内容、修订号与响应一一对应"：
  * 携带 expected_revision 时，由单条条件 UPDATE 原子完成
    "比对修订号 + 整版替换"；命中 0 行说明方案不存在
    （404 PLAN_NOT_FOUND）或读取后修订号已被他人推进
    （409 REVISION_CONFLICT），落败写入回滚，不改写现有方案；
  * 未携带 expected_revision 时保持旧语义（存在则整版替换、
    不存在则新建），但替换同样由单条
    UPDATE ... SET revision = revision + 1 原子完成，并发替换
    绝不会拿到相同的新修订号；新建依赖主键唯一约束，并发首次
    创建同一方案时落败事务回滚并以 409 REVISION_CONFLICT 拒绝，
    绝不把数据库异常暴露给调用方；
  * 成功响应取自本事务提交前持行锁重读的同一行（新建则取本次
    写入值本身），因此响应就是本次提交的内容，修订号全局唯一；
- 计算失败仅落一条 FAILED 记录，不触碰方案与已采用结果；
- 成功计算在创建时冻结计算时刻的方案修订号、方案负载与最小割结果，
  三者共同组成不可混合的快照；采用时只使用该冻结快照，绝不读取
  "当前方案"，因此计算后方案被修订也不会污染快照；修订号与方案
  内容一一对应后，计算记录可始终追溯到确定版本；
- 采用历史 adoption_events 对 computation_id 永久唯一，保证
  "每个成功计算至多采用一次"——即使该采用后来被其他计算替换，
  原计算仍不可再次采用；
- 采用事务先对 plans 行加行锁串行化并发采用：仅当当前生效采用在
  持锁前后一致时才允许写入，否则以 409 ADOPTION_CONFLICT 拒绝，
  保证并发采用的返回结果确定且与最终记录一致。任何失败/冲突都
  回滚，当前方案与原采用快照不变。
- 复核只读取当前生效采用快照中冻结的方案（不读取当前方案），两类现场
  约束（已关闭、必须保持开启）的存在性/互斥校验、约束网络求解与记录
  落库共享同一冻结版本；已关闭边视为移除，保持开启边视为不可切断。
  校验失败或约束使方案不可执行都在写入前拒绝，单行写入失败整体回滚，
  绝不留下半条复核记录，也不修改方案、计算记录与采用快照。
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from . import models
from .errors import ApiError
from .flow import ResidualCutInfeasible, solve_min_cut, solve_residual_min_cut
from .validation import validate_review_segments_known


def get_plan_or_404(db, plan_id):
    plan = db.get(models.Plan, plan_id)
    if plan is None:
        raise ApiError(404, "PLAN_NOT_FOUND", f"plan {plan_id!r} does not exist")
    return plan


def _reread_plan_in_transaction(db, plan_id):
    """在本写入事务内重读方案行。

    替换路径的 UPDATE 已持有该行行锁直至提交，此处读到的即本次
    事务将要提交的修订号与负载；直接以它构造响应，杜绝"提交后被
    他人覆盖、响应却属于另一版本"。
    """
    return (
        db.query(models.Plan)
        .filter(models.Plan.plan_id == plan_id)
        .populate_existing()
        .one()
    )


def save_plan(db, plan_id, canonical_payload, expected_revision=None):
    """保存（新建或整版替换）方案；payload 必须先通过校验。

    每次被接受的写入都对应唯一的新修订号，成功响应的内容就是本次
    事务提交的内容：

    - 携带 expected_revision：仅当当前修订号与之相等时才整版替换，
      "比对修订号 + 写入"由单条条件 UPDATE 原子完成。命中 0 行时
      方案不存在（404 PLAN_NOT_FOUND）或修订号已被并发请求推进
      （409 REVISION_CONFLICT，details 中带 REVISION_MISMATCH），
      落败写入回滚，不改写现有方案；
    - 未携带 expected_revision（或显式为 null）：保持旧语义——存在
      则整版替换、否则新建。替换由单条
      ``UPDATE ... SET revision = revision + 1`` 原子完成，两个并发
      替换因此一定得到不同修订号；新建依赖主键唯一约束，并发首次
      创建同一方案时落败事务回滚并以 409 REVISION_CONFLICT
      （details 中带 PLAN_ALREADY_EXISTS）拒绝，绝不暴露数据库异常。
    """
    if expected_revision is not None:
        result = db.execute(
            update(models.Plan)
            .where(
                models.Plan.plan_id == plan_id,
                models.Plan.revision == expected_revision,
            )
            .values(
                revision=expected_revision + 1,
                payload=canonical_payload,
            )
        )
        if result.rowcount == 0:
            # 条件 UPDATE 未命中：行不存在或修订号已被他人推进。
            # 先回滚释放可能持有的行锁，再在新事务中区分两种情况。
            db.rollback()
            if db.get(models.Plan, plan_id) is None:
                raise ApiError(
                    404, "PLAN_NOT_FOUND", f"plan {plan_id!r} does not exist"
                )
            raise ApiError(
                409,
                "REVISION_CONFLICT",
                f"plan {plan_id!r} was modified after revision "
                f"{expected_revision} was read; re-read the current plan and retry",
                details=[
                    {
                        "code": "REVISION_MISMATCH",
                        "field": "expected_revision",
                        "message": (
                            f"expected revision {expected_revision} no longer "
                            "matches the current revision"
                        ),
                    }
                ],
            )
        plan = _reread_plan_in_transaction(db, plan_id)
        db.commit()
        return plan

    # 未携带修订号：旧语义的"存在则整版替换，否则新建"。
    result = db.execute(
        update(models.Plan)
        .where(models.Plan.plan_id == plan_id)
        .values(
            revision=models.Plan.revision + 1,
            payload=canonical_payload,
        )
    )
    if result.rowcount == 0:
        # 行尚不存在：尝试新建。revision=1 与本次提交的负载即响应。
        plan = models.Plan(plan_id=plan_id, revision=1, payload=canonical_payload)
        db.add(plan)
        try:
            db.commit()
        except IntegrityError:
            # 并发首次创建同一方案：主键唯一约束拒绝落败事务。
            # 回滚后报告明确冲突，绝不改写已由胜者落库的方案。
            db.rollback()
            raise ApiError(
                409,
                "REVISION_CONFLICT",
                f"plan {plan_id!r} already exists; re-read the current plan "
                "and retry with expected_revision",
                details=[
                    {
                        "code": "PLAN_ALREADY_EXISTS",
                        "field": "expected_revision",
                        "message": f"plan {plan_id!r} was created concurrently",
                    }
                ],
            )
        return plan
    plan = _reread_plan_in_transaction(db, plan_id)
    db.commit()
    return plan


def compute(db, plan_id):
    """对当前方案执行最小割计算并持久化计算记录。

    计算记录冻结计算时刻的方案修订号 (plan_revision)、方案负载
    (plan_payload) 与最小割结果 (result)；之后方案再被修订也不影响
    该记录，采用时三者始终属于同一版本。
    """
    plan = get_plan_or_404(db, plan_id)
    computation_id = uuid.uuid4().hex
    # 冻结快照输入，避免与随后的写操作共享可变引用
    plan_payload = plan.payload
    plan_revision = plan.revision
    try:
        result = solve_min_cut(plan_payload)
    except Exception as exc:  # 已校验输入不应失败；兜底记录失败
        computation = models.Computation(
            computation_id=computation_id,
            plan_id=plan.plan_id,
            plan_revision=plan_revision,
            plan_payload=plan_payload,
            status="FAILED",
            result=None,
            error={"code": "INTERNAL_ERROR", "message": str(exc)},
        )
        db.add(computation)
        db.commit()
        raise ApiError(500, "COMPUTATION_FAILED", "min-cut computation failed")
    computation = models.Computation(
        computation_id=computation_id,
        plan_id=plan.plan_id,
        plan_revision=plan_revision,
        plan_payload=plan_payload,
        status="SUCCESS",
        result=result,
        error=None,
    )
    db.add(computation)
    db.commit()
    db.refresh(computation)
    return computation


def get_computation_or_404(db, plan_id, computation_id):
    computation = db.get(models.Computation, computation_id)
    if computation is None or computation.plan_id != plan_id:
        raise ApiError(
            404,
            "COMPUTATION_NOT_FOUND",
            f"computation {computation_id!r} does not exist for plan {plan_id!r}",
        )
    return computation


def _get_adoption_row(db, plan_id):
    return db.get(models.Adoption, plan_id)


def _get_adoption_event(db, computation_id):
    return (
        db.query(models.AdoptionEvent)
        .filter(models.AdoptionEvent.computation_id == computation_id)
        .first()
    )


def adopt(db, plan_id, computation_id):
    """采用一次成功计算，保存计算时刻冻结的完整快照。

    规则：
    - 只能采用本方案状态为 SUCCESS 的计算；
    - 每个成功计算至多采用一次（adoption_events 永久唯一），即使其
      先前采用已被替换，再次采用仍返回 409 COMPUTATION_ALREADY_ADOPTED；
    - 新采用替换该方案当前生效采用；
    - 并发采用由 plans 行锁串行化；若当前生效采用在等待锁期间被他人
      改变，则本请求以 409 ADOPTION_CONFLICT 失败，绝不出现"响应成功
      但最终记录属于另一次采用"或 500。
    """
    # ---- 加锁前校验（不与当前方案/修订发生任何关联，只读计算快照）----
    get_plan_or_404(db, plan_id)
    computation = get_computation_or_404(db, plan_id, computation_id)
    if computation.status != "SUCCESS":
        raise ApiError(
            409,
            "COMPUTATION_NOT_ADOPTABLE",
            f"computation {computation_id!r} did not succeed",
        )
    if _get_adoption_event(db, computation_id) is not None:
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )

    # 并发采用同一方案时，以当前生效采用的 computation_id 作为一致性令牌。
    # 首次采用时令牌为 None（此时尚无生效采用）。
    before_row = _get_adoption_row(db, plan_id)
    before_token = before_row.computation_id if before_row is not None else None

    # ---- 对方案行加锁，串行化同一方案上的所有采用 ----
    # SQLite 不支持 SELECT ... FOR UPDATE，SQLAlchemy 对其退化为普通查询；
    # 并发验收在真实 PostgreSQL 上进行。
    plan = (
        db.query(models.Plan)
        .filter(models.Plan.plan_id == plan_id)
        .with_for_update()
        .populate_existing()
        .one()
    )

    # ---- 持锁后复查（顺序有意义）----
    # 1) 同一计算并发重复采用：精确报 COMPUTATION_ALREADY_ADOPTED；
    if _get_adoption_event(db, computation_id) is not None:
        db.rollback()
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )
    # 2) 不同计算并发首次采用：等待期间生效采用若被改变，属并发冲突。
    after_row = _get_adoption_row(db, plan_id)
    after_token = after_row.computation_id if after_row is not None else None
    if after_token != before_token:
        db.rollback()
        raise ApiError(
            409,
            "ADOPTION_CONFLICT",
            "a concurrent adoption changed the current adopted result; "
            "re-query and retry with the intended computation",
        )

    adopted_at = datetime.now(timezone.utc)
    # 快照完全来自计算记录冻结的内容：计算时刻的修订号、方案负载与
    # 最小割结果同属一个版本，绝不使用当前 plan 的修订号或负载。
    snapshot = {
        "plan_id": plan.plan_id,
        "plan_revision": computation.plan_revision,
        "computation_id": computation_id,
        "adopted_at": adopted_at.isoformat(),
        "plan": computation.plan_payload,
        "result": computation.result,
    }

    event = models.AdoptionEvent(
        plan_id=plan.plan_id,
        computation_id=computation_id,
        plan_revision=computation.plan_revision,
        snapshot=snapshot,
        adopted_at=adopted_at,
    )
    db.add(event)
    if after_row is None:
        adoption = models.Adoption(
            plan_id=plan_id,
            computation_id=computation_id,
            snapshot=snapshot,
            adopted_at=adopted_at,
        )
        db.add(adoption)
    else:
        adoption = after_row
        adoption.computation_id = computation_id
        adoption.snapshot = snapshot
        adoption.adopted_at = adopted_at

    try:
        db.commit()
    except IntegrityError:
        # 极端竞态（历史唯一约束）下的最终防线：回滚，现状不变
        db.rollback()
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )
    db.refresh(adoption)
    return adoption


def get_adoption(db, plan_id):
    adoption = db.get(models.Adoption, plan_id)
    if adoption is None:
        raise ApiError(
            404,
            "ADOPTION_NOT_FOUND",
            f"plan {plan_id!r} has no adopted result",
        )
    return adoption


def review(db, plan_id, closed_ids, keep_open_ids=None):
    """对当前已采用结果执行一次现场关闭复核并持久化复核记录。

    - 复核只读取已采用快照中冻结的方案（采用时刻的版本），绝不读取
      "当前方案"：后来修订的同名管段费用不会混入，算法、校验、持久化
      与接口共享同一冻结版本；
    - 现场已关闭管段视为已移除，必须保持开启管段视为不可切断；在其余
      有向边中沿用同一最小割裁决（最低费用、源侧按包含关系最小、字典序
      升序）求追加关闭费用最小的隔断。若保持开启约束仍留下污染源到保护
      区的路径，则以 422 REVIEW_NOT_EXECUTABLE 明确拒绝且不写复核记录；
    - 复核记录冻结两类现场约束与结果，单行整体一次提交：任何校验失败
      （未知/重复管段、两类约束重叠、无采用结果、不可执行）都在写入前
      拒绝，写入失败整体回滚，绝不留下半条复核记录，也不修改原方案、
      计算记录或采用快照。
    """
    get_plan_or_404(db, plan_id)
    adoption = get_adoption(db, plan_id)
    snapshot = adoption.snapshot
    frozen_plan = snapshot["plan"]
    # 未知管段（含冻结版本之外、后来修订才出现的 ID）在写入前拒绝
    validate_review_segments_known(closed_ids, keep_open_ids, frozen_plan)

    try:
        outcome = solve_residual_min_cut(frozen_plan, closed_ids, keep_open_ids)
    except ResidualCutInfeasible as exc:
        db.rollback()
        raise ApiError(
            422,
            "REVIEW_NOT_EXECUTABLE",
            str(exc),
            [
                {
                    "code": "REQUIRED_OPEN_PATH",
                    "field": "keep_open_segments",
                    "message": (
                        "required-open segments still connect a pollution source "
                        "to a protected zone"
                    ),
                }
            ],
        )

    created_at = datetime.now(timezone.utc)
    review_id = uuid.uuid4().hex
    record = {
        "review_id": review_id,
        "plan_id": plan_id,
        "plan_revision": snapshot["plan_revision"],
        "computation_id": snapshot["computation_id"],
        "created_at": created_at.isoformat(),
        "closed_segments": outcome["closed_segments"],
    }
    if keep_open_ids is not None:
        record["keep_open_segments"] = outcome["keep_open_segments"]
    record.update(
        {
            "additional_segments": outcome["additional_segments"],
            "additional_cost": outcome["additional_cost"],
            "witness": outcome["witness"],
        }
    )
    row = models.Review(
        review_id=review_id,
        plan_id=plan_id,
        computation_id=snapshot["computation_id"],
        plan_revision=snapshot["plan_revision"],
        record=record,
        created_at=created_at,
    )
    db.add(row)
    try:
        db.commit()
    except Exception:
        # 写入失败整体回滚：绝不留下半条复核记录，现状不变
        db.rollback()
        raise ApiError(500, "INTERNAL_ERROR", "failed to persist the review")
    return row


def get_review_or_404(db, plan_id, review_id):
    review = db.get(models.Review, review_id)
    if review is None or review.plan_id != plan_id:
        raise ApiError(
            404,
            "REVIEW_NOT_FOUND",
            f"review {review_id!r} does not exist for plan {plan_id!r}",
        )
    return review
