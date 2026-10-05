"""洁净厂房通风管段最小费用隔断服务 —— HTTP 接口层。"""

from contextlib import asynccontextmanager
from datetime import timezone

from anyio import to_thread
from fastapi import Depends, FastAPI, Path, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import services
from .db import get_db, init_db
from .errors import ApiError, error_body
from .validation import (
    validate_checkpoint_append_payload,
    validate_plan_payload,
    validate_review_payload,
)

PLAN_ID_REGEX = r"^[A-Za-z0-9_-]{1,64}$"


@asynccontextmanager
async def lifespan(_app):
    init_db()
    yield


app = FastAPI(
    title="Cleanroom Ventilation Cut Service",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------- 异常处理：统一错误响应体，错误码稳定 ----------

@app.exception_handler(ApiError)
async def api_error_handler(_request, exc: ApiError):
    return JSONResponse(
        status_code=exc.status_code,
        content=error_body(exc.code, exc.message, exc.details),
    )


@app.exception_handler(RequestValidationError)
async def request_validation_handler(_request, exc: RequestValidationError):
    details = [
        {
            "code": "INVALID_PARAMETER",
            "field": ".".join(str(part) for part in err.get("loc", [])),
            "message": err.get("msg", ""),
        }
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content=error_body("VALIDATION_ERROR", "request validation failed", details),
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(_request, exc: StarletteHTTPException):
    code = {
        404: "NOT_FOUND",
        405: "METHOD_NOT_ALLOWED",
    }.get(exc.status_code, "HTTP_ERROR")
    return JSONResponse(
        status_code=exc.status_code,
        content=error_body(code, str(exc.detail)),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=error_body("INTERNAL_ERROR", "unexpected server error"),
    )


# ---------- 工具 ----------

async def _json_body(request: Request):
    try:
        return await request.json()
    except Exception:
        raise ApiError(400, "INVALID_JSON", "request body is not valid JSON")


def _plan_view(plan):
    return {"plan_id": plan.plan_id, "revision": plan.revision, "plan": plan.payload}


def _parse_expected_revision(body):
    """从请求体提取可选的 ``expected_revision``（乐观并发令牌）。

    - 缺省或显式为 null：不声明修订号，沿用"存在则整版替换、否则
      新建"的旧语义；
    - 正整数：仅当当前修订号与之相等时才接受整版替换，冲突由持久
      层原子判定并以 409 REVISION_CONFLICT 返回；
    - 其他类型/非正数：422 VALIDATION_ERROR（INVALID_EXPECTED_REVISION）。
    """
    if not isinstance(body, dict):
        return None  # 非对象负载由 validate_plan_payload 统一以 422 拒绝
    if "expected_revision" not in body or body["expected_revision"] is None:
        return None
    value = body["expected_revision"]
    # bool 是 int 的子类，必须显式排除
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ApiError(
            422,
            "VALIDATION_ERROR",
            "expected_revision must be a positive integer",
            [
                {
                    "code": "INVALID_EXPECTED_REVISION",
                    "field": "expected_revision",
                    "message": "expected_revision must be an integer >= 1",
                }
            ],
        )
    return value


def _computation_view(computation):
    return {
        "computation_id": computation.computation_id,
        "plan_id": computation.plan_id,
        "plan_revision": computation.plan_revision,
        "status": computation.status,
        "result": computation.result,
        "error": computation.error,
    }


def _iso(dt):
    """统一时间戳为 UTC ISO 字符串（SQLite 读出的是 naive，按 UTC 解释）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _checkpoint_view(checkpoint):
    """检查点进度视图：累计约束、最近一次完整复核结果与冻结版本。"""
    outcome = checkpoint.outcome
    return {
        "checkpoint_id": checkpoint.checkpoint_id,
        "plan_id": checkpoint.plan_id,
        "revision": checkpoint.revision,
        "status": checkpoint.status,
        "plan_revision": checkpoint.plan_revision,
        "computation_id": checkpoint.computation_id,
        "created_at": _iso(checkpoint.created_at),
        "updated_at": _iso(checkpoint.updated_at),
        "closed_segments": checkpoint.closed_segments,
        "keep_open_segments": checkpoint.keep_open_segments,
        "additional_segments": outcome["additional_segments"],
        "additional_cost": outcome["additional_cost"],
        "witness": outcome["witness"],
        "review_id": checkpoint.review_id,
    }


# ---------- 接口 ----------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.put("/plans/{plan_id}")
async def save_plan(
    request: Request,
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    db: Session = Depends(get_db),
):
    """保存（新建或整版替换）方案；非法负载不改写当前方案。

    携带 ``expected_revision`` 时仅当当前修订号一致才接受
    （乐观并发控制），否则返回 409 REVISION_CONFLICT 供调用方
    重新读取后重试。
    """
    body = await _json_body(request)
    expected_revision = _parse_expected_revision(body)
    canonical = validate_plan_payload(body)
    # 阻塞式 psycopg2 写事务（可能等待行锁）放到工作线程执行，
    # 使并发保存请求能在数据库层真正并行地争锁，由条件 UPDATE /
    # 唯一约束给出确定结果。
    plan = await to_thread.run_sync(
        services.save_plan, db, plan_id, canonical, expected_revision
    )
    return _plan_view(plan)


@app.get("/plans/{plan_id}")
def get_plan(plan_id: str = Path(pattern=PLAN_ID_REGEX), db: Session = Depends(get_db)):
    return _plan_view(services.get_plan_or_404(db, plan_id))


@app.post("/plans/{plan_id}/computations")
def compute(plan_id: str = Path(pattern=PLAN_ID_REGEX), db: Session = Depends(get_db)):
    """对当前方案计算最小费用隔断（源侧最小的唯一最低费用割）。"""
    return _computation_view(services.compute(db, plan_id))


@app.get("/plans/{plan_id}/computations/{computation_id}")
def get_computation(
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    computation_id: str = "",
    db: Session = Depends(get_db),
):
    services.get_plan_or_404(db, plan_id)
    return _computation_view(
        services.get_computation_or_404(db, plan_id, computation_id)
    )


@app.post("/plans/{plan_id}/adopt")
async def adopt(
    request: Request,
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    db: Session = Depends(get_db),
):
    """采用一次成功计算并保存完整快照；同一计算只能采用一次。"""
    body = await _json_body(request)
    computation_id = body.get("computation_id") if isinstance(body, dict) else None
    if not isinstance(computation_id, str) or not computation_id:
        raise ApiError(
            422,
            "VALIDATION_ERROR",
            "request body must be an object with a computation_id string",
            [
                {
                    "code": "INVALID_COMPUTATION_ID",
                    "field": "computation_id",
                    "message": "non-empty string expected",
                }
            ],
        )
    # services.adopt 是同步的 psycopg2 阻塞事务（内含行锁等待）；
    # 放到工作线程执行，使并发采用请求能在数据库层真正并行地争锁，
    # 由行锁串行化后得到确定结果。
    adoption = await to_thread.run_sync(services.adopt, db, plan_id, computation_id)
    return adoption.snapshot


@app.get("/plans/{plan_id}/adoption")
def get_adoption(
    plan_id: str = Path(pattern=PLAN_ID_REGEX), db: Session = Depends(get_db)
):
    """查询当前已采用结果（完整快照）。"""
    return services.get_adoption(db, plan_id).snapshot


@app.post("/plans/{plan_id}/reviews")
async def create_review(
    request: Request,
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    db: Session = Depends(get_db),
):
    """对当前已采用结果执行现场关闭复核。

    提交现场已关闭管段 ID 集合，以及可选的必须保持开启管段 ID 集合：
    服务把已关闭边视为移除、把保持开启边视为不可切断，只在该次采用快照
    冻结的方案上（不读取后来修订的同名管段）求追加关闭费用最小的隔断，
    返回两类现场约束、新增建议、追加费用与合并后的隔断见证。约束使方案
    不可执行时返回 422 且不写复核记录；复核记录不修改方案、计算记录或
    采用快照。
    """
    body = await _json_body(request)
    closed_ids, keep_open_ids = validate_review_payload(body)
    review = await to_thread.run_sync(
        services.review, db, plan_id, closed_ids, keep_open_ids
    )
    return review.record


@app.get("/plans/{plan_id}/reviews/{review_id}")
def get_review(
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    review_id: str = "",
    db: Session = Depends(get_db),
):
    """按 ID 查询复核记录（冻结的输入与结果，重启后仍可读取）。"""
    services.get_plan_or_404(db, plan_id)
    return services.get_review_or_404(db, plan_id, review_id).record


# ---------- 执行检查点：分班逐次登记，完成时转不可变复核记录 ----------

@app.post("/plans/{plan_id}/checkpoints")
def create_checkpoint(
    plan_id: str = Path(pattern=PLAN_ID_REGEX), db: Session = Depends(get_db)
):
    """从当前已采用结果创建执行检查点。

    检查点永久绑定该采用事件冻结的方案与计算版本；之后编辑方案或采用
    另一计算，本检查点仍只按创建时的冻结快照推进。初始累计约束为空，
    初始复核结果即冻结方案的最小费用隔断。
    """
    return _checkpoint_view(services.create_checkpoint(db, plan_id))


@app.get("/plans/{plan_id}/checkpoints/{checkpoint_id}")
def get_checkpoint(
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    checkpoint_id: str = "",
    db: Session = Depends(get_db),
):
    """查询检查点当前进度（交班者在同一执行进度上继续复核）。"""
    services.get_plan_or_404(db, plan_id)
    return _checkpoint_view(
        services.get_checkpoint_or_404(db, plan_id, checkpoint_id)
    )


@app.post("/plans/{plan_id}/checkpoints/{checkpoint_id}/appends")
async def append_checkpoint(
    request: Request,
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    checkpoint_id: str = "",
    db: Session = Depends(get_db),
):
    """按预期检查点修订追加登记现场管段。

    只能增加管段：新登记的已关闭/保持开启管段不得与累计集合重复或交叉。
    服务在同一事务中按累计约束重算最低追加隔断、费用及见证，并与新修订
    一同保存；约束不可执行（422）、修订过期（409）或写入失败（500）时，
    进度与上一次完整复核结果都不变。
    """
    body = await _json_body(request)
    expected_revision, closed_ids, keep_open_ids = (
        validate_checkpoint_append_payload(body)
    )
    # 条件 UPDATE 可能等待行锁，放到工作线程执行，使并发追加能在
    # 数据库层真正并行地争锁，由条件 UPDATE 给出确定结果。
    checkpoint = await to_thread.run_sync(
        services.append_checkpoint,
        db,
        plan_id,
        checkpoint_id,
        expected_revision,
        closed_ids,
        keep_open_ids,
    )
    return _checkpoint_view(checkpoint)


@app.post("/plans/{plan_id}/checkpoints/{checkpoint_id}/complete")
async def complete_checkpoint(
    plan_id: str = Path(pattern=PLAN_ID_REGEX),
    checkpoint_id: str = "",
    db: Session = Depends(get_db),
):
    """完成检查点：把当前进度转成可按 ID 读取的不可变复核记录。

    复核记录冻结累计约束与最近一次完整复核结果（取自条件更新后持锁
    重读的最终状态），与状态翻转在同一事务中提交；之后该检查点不再
    接受追加。返回的复核记录可用 GET /plans/{plan_id}/reviews/{review_id}
    读取。
    """
    review = await to_thread.run_sync(
        services.complete_checkpoint, db, plan_id, checkpoint_id
    )
    return review.record
