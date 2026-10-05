"""持久化模型：方案、计算记录、采用快照、不可变采用历史与执行检查点。"""

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)

from .db import Base


class Plan(Base):
    __tablename__ = "plans"

    plan_id = Column(String(64), primary_key=True)
    revision = Column(Integer, nullable=False)
    payload = Column(JSON, nullable=False)  # 校验后的规范化方案
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Computation(Base):
    __tablename__ = "computations"

    computation_id = Column(String(32), primary_key=True)
    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    # 计算时刻的方案修订号与方案负载，与 result 共同构成不可混合的快照
    plan_revision = Column(Integer, nullable=False)
    plan_payload = Column(JSON, nullable=False)
    status = Column(String(16), nullable=False)  # SUCCESS / FAILED
    result = Column(JSON, nullable=True)
    error = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Adoption(Base):
    """每个方案当前生效的采用（指向 adoption_events 中最新一条）。"""

    __tablename__ = "adoptions"

    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"), primary_key=True
    )
    # 注意：这里 intentionally 不加 unique —— 同一计算"至多采用一次"
    # 由不可变的 adoption_events.computation_id 唯一约束永久保证；
    # 新采用替换当前采用后，旧计算仍不允许再次被采用。
    computation_id = Column(String(32), nullable=False, index=True)
    snapshot = Column(JSON, nullable=False)  # 方案 + 结果的完整快照
    adopted_at = Column(DateTime(timezone=True), nullable=False)


class AdoptionEvent(Base):
    """不可变采用历史：每个成功计算至多产生一条事件（永久唯一）。"""

    __tablename__ = "adoption_events"

    # SQLite 仅对 INTEGER PRIMARY KEY 自动递增，BigInteger 需在 SQLite
    # 退化为 Integer；PostgreSQL 使用 BIGINT 标识列。
    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    plan_id = Column(
        String(64),
        ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # 一次成功计算最多被采用一次，即使其采用结果后来被其他计算替换
    computation_id = Column(
        String(32),
        ForeignKey("computations.computation_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    plan_revision = Column(Integer, nullable=False)
    snapshot = Column(JSON, nullable=False)  # 采用时刻写入的完整快照
    adopted_at = Column(DateTime(timezone=True), nullable=False)


class Checkpoint(Base):
    """执行检查点：从一次已采用结果创建的逐次登记进度。

    创建时永久绑定该采用事件冻结的方案与计算版本（snapshot），之后方案
    修订或采用被其他计算替换都不影响本检查点——追加只在创建时的冻结
    快照上推进。累计约束（已关闭 / 必须保持开启管段集合）只增不减：
    每次被接受的追加在同一事务中按累计约束重算最低追加隔断、费用及
    见证，并与新修订号一同保存；约束不可执行、修订号过期或写入失败时，
    进度与最近一次完整复核结果都不变。完成后状态转为 COMPLETED 并
    生成不可变复核记录（reviews 表），review_id 指向该记录。
    """

    __tablename__ = "checkpoints"

    checkpoint_id = Column(String(32), primary_key=True)
    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    revision = Column(Integer, nullable=False)  # 创建为 1，每次接受的追加 +1
    status = Column(String(16), nullable=False)  # OPEN / COMPLETED
    # 创建时冻结的采用事件：计算 ID、方案修订号与完整快照，永久绑定
    computation_id = Column(String(32), nullable=False, index=True)
    plan_revision = Column(Integer, nullable=False)
    snapshot = Column(JSON, nullable=False)
    closed_segments = Column(JSON, nullable=False)  # 累计已关闭管段（升序）
    keep_open_segments = Column(JSON, nullable=False)  # 累计保持开启管段（升序）
    outcome = Column(JSON, nullable=False)  # 最近一次完整复核结果
    review_id = Column(String(32), nullable=True)  # 完成时生成的复核记录
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class Review(Base):
    """现场关闭复核记录：对一次已采用结果的执行复核，只增不改。

    记录冻结两类复核输入（现场已关闭管段、必须保持开启管段）与复核结果
    （新增建议、追加费用、合并后的隔断见证），并标注它针对的已采用计算
    与修订号；全部取自采用快照冻结的同一版本。写入不触碰 plans /
    computations / adoptions / adoption_events，方案随后修订或采用被
    替换都不影响已落库的复核记录，可按 review_id 在重启后读取。
    记录有两个来源：一次性复核接口（POST .../reviews），以及执行检查点
    完成时把当前进度整体冻结转存（record 中带 checkpoint_id）。
    """

    __tablename__ = "reviews"

    review_id = Column(String(32), primary_key=True)
    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    # 复核所针对的已采用结果（采用快照中的计算），仅作追溯，不约束
    # 该方案上的采用随后被替换
    computation_id = Column(String(32), nullable=False, index=True)
    plan_revision = Column(Integer, nullable=False)
    record = Column(JSON, nullable=False)  # 冻结的完整输入与结果
    created_at = Column(DateTime(timezone=True), nullable=False)
