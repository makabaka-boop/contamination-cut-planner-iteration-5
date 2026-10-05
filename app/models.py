"""持久化模型：方案、计算记录、采用快照与不可变采用历史。"""

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


class ExecutionCheckpoint(Base):
    """逐班执行的累计现场检查点。

    检查点在创建时永久绑定当时生效的 adoption event：直接保存该采用事件
    的计算 ID、方案修订号与完整冻结快照。随后方案编辑或生效采用切换都不
    会影响检查点，所有追加都只按这份快照重算。

    revision 从 1 开始；每次成功追加递增。状态为 COMPLETED 时，同一行与
    reviews 中按同一 checkpoint_id 可读取的不可变复核记录一起冻结。
    """

    __tablename__ = "execution_checkpoints"

    checkpoint_id = Column(String(32), primary_key=True)
    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    adoption_event_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey("adoption_events.id", ondelete="CASCADE"),
        nullable=False,
    )
    computation_id = Column(String(32), nullable=False, index=True)
    plan_revision = Column(Integer, nullable=False)
    revision = Column(Integer, nullable=False)
    status = Column(String(16), nullable=False)  # ACTIVE / COMPLETED
    frozen_snapshot = Column(JSON, nullable=False)
    closed_segments = Column(JSON, nullable=False)
    keep_open_segments = Column(JSON, nullable=False)
    outcome = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)


class Review(Base):
    """现场关闭复核记录：对一次已采用结果的执行复核，只增不改。

    记录冻结两类复核输入（现场已关闭管段、必须保持开启管段）与复核结果
    （新增建议、追加费用、合并后的隔断见证），并标注它针对的已采用计算
    与修订号；全部取自采用快照冻结的同一版本。写入不触碰 plans /
    computations / adoptions / adoption_events，方案随后修订或采用被
    替换都不影响已落库的复核记录，可按 review_id 在重启后读取。

    执行检查点完成时复用本表作为不可变复核记录，并将 review_id 取为
    checkpoint_id；旧的一次性复核接口仍直接写入本表，两者读取接口相同。
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
