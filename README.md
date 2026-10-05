# 洁净厂房通风管段最小费用隔断服务

洁净厂房发生局部污染时，设施工程师需要关闭一组**定向通风管段**，使所有
**污染源区域**都无法到达任何**保护区域**，并把停产代价（关闭费用之和）
降到最低。本服务在**所有最低费用割**中，返回**源侧区域集合按包含关系
最小**的唯一方案，保证：无论管段提交顺序如何、服务是否重启，工程师读到
的都是同一份可直接执行、确实隔断全部污染路径的最低费用清单。

纯后端服务：FastAPI 提供普通 JSON 接口，PostgreSQL 持久化方案与采用快照，
最大流/最小割算法为服务自行实现（64 位整数容量的 Dinic），不依赖任何
图算法库，也不返回占位结果。

## 快速开始

```bash
docker compose up --build
```

- API 监听 `http://localhost:8000`（`GET /health` 健康检查）。
- PostgreSQL 数据保存在命名数据卷 `pgdata` 中，服务重启后方案与已采用
  快照仍然保留。
- 数据库连接可通过环境变量 `DATABASE_URL` 覆盖（compose 中已指向 `db`
  服务）。

## 算法说明

建模：超级源 → 每个污染源（容量 INF）、每个保护区 → 超级汇（容量 INF）、
每条管段为一条有向边（容量 = 关闭费用）。INF = 全部管段费用之和 + 1，
严格大于任何真实割，因此最小割绝不会切断 INF 边。

- 用 Dinic 算法求最大流（容量为 64 位整数，总费用上限
  2000 × 10⁹ = 2×10¹² ≪ 2⁶³）。
- 最大流完成后，残量网络中从超级源可达的节点集合，恰好是**所有最小割
  源侧的交集**——即按包含关系最小的唯一源侧；对该集合取"源侧 → 汇侧"
  的管段，得到唯一的最小切断清单。
- 结果与管段/区域的提交顺序无关：`source_zones`、`cut_segments` 均按
  字典序升序返回，`total_cost` 为精确整数。

## 数据约束

| 项 | 约束 |
| --- | --- |
| 区域 ID / 管段 ID | 匹配 `[A-Za-z0-9_-]{1,32}`，各自唯一 |
| 区域数 | ≤ 300（`zones` 缺省时从管段端点与污染源/保护区推导） |
| 管段数 | ≤ 2000 |
| 管段 | `from`、`to` 必须引用现有区域，方向不可反转；`cost` 为 0 至 10⁹ 的整数 |
| 污染源 / 保护区 | 均非空、互不相交、只能引用现有区域 |

## 接口说明

所有错误响应均为统一信封，错误代码稳定：

```json
{"error": {"code": "VALIDATION_ERROR", "message": "...", "details": [{"code": "EMPTY_SOURCES", "field": "sources", "message": "..."}]}}
```

| 接口 | 说明 |
| --- | --- |
| `PUT /plans/{plan_id}` | 保存（新建或整版替换）方案；可携带 `expected_revision` 声明读取到的修订号，仅当当前修订号一致才接受（乐观并发控制，冲突返回 409）；非法负载返回 422 且**不改写**当前方案 |
| `GET /plans/{plan_id}` | 查询当前方案 |
| `POST /plans/{plan_id}/computations` | 对当前方案计算最小费用隔断，返回计算记录 |
| `GET /plans/{plan_id}/computations/{computation_id}` | 查询计算记录 |
| `POST /plans/{plan_id}/adopt` | 采用一次**成功**计算并保存该计算时刻冻结的完整快照；同一计算**至多采用一次**，即使后来被其他计算替换也不可再次采用；并发采用冲突返回 409 |
| `GET /plans/{plan_id}/adoption` | 查询当前已采用结果（完整快照） |
| `POST /plans/{plan_id}/reviews` | 对当前已采用结果执行**现场关闭复核**：提交现场确实已关闭的管段 ID 集合，并可选提交必须保持开启的管段 ID 集合；在采用快照冻结方案中，把已关闭边移除、把保持开启边视为不可切断，再在其余边上求追加关闭费用最小的隔断；复核记录冻结输入与结果，不修改方案、计算记录或采用快照 |
| `GET /plans/{plan_id}/reviews/{review_id}` | 按 ID 查询复核记录（冻结的输入与结果，重启后仍可读取） |

### 调用示例

保存方案：

```bash
curl -X PUT http://localhost:8000/plans/demo \
  -H 'content-type: application/json' \
  -d '{
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": [
      {"id": "p1", "from": "SRC1", "to": "MID",   "cost": 4},
      {"id": "p2", "from": "SRC2", "to": "MID",   "cost": 6},
      {"id": "p3", "from": "MID",  "to": "SAFE1", "cost": 5},
      {"id": "p4", "from": "MID",  "to": "SAFE2", "cost": 7}
    ],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"]
  }'
```

```json
{"plan_id": "demo", "revision": 1, "plan": {"zones": ["..."], "segments": ["..."], "sources": ["SRC1", "SRC2"], "protections": ["SAFE1", "SAFE2"]}}
```

带修订号令牌的整版替换（仅当当前修订号仍为 `1` 时才接受）：

```bash
curl -X PUT http://localhost:8000/plans/demo \
  -H 'content-type: application/json' \
  -d '{
    "expected_revision": 1,
    "zones": ["..."],
    "segments": ["..."],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"]
  }'
```

成功时修订号原子推进为 `2`；若读取后方案已被他人修改，则返回
`409 REVISION_CONFLICT`（`details[].code = REVISION_MISMATCH`），
本次写入回滚、不改写现有方案，调用方重新读取后按新修订号重试即可。

计算最小费用隔断：

```bash
curl -X POST http://localhost:8000/plans/demo/computations
```

```json
{
  "computation_id": "6dc73aa10a0d4ee897af3b6abc5d93d7",
  "plan_id": "demo",
  "plan_revision": 1,
  "status": "SUCCESS",
  "result": {"source_zones": ["SRC1", "SRC2"], "cut_segments": ["p1", "p2"], "total_cost": 10},
  "error": null
}
```

采用该计算结果（保存计算时刻冻结的完整快照；同一 `computation_id`
再次采用——即便其采用结果已被其他计算替换——返回
`409 COMPUTATION_ALREADY_ADOPTED`）：

```bash
curl -X POST http://localhost:8000/plans/demo/adopt \
  -H 'content-type: application/json' \
  -d '{"computation_id": "6dc73aa10a0d4ee897af3b6abc5d93d7"}'
```

查询已采用结果：

```bash
curl http://localhost:8000/plans/demo/adoption
```

```json
{
  "plan_id": "demo",
  "plan_revision": 1,
  "computation_id": "6dc73aa10a0d4ee897af3b6abc5d93d7",
  "adopted_at": "2026-09-22T17:03:34.619220+00:00",
  "plan": {"zones": ["..."], "segments": ["..."], "sources": ["SRC1", "SRC2"], "protections": ["SAFE1", "SAFE2"]},
  "result": {"source_zones": ["SRC1", "SRC2"], "cut_segments": ["p1", "p2"], "total_cost": 10}
}
```

### 施工中途的现场关闭复核

隔断方案施工到一半时，工程师提交现场**确实已关闭**的管段 ID 集合；
若几段通风管必须保持开启，还可同时提交 `keep_open_segments`。服务把
已关闭边视为已移除、把保持开启边视为不可切断，在**该次采用快照冻结的
方案**上（绝不读取当前方案，后来修订的同名管段费用不会混入）求追加
关闭费用最小的隔断，算法与初始计算完全一致（Dinic 最大流 + 最小源侧
交集 + 字典序排序）。

```bash
curl -X POST http://localhost:8000/plans/demo/reviews \
  -H 'content-type: application/json' \
  -d '{"closed_segments": ["p1"], "keep_open_segments": ["p2"]}'
```

```json
{
  "review_id": "9f2c7b1e...",
  "plan_id": "demo",
  "plan_revision": 1,
  "computation_id": "6dc73aa10a0d4ee897af3b6abc5d93d7",
  "created_at": "2026-09-26T08:00:00+00:00",
  "closed_segments": ["p1"],
  "keep_open_segments": ["p2"],
  "additional_segments": ["p3", "p4"],
  "additional_cost": 12,
  "witness": {
    "source_zones": ["MID", "SRC1", "SRC2"],
    "cut_segments": ["p1", "p3", "p4"],
    "total_cost": 16
  }
}
```

- `closed_segments`：现场已关闭（升序回显）；
- `keep_open_segments`：可选的现场必须保持开启管段（升序回显并冻结）；
  未提供该字段时旧响应不包含此字段；显式提供空列表时冻结空约束；
- `additional_segments` / `additional_cost`：只能从“未关闭且未要求保持
  开启”的其余管段中选择的新增建议与追加费用；若现场关闭已足以隔断全部
  污染路径，则清单为空、费用为 0；
- `witness`：合并后的隔断见证——源侧区域、`已关闭 ∪ 新增` 的完整
  清单及其按冻结方案费用计算的总费用；保持开启管段不会进入见证清单；
  删除见证清单中的管段后，冻结方案中不存在任何污染源到保护区的路径。

如果仅由保持开启管段组成的路径仍连通污染源与保护区，则无论关闭其余
哪些边都无法执行；服务返回 `422 REVIEW_NOT_EXECUTABLE`
（`details[].code = REQUIRED_OPEN_PATH`），且**不写复核记录**。

复核记录只增不改：方案随后修订、采用被其他计算替换，都不影响已
落库的复核记录，仍可按 `review_id` 读取；其中 `plan_revision` 与
`computation_id` 标明它针对的是哪一次冻结版本。未知管段（含后来
修订才新增的 ID）、重复管段、两类约束引用同一管段、保持开启导致不可
执行、无采用结果都以 422/404 拒绝且**不留任何记录**；数据库写入失败
整体回滚（500），不留下半条复核记录，也绝不修改原方案、计算记录或
采用快照。

### 错误代码

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | `INVALID_JSON` | 请求体不是合法 JSON |
| 404 | `PLAN_NOT_FOUND` / `COMPUTATION_NOT_FOUND` / `ADOPTION_NOT_FOUND` / `REVIEW_NOT_FOUND` / `NOT_FOUND` | 资源不存在 |
| 409 | `REVISION_CONFLICT` | 保存冲突，`details[].code` 区分：`PLAN_ALREADY_EXISTS`（并发首次创建同一方案，落败请求）/ `REVISION_MISMATCH`（携带的 `expected_revision` 已过期，方案在读取后被他人修改）；重新读取当前方案与修订号后再提交 |
| 409 | `COMPUTATION_ALREADY_ADOPTED` / `COMPUTATION_NOT_ADOPTABLE` / `ADOPTION_CONFLICT` | 计算已被采用过（含已被替换下来的历史采用）/ 计算未成功 / 并发采用时当前生效结果已被他人改变，请重新查询后再采用 |
| 422 | `VALIDATION_ERROR` | 负载非法，`details[].code` 给出细分原因（如 `INVALID_ZONE_ID`、`DUPLICATE_SEGMENT_ID`、`UNKNOWN_ZONE`、`INVALID_COST`、`EMPTY_SOURCES`、`SOURCE_PROTECTION_OVERLAP`、`TOO_MANY_ZONES`、`TOO_MANY_SEGMENTS`、`INVALID_EXPECTED_REVISION`、`UNKNOWN_SEGMENT`、`INVALID_CLOSED_SEGMENTS_FIELD`、`INVALID_KEEP_OPEN_SEGMENTS_FIELD`、`CLOSED_KEEP_OPEN_OVERLAP` 等） |
| 422 | `REVIEW_NOT_EXECUTABLE` | 复核的保持开启约束保留了污染源到保护区的路径，关闭其余边也无法隔断（`details[].code = REQUIRED_OPEN_PATH`），不写复核记录 |
| 500 | `INTERNAL_ERROR` / `COMPUTATION_FAILED` | 服务内部错误 |

非法整版、计算失败、采用不存在或已采用过的结果，以及并发保存的落败
写入，都**不会**改写当前方案或已采用结果。

### 修订号与并发保存

每次被接受的保存都对应唯一的修订号（新建为 `1`，此后每次接受的整版
替换原子地 `+1`），且成功响应中的修订号与方案内容就是本次事务提交的
同一行——不存在"两个请求返回同一新修订号"或"响应与本次提交不符"。

- 请求体中的 `expected_revision` 是可选的乐观并发令牌：为正整数时，
  保存退化为单条原子的条件 UPDATE
  （`SET revision = revision + 1 ... WHERE revision = expected_revision`），
  "比对修订号 + 写入"在数据库内一次完成。两个都读到修订 `N` 的并发
  请求因此恰有一个被接受为 `N+1`，另一个得到
  `409 REVISION_CONFLICT` / `REVISION_MISMATCH`；
- 两个请求同时首次创建同一方案时，主键唯一约束保证只有一条写入成功，
  落败事务回滚并返回 `409 REVISION_CONFLICT` / `PLAN_ALREADY_EXISTS`，
  绝不把数据库唯一约束异常暴露成 500；
- 不携带（或显式传 `null`）`expected_revision` 时保持旧语义：存在则
  整版替换、否则新建。替换同样是原子的 `revision + 1`，因此并发的
  无令牌替换也一定得到互不相同的修订号，每次响应仍与各自的提交一一
  对应；需要"修改不被静默覆盖"的调用方应携带 `expected_revision`；
- 计算记录冻结的 `plan_revision` 因此始终对应唯一的一份方案负载，
  后续计算与采用都能追溯到确定版本。

### 快照一致性与并发采用

计算记录在创建时即**冻结**三件同属一个版本的内容：来源方案负载、来源
修订号 (`plan_revision`) 与最小割结果。采用时快照只取自计算记录，绝不
读取"当前方案"，因此先计算、再修订方案、再采用旧计算时，快照中的方案、
修订号、切断管段与总费用仍然彼此匹配，清单也确实隔断的是该版快照中的
全部污染路径。

每个成功计算至多产生一条不可变的采用历史（`adoption_events`，
`computation_id` 永久唯一）：它先被采用、再被另一个计算替换后，仍不能
再次被采用。同一方案上的并发采用由方案行锁串行化：首个采用成功，与其
并发的首次采用返回 `409 ADOPTION_CONFLICT`（不会出现 500，也不会把行
竞争误报为"计算已采用"）；成功响应与随后 `GET .../adoption` 查到的最终
记录逐项一致。

## 测试

```bash
pip install -r requirements-dev.txt
pytest
```

- `tests/test_exhaustive.py`：对不超过 8 个区域的**全部合法划分**
  （污染源、保护区非空且互不相交，共 3ⁿ − 2ⁿ⁺¹ + 1 种）与暴力枚举所有
  源侧集合对拍，并验证删除切断管段后不存在任何污染源到保护区的路径。
- `tests/test_flow.py`：并列割取源侧最小、多源多汇、零费用割、超过
  32 位（直至 2×10¹²）的总费用、自环与提交顺序无关性。
- `tests/test_api.py`：保存/计算/采用/查询全流程、稳定错误码、非法
  操作不改写既有数据、结果确定性，以及 `expected_revision` 乐观
  并发令牌的串行契约（接受/冲突/404/非法令牌 422）。
- `tests/test_snapshot.py`：计算后修订再采用时快照的方案/修订/清单/费用
  逐项冻结且能隔断快照内污染路径；采用被替换后旧计算不可重用；历史采用
  次数核对；冲突不改写当前方案与生效快照。
- `tests/test_review.py`：现场关闭复核——小图独立枚举对拍（全部合法
  划分 × 多组已关闭子集，并枚举每条边“已关闭 / 必须保持开启 / 可追加
  切断”的状态组合）、非建议管段先被关闭、零费用边、保持开启约束与
  “已隔断则新增为空”、不可执行、方案修订后复核仍使用冻结版本且不改写
  方案/计算/采用快照、复核记录按 ID 读取（含 8 线程并发读取一致），以及
  未知/重复管段、两类约束重叠、无采用结果、数据库写入失败都不留下半条
  记录。
- `tests/test_concurrency_pg.py`：**仅在真实 PostgreSQL 下运行**
  （SQLite 自动跳过），在数据库层确定性地卡住两次操作：
  - 并发首次创建同一方案：一胜（200 修订 1）一负（409
    `REVISION_CONFLICT` / `PLAN_ALREADY_EXISTS`），无 500，落败
    写入不改写胜者方案；
  - 并发整版覆盖（都读到同一修订）：恰一胜（修订号唯一推进，响应
    等于其提交）一负（409 `REVISION_CONFLICT` / `REVISION_MISMATCH`），
    落败内容不可见；随后计算与采用的版本/内容/错误结构逐项自洽；
  - 并发首次采用不同计算：一胜（200）一负（409 `ADOPTION_CONFLICT`）、
    无 500/无误报、成功响应与最终记录一致、历史采用次数为 1；
  - 另含保存与采用的多轮无协调并行对拍。

测试默认使用 SQLite 内存库；设置 `TEST_DATABASE_URL` 可指向 PostgreSQL
进行对拍。

## 目录结构

```
app/
  main.py        FastAPI 路由与全局异常处理
  flow.py        自实现 64 位整数容量 Dinic 最大流 / 最小割（含剩余网络复核）
  validation.py  方案负载与复核负载的域校验（稳定错误码）
  services.py    保存、计算、采用、复核、查询业务逻辑
  models.py      plans / computations / adoptions / adoption_events / reviews 表
  db.py          引擎、会话、建表（带重试）
  errors.py      统一错误信封
tests/           穷举对拍 + 单元 + 接口 + 快照 + 复核 + PostgreSQL 并发验收
```
