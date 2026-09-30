# 本地增量数据接入流程（Local Incremental Ingestion）

用**本地文件存储**模拟源端与目标端，实现一条可预期的增量接入链路：批次幂等提交、
断点续传（不重复、不丢失）、非法位点明确拒绝、表结构变更可兼容，并在同一套机制上
扩展了**结算链路**所需的跨表业务批次整体提交、历史更正与撤回。仅依赖仓库已有的
`fastapi[standard]`，测试使用 Python 标准库 `unittest`，无额外依赖。

## 1. 目录与运行

```
app/
  models.py       领域模型：Batch / Record / RecordResult / RejectReason / SchemaChange
                  BusinessBatch / BusinessOp(UPSERT/RETRACT) / BusinessBatchResult
  storage.py      JSONL 追加存储 + 位点 KV 存储（进程内锁，模拟持久化介质）
  checkpoint.py   位点存储与推进规则（回退 / 基线过期拒绝）
  schema_store.py 表结构与不可变变更流水（INIT/ADD/DROP/RENAME），支持历史版本回放，
                  建表可声明业务键列 key_columns（键列禁止删除/改名）
  target.py       目标端：按记录生产版本对齐到当前结构并落地
  source.py       源端模拟：单调连续位点的可回放 JSONL 日志
  settlement.py   跨表业务批次提交日志（journal）：单次追加即原子提交点、坏尾行截断、
                  按业务键折叠当前结论与历史轨迹
  pipeline.py     编排核心：表锁、幂等、连续前缀提交、留痕、位点推进，
                  以及跨表业务批次（更正/撤回）的整体提交与结论查询
  api.py          FastAPI HTTP 接入层
demo.py           端到端演示脚本
tests/            unittest 测试（运行时打印批次、位点、结构变更、业务批次日志）
data/             默认持久化目录（运行后生成）
```

运行：

```bash
uv sync
uv run python demo.py                 # 端到端演示，产物在 ./data_demo
uv run python -m unittest discover -s tests -v   # 全部测试
uv run fastapi dev main.py            # HTTP 服务（http://127.0.0.1:8000/docs）
```

## 2. 接入协议

### 2.1 数据模型

- 源记录：`Record(offset, data, schema_version)`。`offset` 由源端按表从 **1** 开始
  单调递增、连续不跳号；`schema_version` 是记录生产时的结构版本。
- 批次：`Batch(batch_id, table, records[], schema_version)`。
  - `batch_id` 是**幂等键**；
  - `schema_version` 是调用方认为目标端所处的结构版本（批次头）；
  - 批次内记录位点必须严格 `+1` 连续。

### 2.2 提交规则

1. **幂等**：相同 `(table, batch_id)` 的重复提交直接回放首次结论（`replay=true`），
   目标端不重复写入，位点不二次推进。
2. **位点连续**：批次首位点必须等于当前 `checkpoint + 1`；
   - 小于 → `OFFSET_BACKWARD`；大于 → `OFFSET_GAP`；批次内断号 → `OFFSET_GAP`。
   两类错误均为**整批拒绝（HTTP 409），不写入任何记录，位点不变**。
3. **结构版本**：批次（或记录）的版本不得高于目标当前版本，否则
   `SCHEMA_VERSION_AHEAD` 整批拒绝；旧版本记录允许提交，由目标端做结构对齐。
4. **部分失败不静默**：逐条校验，只落地“首个被拒记录之前的**连续前缀**”；
   首个被拒记录之后的记录即使合法本次也不落地（否则会跳过位点写入），
   统一标记为 `NOT_PROCESSED_AFTER_REJECTION`。每条记录的结论（成功 / 失败原因 /
   详情）都写入批次留痕文件 `batches.jsonl`，可按 `batch_id` 永久追溯。
5. **修复重提**：调用方修正坏记录后，使用**新的 batch_id**、从 `checkpoint+1`
   重新提交；位点只推进到成功前缀末尾，因此不重复、不丢失。

### 2.3 可区分的失败原因（RejectReason）

| 原因 | 级别 | 含义 |
|---|---|---|
| `OFFSET_BACKWARD` | 整批 | 位点回退，或基于过期位点基线提交 |
| `OFFSET_GAP` | 整批 | 首位点越界跳号，或批次内位点不连续 |
| `SCHEMA_VERSION_AHEAD` | 整批/单条 | 基于比目标更新的结构版本 |
| `UNKNOWN_COLUMN` | 单条 | 数据字段不属于记录生产版本的结构（防错位） |
| `TYPE_MISMATCH` | 单条 | 字段值类型不符（`int/str/float/bool`，bool 不等于 int） |
| `DUPLICATE_KEY` | 单条 | 主键冲突（位点连续协议下保留，正常路径不会出现） |
| `NOT_PROCESSED_AFTER_REJECTION` | 单条 | 前序记录已失败，为保位点连续，后续记录本次不处理 |

## 3. 位点语义（Checkpoint）

- 每表一个整数位点，初始 `0`，含义为「**已连续提交的最大源端位点**」。
- 推进采用 CAS：`advance(expected, new)`。
  - `expected != 当前值` → 拒绝（基线已被并发/重启后的其他提交推进）；
  - `new < expected` → 拒绝（回退）。
- 位点与目标行写入在**同一把表锁**内完成，崩溃边界外不会观察到
  “数据已写但位点未推进”或反之。重启后以位点为准从 `checkpoint+1` 拉取即可。
- 位点持久化在 `checkpoints.json`（原子替换写），目标行在 `rows.jsonl`，
  批次留痕在 `batches.jsonl`，结构流水在 `changelog.jsonl`。

## 4. 结构变更兼容规则

- 建表版本为 **1**，初始列定义作为第一条 `INIT` 流水持久化；每次变更版本严格 `+1`，
  流水不可变，任意历史版本的列集合都可通过**回放流水**重建（`columns_at(v)`）。
- **ADD_COLUMN（加列）**：
  - 新增可空列缺省固定为 `null`；
  - 新增必填列**必须显式提供 `default`**，否则 DDL 阶段直接报错
    （缺省语义必须确定且稳定）；
  - 旧版本记录对齐时，用该列的确定缺省补齐。
- **RENAME_COLUMN（改名）**：按版本顺序回放重命名链得到「旧列名→当前列名」映射，
  改名**保持列位置不变**，旧数据按名映射到新列，绝不与同名列错位。
- **DROP_COLUMN（删列）**：旧版本记录中的该列在对齐到新结构时被丢弃、不再写入，
  不会污染后续重新新增的同名列。
- 校验以**记录生产时的版本**为准：记录只允许携带当时结构中存在的字段，
  多余字段判 `UNKNOWN_COLUMN`；旧必填列缺失判 `MISSING_REQUIRED`。
- **并发一致性**：同一张表的批次提交与结构变更由同一把表锁串行化，
  二者结论可线性化：任何提交要么整体发生在某条 DDL 之前，要么之后，
  结果按提交时的结构版本解释，并体现在批次留痕 `schema_version` 中。

## 5. 跨表业务批次与更正/撤回（结算链路）

在不改动单表增量路径的前提下，新增一条**按业务键寻址**的通道，用于一次结算同时
影响多张关联表、以及上游对历史结果的更正/撤回。两类通道共用同一把表锁，互不影响。

### 5.1 业务键表

- 建表时通过 `key_columns` 声明业务键（如 `orders.order_id`、
  `items.(order_id, sku)` 复合键）；键值即一条业务结果的稳定身份。
- **业务键列禁止 DROP / RENAME**：身份必须跨更正、撤回、结构变化保持稳定
  （DDL 阶段直接报错）。未声明业务键的表只能走原单表位点通道，业务批次提交会得到
  `TABLE_NOT_KEYED`。
- 结算数据只持久化在业务提交日志 `business_batches.jsonl` 中，不与 `rows.jsonl`
  的位点数据混用。

### 5.2 跨表一致性语义（整体提交）

- `BusinessBatch(batch_id, ops[], schema_version)`：一个业务批次可含多张表的多个
  `BusinessOp(table, UPSERT|RETRACT, key, revision, data)`。
- 提交流程：**先按表名排序获取全部相关表锁 → 在锁内完成所有表的全部校验
  → 分配全局顺序号 sequence → 向 journal 做且只做一次单行追加（fsync）**。
  - journal 追加成功 = 所有表**同时生效**；追加不存在 / 只有半行 = 所有表都不生效。
    崩溃边界外不可能观察到“半张表有数据、半张表没有”。
  - 任何一张表的任何一条数据校验失败，整批在写入前拒绝，**不占用顺序号、不留提交痕迹**；
    错误精确定位到 `batch_id` / `table` / `key` / `reason`（哪类数据），
    HTTP 为 `409 detail.{reason,table,key,batch_id}`。修复后用同一 `batch_id` 重试即可。
- `sequence` 是全局单调提交顺序号，也就是“上次确认的边界”；重启后从 journal 完整
  重放即可重建同一最终状态。
- 多表加锁按表名全局排序获取，避免死锁；业务批次、单表增量提交、DDL 因此可线性化。

### 5.3 更正与撤回规则

- **UPSERT（写入/更正）**：按业务键给出该键的**完整结论**，附上游严格递增的
  `revision`；**RETRACT（撤回）**：撤销该键当前结论，不携带数据。
- **当前状态只看最后一次有效结论**：按 sequence 顺序折叠日志，每个业务键只保留
  revision 最大的结论。撤回后当前查询中该行消失（`current_business_rows` 不返回），
  但 `current_conclusion` 仍能取到状态为 `RETRACT` 的最后结论。
- **处理痕迹不抹除**：`business_history(table, key)` 返回该键经历过的全部
  UPSERT/RETRACT（含批次号、顺序号、revision、当时数据与结构版本），可解释更正链。
- **确定性顺序（防翻转/防复活）**：提交时 `revision` 必须**严格新于**该键已生效的
  最后结论，否则整批 `STALE_REVISION` 拒绝。于是旧值不会覆盖新值；撤回后，一个
  迟到的旧更正也不会让结果“冒回来”。撤回后允许用更高 revision 给出新结论。
- **幂等与中断重放**：
  - 同一 `batch_id` 重复提交直接回放首次结论（`replay=true`），不二次追加、不产生重复行；
  - “执行到一半中断”发生在 journal 追加之前：没有任何结论，用同一 `batch_id` 重放即首次生效；
  - 进程在 journal 行写到一半时崩溃：重启加载时把**末行坏行截断丢弃**（仅允许末行），
    该批次视为未提交，重放后收敛到与一次成功完全相同的最终状态。

### 5.4 结构变化兼容范围（业务批次）

- 业务批次中每个操作的数据都按其生产版本对齐到**提交时刻该表的当前结构**，
  加列补稳定缺省、改名按链映射、删列丢弃，规则与单表通道完全一致
  （复用同一套对齐器）。
- 由于“整批校验 + 单次提交”都在相关表锁内完成，而 DDL 取同一把锁，
  **一个业务批次不可能一半落在改名前的字段、一半落在改名后的字段**：它要么整体
  线性化在 DDL 之前，要么整体在之后；每张表生效时的结构版本记录在 journal 的
  `tables[].schema_version` 中。
- 兼容范围与限制：
  - 支持 ADD_COLUMN（必填须有 default）、RENAME_COLUMN、DROP_COLUMN；
  - 业务键列不允许改名/删除（身份稳定）；
  - 业务批次版本头 / 操作版本高于目标当前版本 -> `SCHEMA_VERSION_AHEAD`；
  - 结算表的当前视图始终按当前结构解释（历史结论保留原始数据与版本，可按轨迹追溯）。

### 5.5 新增的可区分拒绝原因

| 原因 | 含义 |
|---|---|
| `TABLE_NOT_KEYED` | 表未声明业务键列，不能按业务键更正/撤回 |
| `KEY_MISMATCH` | 业务键取值个数与键列不符，或键值类型不符（防 `1`/`"1"` 错位） |
| `STALE_REVISION` | 更正/撤回的 revision 不新于该键已生效结论（旧值翻案） |
| `DUPLICATE_KEY_IN_BATCH` | 同一业务批次内同表同键出现多次（批内顺序不可确定） |
| `INVALID_OPERATION` | 操作非法：撤回带数据、UPSERT 缺数据、未知操作类型 |
| `SCHEMA_VERSION_AHEAD` 等 | 与单表通道同名同义 |

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tables/{table}/init` | 建表（初始列定义，可带 `key_columns` 业务键） |
| GET  | `/tables/{table}` | 快照：checkpoint / schema_version / rows |
| POST | `/tables/{table}/schema` | 结构变更 `add_column`/`drop_column`/`rename_column` |
| POST | `/tables/{table}/batches` | 提交单表增量批次（位点通道，幂等） |
| GET  | `/tables/{table}/batches/{batch_id}` | 查询批次逐条处理留痕 |
| POST | `/sources/{table}` | 源端写入一条记录（演示用，位点自动分配） |
| POST | `/tables/{table}/run` | 从当前位点之后从源端拉取一轮（断点续传） |
| POST | `/business-batches` | **提交跨表业务批次（整体生效；更正/撤回）** |
| GET  | `/business-batches/{batch_id}` | 查询业务批次提交留痕（顺序号、各表结构版本） |
| GET  | `/tables/{table}/business/current?key=<JSON>` | 业务键当前最后一次有效结论 |
| GET  | `/tables/{table}/business/history?key=<JSON>` | 业务键的全部更正/撤回轨迹 |

`key` 为 JSON 数组：单键 `?key=[1]`，复合键 `?key=[1,"A"]`。

批次级拒绝返回 `409`，响应体 `detail.reason` 给出可区分原因；
记录级部分失败返回 `200`，在 `rejected[]` 中逐条给出原因。
业务批次失败时 `detail` 额外携带 `batch_id` / `table` / `key` 用于定位。

### curl 速览

```bash
# 单表增量（位点通道）
curl -s -XPOST localhost:8000/tables/orders/init -H 'content-type: application/json' \
  -d '{"columns":[{"name":"id","typ":"int"},{"name":"name","typ":"str"}]}'

curl -s -XPOST localhost:8000/tables/orders/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","schema_version":1,
       "records":[{"offset":1,"data":{"id":1,"name":"alice"}}]}'

# 结算：建带业务键的表（订单 + 明细）
curl -s -XPOST localhost:8000/tables/so/init -H 'content-type: application/json' \
  -d '{"columns":[{"name":"order_id","typ":"int"},{"name":"amount","typ":"int"}],
       "key_columns":["order_id"]}'
curl -s -XPOST localhost:8000/tables/si/init -H 'content-type: application/json' \
  -d '{"columns":[{"name":"order_id","typ":"int"},{"name":"sku","typ":"str"},
                  {"name":"qty","typ":"int"}],"key_columns":["order_id","sku"]}'

# 一个业务批次跨两张表，整体提交
curl -s -XPOST localhost:8000/business-batches -H 'content-type: application/json' \
  -d '{"batch_id":"set-1","schema_version":1,"ops":[
       {"table":"so","op_type":"UPSERT","key":[1],"revision":1,
        "data":{"order_id":1,"amount":100}},
       {"table":"si","op_type":"UPSERT","key":[1,"A"],"revision":1,
        "data":{"order_id":1,"sku":"A","qty":2}}]}'

# 历史更正（revision 递增）-> 撤回
curl -s -XPOST localhost:8000/business-batches -H 'content-type: application/json' \
  -d '{"batch_id":"corr-1","schema_version":1,"ops":[
       {"table":"so","op_type":"UPSERT","key":[1],"revision":2,
        "data":{"order_id":1,"amount":120}}]}'
curl -s -XPOST localhost:8000/business-batches -H 'content-type: application/json' \
  -d '{"batch_id":"retract-1","schema_version":1,"ops":[
       {"table":"so","op_type":"RETRACT","key":[1],"revision":3}]}'

curl -s 'localhost:8000/tables/so/business/current?key=[1]'
curl -s 'localhost:8000/tables/so/business/history?key=[1]'
```

## 7. 本地验证方法（测试覆盖）

```bash
uv sync
uv run python -m unittest discover -s tests -v   # 全部用例
uv run python -m unittest tests.test_settlement tests.test_correction_retract -v
uv run python demo.py                            # 端到端演示（./data_demo）
uv run fastapi dev main.py                       # HTTP：/docs
```

共 34 个用例：

- `test_batch_and_offset.py`（单表增量，不回归）
  - 重复提交幂等、不重复写入；
  - 部分失败的逐条留痕与可区分原因，成功前缀可追溯；
  - 修复坏记录后从断点重提，不重不丢；
  - 位点回退 `OFFSET_BACKWARD`、跳号 `OFFSET_GAP` 明确拒绝且无副作用；
  - 进程重启重建 Pipeline 后续传，结果一致。
- `test_schema_and_concurrency.py`（结构变更，不回归）
  - 新增必填列无 default 时 DDL 拒绝；
  - 加列稳定缺省、改名映射、删列丢弃均不错位；
  - `UNKNOWN_COLUMN / TYPE_MISMATCH / MISSING_REQUIRED` 可区分；
  - 超前结构版本整批拒绝；
  - 200 条数据的并发批次与加列 DDL 同时发生，最终行数、位点、顺序严格一致
    （并发用例从“当时已确认位点”续读，消除调度竞态）。
- `test_settlement.py`（跨表整体提交）
  - 多表批次整体成功、各表结果与顺序号正确；
  - 明细表中途失败：任何表都不生效，错误定位到业务批次/表/业务键/数据类别，
    且不占顺序号、不留提交痕迹；
  - 失败修复后用同一批次重试，不出现半张表数据；
  - `TABLE_NOT_KEYED / KEY_MISMATCH / INVALID_OPERATION / DUPLICATE_KEY_IN_BATCH`
    可区分且全部无副作用；
  - 单表增量通道与结算通道共存互不影响。
- `test_correction_retract.py`（更正 / 撤回 / 中断重放）
  - 当前状态只取最后一次有效结论；历史更正/撤回轨迹完整可解释；
  - 撤回后当前查询不可见、但结论与轨迹保留，更高 revision 可重新给出结论；
  - 同一更正请求重复提交幂等；
  - 旧 revision 翻案被拒（覆盖场景与撤回后“复活”场景）；
  - 提交点前崩溃重放、journal 坏尾行截断后重放，均收敛到同一最终状态；
  - 重启重建后当前状态、历史、顺序号一致，幂等回放仍成立。
- `test_business_concurrency.py`（并发确定性顺序）
  - 同键并发乱序更正：最大 revision 胜出，低版本翻案被拒，结论不翻转；
  - 更正与撤回并发：最终状态由 revision 顺序唯一决定，撤回后不冒回来；
  - 50 个跨表业务批次与 50 个单表增量并发：无跨表半生效、无重复写；
  - 业务批次与 RENAME 类型 DDL 并发：整批线性化在同一结构版本上，字段不分裂；
  - 混合并发后重启：从已确认顺序边界重放到同一最终状态。
- `test_api.py`：HTTP 维度验证单表幂等回放、409 原因区分、结构变更后拉取对齐，
  以及跨表提交/回放/失败定位、更正/撤回/当前状态/历史轨迹。

持久化文件：`checkpoints.json`（位点）、`rows.jsonl`（单表数据）、
`batches.jsonl`（单表批次留痕）、`changelog.jsonl`（结构流水）、
`business_batches.jsonl`（跨表业务批次提交日志，结算事实来源）。

测试日志由 `tests/_helpers.py` 统一配置，打印：

- `[BATCH] table=.. batch=.. version=.. checkpoint=A->B accepted=.. rejected=.. reasons=[..]`
- `[DDL] table=.. action=ADD_COLUMN/RENAME_COLUMN/... version=.. detail=old->new`
- `[BIZ-BATCH] batch=.. COMMIT sequence=.. tables=[..] ops=..` /
  `REPLAY` / `REJECT table=.. key=.. reason=..`

即题目要求的**批次、位点、变更流水与跨表业务批次提交/更正/撤回流水**。
