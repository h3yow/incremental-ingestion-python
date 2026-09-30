# 本地增量数据接入流程（Local Incremental Ingestion）

用**本地文件存储**模拟源端与目标端，实现一条可预期的增量接入链路：批次幂等提交、
断点续传（不重复、不丢失）、非法位点明确拒绝、表结构变更可兼容；并在同一套能力上
扩展了**跨表业务批次的整体提交（两阶段提交）**与**历史更正/撤回**（按业务键覆盖、
当前状态只反映最后一次有效结论、完整更正历史可解释），二者与结构变更在并发下结论
确定。仅依赖仓库已有的 `fastapi[standard]`，测试使用 Python 标准库 `unittest`，
无额外依赖。

## 1. 目录与运行

```
app/
  models.py       领域模型：Batch / Record / RecordResult / RejectReason / SchemaChange
  storage.py      JSONL 追加存储 + 位点 KV 存储（进程内锁，模拟持久化介质）
  checkpoint.py   位点存储与推进规则（回退 / 基线过期拒绝）
  schema_store.py 表结构与不可变变更流水（INIT/ADD/DROP/RENAME），支持历史版本回放
  target.py       目标端：按记录生产版本对齐到当前结构并落地
  state_store.py  当前状态折叠与更正/撤回事件（表级单调写序号，历史不抹除）
  tx_log.py       业务事务日志（PREPARED/COMMITTED/ABORTED 两阶段留痕与恢复）
  source.py       源端模拟：单调连续位点的可回放 JSONL 日志
  pipeline.py     编排核心：表锁/DDL 协调锁、幂等、连续前缀提交、两阶段提交、恢复
  api.py          FastAPI HTTP 接入层
demo.py           端到端演示脚本（单表接入 + 结算链路跨表批次/更正/撤回）
tests/            unittest 测试（运行时打印批次、位点、结构变更日志）
data/             默认持久化目录（运行后生成）
```

持久化文件：

| 文件 | 内容 |
|---|---|
| `changelog.jsonl` | 不可变结构变更流水（INIT 含业务键列） |
| `rows.jsonl` | 增量行 + 更正/撤回事件（均带表名；更正事件带 `kind/key/seq/correction_id`） |
| `batches.jsonl` | 单表批次逐条处理留痕 |
| `transactions.jsonl` | 跨表业务/更正事务的 PREPARED/COMMITTED/ABORTED 留痕 |
| `checkpoints.json` | 每表连续位点 |
| `seq.json` | 每表单调写序号（增量行与更正事件的全序） |

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
| `KEY_NOT_FOUND` | 更正条目 | 更正/撤回的业务键当前无有效结果（从未接入或已撤回） |
| `ENTRY_INVALID` | 更正/分片 | 撤回携带数据、更正缺数据、业务键与数据不一致等 |
| `TABLE_NOT_INITIALIZED` | 业务事务 | 业务批次涉及尚未建表的表 |

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
- **跨表事务的结构快照一致性**：跨多张表的业务批次/更正批次额外持有一把
  DDL 协调锁（锁序恒为 协调锁 → 表锁，与单表 DDL 相同），因此结构变更不会
  插入到跨表事务的中间——整批的所有表分片按**同一个结构快照**校验与对齐，
  不会出现「前几张表按旧字段、后几张表按新字段」。
- **兼容范围**：加列（含必填列带稳定缺省）、改名、删列对历史版本数据仍然可对齐；
  更正数据同样走版本对齐（按更正声明的 `schema_version` 校验、对齐到当前结构）。
  不支持列类型变更与删除业务键列（业务键是更正/撤回的定位锚点）。

## 4A. 跨表业务批次的整体提交（结算链路）

一次结算可能同时写订单表和明细表。系统在原有单表能力之上增加业务事务，
**不改变**单表增量接入协议：

- `BusinessBatch(business_batch_id, entries[])`，每个 `entry` 是某张表的
  `(table, records, schema_version)` 增量分片，沿用同一套位点连续 + 结构对齐规则。
- **两阶段提交**：
  1. **预检（不写任何效果）**：对每个分片做建表 / 结构版本 / 位点连续 / 逐条校验；
     任一分片失败则整批中止，**所有表都不生效**，位点不动、不出现半张表。
  2. **PREPARED**：全部通过后，把「已对齐的行 + 各表位点推进区间」作为一份
     自包含文档写入 `transactions.jsonl`；文档不依赖任何运行期状态。
  3. **COMMIT**：在持锁状态下逐表幂等应用（行按 offset 去重、位点 `ensure`
     只升不降），随后追加 `COMMITTED`。
- **失败可定位**：中止时返回 409，`failures[]` 精确给出
  **业务批次 ID → 表 → offset/key → 可区分原因 → 详情**；并写 `ABORTED` 留痕。
- **锁序与死锁**：多表事务按表名排序获取表锁；跨多表时再加 DDL 协调锁。
  单表事务不取协调锁，因此协调锁与表锁之间不存在加锁环。
- **崩溃恢复**：进程重启（`Pipeline()` 构造时）自动把仍为 `PREPARED` 的事务
  按落盘顺序幂等前滚到 `COMMITTED`，最终状态与「从未中断」一致。
  已存在的 `COMMITTED/ABORTED` 结论不会被二次应用。

## 4B. 历史更正与撤回

- 每张表在建表时确定**业务键列** `key_col`（缺省为第一列）；更正/撤回按业务键
  定位历史结果。增量数据的业务键列必须非空。
- `CorrectionBatch(correction_id, items[])`，每个 item 为
  `(table, key, op, schema_version, data?)`：
  - `UPSERT`：key 当前必须存在；`data` 是**更正后的完整数据**（整体覆盖，
    不做字段级合并），其中业务键列必须等于 key；数据按 `schema_version` 对齐到
    当前结构（加列补稳定缺省、改名映射、删列丢弃，规则同增量）。
  - `WITHDRAW`：key 当前必须存在且不得携带数据；撤回后该键从当前状态消失。
- **当前状态 vs 历史留痕**：每次更正/撤回都是 `rows.jsonl` 中一条不可变事件，
  带表级单调写序号 `seq` 与 `correction_id`。
  - `current_state(table)`：按确定全序折叠（无序号历史行按 offset、其后按 seq），
    只返回每个键的**最后一次有效结论**，已撤回键不出现；
  - `key_history(table, key)`：返回该键经历过的全部「增量写入 + 各次更正/撤回」，
    可完整解释一条结果的来龙去脉，任何处理痕迹都不被抹掉。
- **撤回后的语义**：撤回表示该业务结果在当前状态中不再有效；撤回后不能再对该键
  直接更正（`KEY_NOT_FOUND`）——上游需作为新业务结果重新接入，杜绝「撤回后又
  冒出来」。
- **幂等与中断重放**：
  - 相同 `correction_id` 重复提交 → 直接回放首次结论（成功回放结果、失败回放 409），
    不再写事件、不再取号，结论不翻转；
  - 事件按 `(correction_id, table, key)` 去重，PREPARED 后崩溃、重启前滚或
    调用方重放，最终状态完全一致。

## 4C. 并发下的确定顺序

更正、正常增量提交、结构变更可能同时发生，结论必须可线性化：

- **同表操作**由表锁串行：对同一键的并发更正按取号顺序依次生效，当前值恒等于
  最后获胜者；历史 `seq` 严格单调、无空洞，旧值不会与新值来回翻转。
- **跨表操作**整体排在 DDL 的前或后（见 4A 的协调锁），不存在「跨表只生效一半」
  或「同一批更正落到不同字段集合」。
- **重启恢复**从上次确认的边界继续：位点只升不降、PREPARED 事务幂等前滚，
  恢复后继续处理新提交仍收敛到同一最终状态。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/business-batches` | 跨表业务批次整体提交（两阶段，幂等键 `business_batch_id`） |
| POST | `/corrections` | 历史更正/撤回批次（可跨表，幂等键 `correction_id`） |
| GET  | `/transactions/{id}` | 业务事务留痕（PREPARED/COMMITTED/ABORTED、逐表结论） |
| GET  | `/tables/{table}/state` | 当前状态：业务键 → 最后一次有效数据（不含已撤回） |
| GET  | `/tables/{table}/state/history?key=..` | 某业务键的全部增量写入与更正/撤回历史 |
| POST | `/tables/{table}/init` | 建表（初始列定义，可选 `key_col` 指定业务键列） |
| GET  | `/tables/{table}` | 快照：checkpoint / schema_version / rows |
| POST | `/tables/{table}/schema` | 结构变更 `add_column`/`drop_column`/`rename_column` |
| POST | `/tables/{table}/batches` | 提交单表批次（幂等） |
| GET  | `/tables/{table}/batches/{batch_id}` | 查询批次逐条处理留痕 |
| POST | `/sources/{table}` | 源端写入一条记录（演示用，位点自动分配） |
| POST | `/tables/{table}/run` | 从当前位点之后从源端拉取一轮（断点续传） |

业务事务 / 更正预检失败返回 `409`，响应体 `detail.failures[]` 逐条给出
`table / reason / detail / offset / key`；成功返回 `200`，含逐表 `entries[]`。
批次级拒绝返回 `409`，响应体 `detail.reason` 给出可区分原因；
记录级部分失败返回 `200`，在 `rejected[]` 中逐条给出原因。

### curl 速览

```bash
curl -s -XPOST localhost:8000/tables/orders/init -H 'content-type: application/json' \
  -d '{"columns":[{"name":"id","typ":"int"},{"name":"name","typ":"str"}]}'

curl -s -XPOST localhost:8000/tables/orders/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","schema_version":1,
       "records":[{"offset":1,"data":{"id":1,"name":"alice"}}]}'

curl -s localhost:8000/tables/orders
curl -s localhost:8000/tables/orders/batches/b1

# 跨表业务批次（订单 + 明细整体生效）
curl -s -XPOST localhost:8000/business-batches -H 'content-type: application/json' \
  -d '{"business_batch_id":"S001","entries":[
       {"table":"orders","schema_version":1,
        "records":[{"offset":1,"data":{"settle_id":"S001","amount":100}}]},
       {"table":"details","schema_version":1,
        "records":[{"offset":1,"data":{"line_id":"L1","settle_id":"S001","qty":2}}]}]}'

# 对历史结果 S001 发起更正（当前状态覆盖，历史留痕保留）
curl -s -XPOST localhost:8000/corrections -H 'content-type: application/json' \
  -d '{"correction_id":"FIX-S001-1","items":[
       {"table":"orders","key":"S001","op":"UPSERT","schema_version":1,
        "data":{"settle_id":"S001","amount":120}}]}'

curl -s localhost:8000/tables/orders/state
curl -s 'localhost:8000/tables/orders/state/history?key=S001'
curl -s localhost:8000/transactions/S001
```

## 6. 验证方法（测试覆盖）

`uv run python -m unittest discover -s tests -v`，共 30 个用例：

- `test_batch_and_offset.py`（现有，不回归）
  - 重复提交幂等、不重复写入；
  - 部分失败的逐条留痕与可区分原因，成功前缀可追溯；
  - 修复坏记录后从断点重提，不重不丢；
  - 位点回退 `OFFSET_BACKWARD`、跳号 `OFFSET_GAP` 明确拒绝且无副作用；
  - 进程重启重建 Pipeline 后续传，结果一致。
- `test_schema_and_concurrency.py`（现有，不回归）
  - 新增必填列无 default 时 DDL 拒绝；
  - 加列稳定缺省、改名映射、删列丢弃均不错位；
  - `UNKNOWN_COLUMN / TYPE_MISMATCH / MISSING_REQUIRED` 可区分；
  - 超前结构版本整批拒绝；
  - 200 条数据的并发批次与加列 DDL 同时发生，最终行数、位点、顺序严格一致。
- `test_business_and_correction.py`（新增，14 个）
  - 跨表业务批次整体成功；中途某表失败时**所有表都不生效**，409 精确指出
    业务批次/表/offset/原因，`ABORTED` 留痕可查；
  - 业务批次成功与失败结论的重复提交均回放首次结果、不重复写入；
  - `PREPARED` 后应用到一半崩溃，重启按文档幂等前滚，再次重启不重复；
  - 更正覆盖当前状态但历史完整保留；更正重复提交不二次取号、结论不翻转；
  - 更正 `PREPARED` 中断后重启前滚，再显式重放结论一致；
  - 撤回后当前状态消失、历史仍可解释，撤回后不能再更正（`KEY_NOT_FOUND`）；
  - 跨表更正中一条失败则整批不生效；撤回携带数据等非法请求 `ENTRY_INVALID`；
  - 20 路并发更正同一键：写序号全序、当前值等于最后获胜者；
  - 更正 / 单表增量 / 加列 DDL 并发：可线性化，状态与结构版本自洽；
  - **跨表更正批次与两表加列 DDL 并发：整批更正落到同一结构快照**
    （两表新增字段有无一致），不会一半旧字段一半新字段；
  - 单表增量与更正/跨表批次写同一业务键时共享一条写序号全序，折叠顺序
    等于真实提交顺序，不会旧值回翻。
- `test_api.py`（5 个）：HTTP 维度验证单表幂等回放、409 原因区分、
  结构变更后的拉取对齐，以及跨表失败 409 定位、更正/撤回/当前状态/历史接口。

端到端演示：`uv run python demo.py`，先演示单表接入，再在
`data_demo/settlement/` 下演示结算链路的跨表整体提交、失败定位、历史更正与撤回。

本地验证建议：

```bash
uv run python -m unittest discover -s tests -v          # 全量单测
uv run python demo.py                                   # 端到端演示
uv run fastapi dev main.py                              # HTTP 服务，/docs 可交互
```

测试日志由 `tests/_helpers.py` 统一配置，打印：

- `[BATCH] table=.. batch=.. version=.. checkpoint=A->B accepted=.. rejected=.. reasons=[..]`
- `[DDL] table=.. action=ADD_COLUMN/RENAME_COLUMN/... version=.. detail=old->new`

即题目要求的**批次、位点与变更流水**。
