# 本地增量数据接入流程（Local Incremental Ingestion）

用**本地文件存储**模拟源端与目标端，实现一条可预期的增量接入链路：批次幂等提交、
断点续传（不重复、不丢失）、非法位点明确拒绝、表结构变更可兼容。仅依赖仓库已有的
`fastapi[standard]`，测试使用 Python 标准库 `unittest`，无额外依赖。

## 1. 目录与运行

```
app/
  models.py       领域模型：Batch / Record / RecordResult / RejectReason / SchemaChange
  storage.py      JSONL 追加存储 + 位点 KV 存储（进程内锁，模拟持久化介质）
  checkpoint.py   位点存储与推进规则（回退 / 基线过期拒绝）
  schema_store.py 表结构与不可变变更流水（INIT/ADD/DROP/RENAME），支持历史版本回放
  target.py       目标端：按记录生产版本对齐到当前结构并落地
  source.py       源端模拟：单调连续位点的可回放 JSONL 日志
  pipeline.py     编排核心：表锁、幂等、连续前缀提交、留痕、位点推进
  api.py          FastAPI HTTP 接入层
demo.py           端到端演示脚本
tests/            unittest 测试（运行时打印批次、位点、结构变更日志）
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

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tables/{table}/init` | 建表（初始列定义） |
| GET  | `/tables/{table}` | 快照：checkpoint / schema_version / rows |
| POST | `/tables/{table}/schema` | 结构变更 `add_column`/`drop_column`/`rename_column` |
| POST | `/tables/{table}/batches` | 提交批次（幂等） |
| GET  | `/tables/{table}/batches/{batch_id}` | 查询批次逐条处理留痕 |
| POST | `/sources/{table}` | 源端写入一条记录（演示用，位点自动分配） |
| POST | `/tables/{table}/run` | 从当前位点之后从源端拉取一轮（断点续传） |

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
```

## 6. 验证方法（测试覆盖）

`uv run python -m unittest discover -s tests -v`，共 14 个用例：

- `test_batch_and_offset.py`
  - 重复提交幂等、不重复写入；
  - 部分失败的逐条留痕与可区分原因，成功前缀可追溯；
  - 修复坏记录后从断点重提，不重不丢；
  - 位点回退 `OFFSET_BACKWARD`、跳号 `OFFSET_GAP` 明确拒绝且无副作用；
  - 进程重启重建 Pipeline 后续传，结果一致。
- `test_schema_and_concurrency.py`
  - 新增必填列无 default 时 DDL 拒绝；
  - 加列稳定缺省、改名映射、删列丢弃均不错位；
  - `UNKNOWN_COLUMN / TYPE_MISMATCH / MISSING_REQUIRED` 可区分；
  - 超前结构版本整批拒绝；
  - 200 条数据的并发批次与加列 DDL 同时发生，最终行数、位点、顺序严格一致。
- `test_api.py`：HTTP 维度验证幂等回放、409 原因区分、结构变更后的拉取对齐。

测试日志由 `tests/_helpers.py` 统一配置，打印：

- `[BATCH] table=.. batch=.. version=.. checkpoint=A->B accepted=.. rejected=.. reasons=[..]`
- `[DDL] table=.. action=ADD_COLUMN/RENAME_COLUMN/... version=.. detail=old->new`

即题目要求的**批次、位点与变更流水**。
