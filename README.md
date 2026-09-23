# 本地增量数据接入（Python）

用**本地文件存储**模拟源端与目标端的一套增量数据接入流程，仅依赖仓库已有的
`fastapi[standard]`（测试只用标准库 `unittest` + FastAPI TestClient）。

目标：让**批次提交、重复消费、结构变更**三类场景下的数据状态与失败原因都
确定、可预期、可追溯。

## 运行

```bash
uv sync                                   # 安装依赖
.venv/bin/python run_demo.py              # 端到端命令行演示（自带日志）
.venv/bin/python -m unittest discover -s tests -v   # 全量单测（21 个）
uv run uvicorn main:app                   # HTTP 服务（默认数据目录 ./data）
INGESTION_DATA_DIR=/tmp/ing uv run uvicorn main:app
```

## 目录与存储布局

```
ingestion/
  errors.py      稳定错误码 + 记录级拒绝原因（枚举）
  models.py      Column / Schema / Batch / BatchResult / RejectedRecord
  schema.py      版本化结构注册表 + 结构变更流水（每表一个 JSON）
  storage.py     LocalSource（JSONL 源日志）/ LocalTarget（JSON 目标表）
  offsets.py     CheckpointManager（位点，原子写 JSON）
  ledger.py      BatchLedger（每批次一个不可变结果文件）+ 内容指纹
  locking.py     表级锁：线程锁 + fcntl 文件锁
  engine.py      接入引擎：提交流程、逐条校验/映射、断点拉取、结构变更
  api.py         FastAPI：接入协议的 HTTP 形态
tests/           幂等 / 位点断点 / 结构变更并发 / HTTP 端到端 四组单测
run_demo.py      本地完整流程演示
```

数据目录（默认 `./data`，演示用 `./demo_data`）：

| 路径 | 含义 |
| --- | --- |
| `source/<table>.jsonl` | 源端追加日志，每行 `{offset, schema_version, data}` |
| `target/<table>.json` | 目标端表，`{主键: {列: 值}}`，按主键 upsert |
| `checkpoint.json` | 每张表的位点 `next_offset` |
| `ledger/<batch_id>.json` | 每个批次不可变的处理结论（含内容指纹、拒绝明细） |
| `schema/<table>.schema.json` | 全部历史结构版本 + 变更流水 |
| `locks/<table>.lock` | 表级跨进程锁文件 |

所有 JSON/JSONL 写入都采用 **temp 文件 + `os.replace` 原子替换**。

## 一、接入协议

### 批次（Batch）

```jsonc
{
  "batch_id": "b1",          // 调用方指定的全局唯一批次号，幂等键
  "table": "orders",
  "start_offset": 0,         // 批次首条记录的位点（左闭右开）
  "schema_version": 1,       // 批次数据写入时对应的结构版本
  "records": [ {"order_id": 1, "customer": "alice", "amount": 10.5} ]
}
```

- offset 语义为**源端日志位点**：`[start_offset, start_offset + n)`，从 0 起连续。
- HTTP：`POST /batches` 提交；`GET /batches/{batch_id}` 查询首次处理结论。
- 拉模式：`POST /ingest?table=...`，引擎从位点处读 `LocalSource` 自动组装批次。

### 提交保证（持表锁，串行执行）

1. **台账查重**：`batch_id` 已处理过
   - 内容指纹（表/位点/结构版本/全部记录的 SHA-256）相同 →
     直接返回首次结论（响应 `duplicate=true`），**不重复写目标端**；
   - 指纹不同 → `409 BATCH_CONFLICT`，明确拒绝“同号不同内容”。
2. **位点校验**：`start_offset` 必须恰好等于当前位点（见下节）。
3. **结构校验**：批次结构版本不得高于目标端当前版本，否则
   `409 SCHEMA_VERSION_AHEAD`。
4. **逐条处理**：记录按**目标端当前结构**做按列名校验与投影：
   - 合法记录按主键 upsert（重放幂等）；
   - 非法记录**不落地但进台账**，绝不静默丢弃。
5. **先落台账，再推进位点**；崩溃恢复时重放同一批次，
   upsert 幂等 + 台账不可变 ⇒ 不重复、不丢失。

### 记录级拒绝原因（可区分、可断言）

| reason | 触发条件 |
| --- | --- |
| `MISSING_KEY` | 缺少主键列 |
| `TYPE_MISMATCH` | 某列（含主键）值类型与结构声明不符（`bool` 不会被当成 `int`） |
| `UNKNOWN_COLUMN` | 携带了当前结构中不存在的列（删列/改名后的旧数据） |
| `MISSING_COLUMN` | `NOT NULL` 且无缺省值的列缺失 |
| `NULL_NOT_ALLOWED` | `NOT NULL` 列显式给出 `null` |

部分失败的批次：合法部分照常提交，位点仍推进到批次末尾；
被拒绝记录保留在 `ledger/<batch_id>.json` 中（含 offset、主键、原因、明细），
属于可追溯的“死信”，修复数据后必须用**新的批次号**重投，不能原批重放
（原批重放只返回历史结论）。

## 二、位点语义（断点管理）

- 位点只保存一个数 `next_offset`：**下一条待处理记录的 offset**；初始为 0。
- 合法衔接：批次 `start_offset == next_offset`。
  - `start_offset < next_offset` → `OFFSET_BACKWARD`（回退/重叠，拒绝）；
  - `start_offset > next_offset` → `OFFSET_GAP`（越界推进、中间有空洞，拒绝）。
- 位点只能在批次台账落盘成功后，由引擎内部推进到 `end_offset`，
  不接受外部直接改写。
- 中断恢复：销毁进程/重建引擎后，`POST /ingest` 自动从 `next_offset`
  继续；目标端 upsert 幂等 + 批次台账去重，保证 **at-least-once 执行、
  exactly-once 落地效果**：不重复也不丢失。

## 三、结构变更兼容规则

结构按整数版本号管理，每次变更产生一条不可变流水
（`seq / kind / from_version / to_version / payload / 时间戳`），历史版本可查。

- **新增列**
  - nullable 列：允许；历史/老版本记录补入时缺省稳定为 `null`；
  - `NOT NULL` 列：**必须**在变更时给显式 `default`，否则变更本身被拒
    （`INCOMPATIBLE_SCHEMA_CHANGE`）；老数据按该 default 补齐。
- **删列**：允许（主键列除外）。因为按**列名**而非位置映射，
  携带已删列的旧记录整被判 `UNKNOWN_COLUMN`，不会发生字段错位。
- **改名**：等价于一次“加新列 + 删旧列”的版本变更，流水同时记录 old/new；
  旧列名数据 → `UNKNOWN_COLUMN`，新列名正常映射。
- **并发**：批次提交与结构变更共用同一把**表级锁**
  （进程内线程锁 + 跨进程 `fcntl`），二者严格串行；
  批次结论中记录 `applied_schema_version`，并发场景的结论一致且可解释
  （同批并发提交恰有一次首次提交，其余全部 `duplicate=true`）。

## 四、HTTP 协议一览

| 方法/路径 | 说明 |
| --- | --- |
| `POST /tables/{table}/schema` | 建表（主键 + 列定义） |
| `GET  /tables/{table}/schema` | 当前结构 + 变更流水 |
| `POST /tables/{table}/evolve` | 结构变更 `ADD_COLUMN/DROP_COLUMN/RENAME_COLUMN` |
| `POST /sources/{table}` | 向模拟源端追加一条记录 |
| `POST /batches` | 提交批次（幂等） |
| `GET  /batches/{batch_id}` | 查询批次结论（含拒绝明细） |
| `GET  /tables/{table}/checkpoint` | 查询位点 |
| `POST /ingest?table=&max_records=` | 拉模式：从位点继续接入 |
| `GET  /tables/{table}/rows` | 查看目标端数据 |

流程级错误统一返回 `409 {"error": <稳定错误码>, "message": ...}`：
`OFFSET_BACKWARD / OFFSET_GAP / BATCH_CONFLICT / SCHEMA_VERSION_AHEAD /
INCOMPATIBLE_SCHEMA_CHANGE / SCHEMA_NOT_FOUND / TABLE_LOCKED`。

## 五、验证方法

```bash
.venv/bin/python -m unittest discover -s tests -v
```

| 测试文件 | 覆盖点 |
| --- | --- |
| `tests/test_idempotency.py` | 重复提交不重复写、同号不同内容冲突、部分失败四种拒绝原因、台账可追溯 |
| `tests/test_offsets.py` | 回退/越界/重叠拒绝、拒绝后位点不动、中断重建后不重不丢 |
| `tests/test_schema_evolution.py` | 新增列缺省语义、NOT NULL 必须带 default、删列/改名不错位、超前版本拒绝、变更流水、并发提交与变更串行一致性 |
| `tests/test_api.py` | HTTP 协议端到端：建表/提交/重复/拒绝码/结构变更/源端拉取 |

单测与演示均开启 INFO 日志，日志中可直接看到：

- `[BATCH-BEGIN] / [BATCH-COMMIT] / [BATCH-DUP]`：批次号与位点区间；
- `[RECORD-REJECT]`：每条拒绝记录的 offset、key、reason、detail；
- `[PULL]`：断点续跑时从哪个位点拉取、组装的批次号；
- `[SCHEMA-CHANGE]`：结构变更流水（v?→v?、kind、payload）。

## 六、快速手工验证（curl）

```bash
uv run uvicorn main:app &
curl -s -XPOST localhost:8000/tables/t/schema -H 'content-type: application/json' \
  -d '{"key_column":"id","columns":[{"name":"id","type":"int","nullable":false},{"name":"name","type":"str","nullable":false}]}'
curl -s -XPOST localhost:8000/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","table":"t","start_offset":0,"schema_version":1,"records":[{"id":1,"name":"a"}]}'
curl -s -XPOST localhost:8000/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","table":"t","start_offset":0,"schema_version":1,"records":[{"id":1,"name":"a"}]}'  # duplicate=true
curl -s localhost:8000/tables/t/checkpoint
```
