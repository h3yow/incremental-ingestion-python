"""FastAPI 接入层。

端点（接入协议的 HTTP 形态）：
  POST /sources/{table}                 模拟源端写入一条记录
  POST /tables/{table}/schema           建表
  GET  /tables/{table}/schema           查看当前结构与变更流水
  POST /tables/{table}/evolve           结构变更（ADD/DROP/RENAME_COLUMN）
  POST /batches                         提交批次（幂等）
  GET  /batches/{batch_id}              查询批次处理结论
  GET  /tables/{table}/checkpoint       查询位点
  POST /ingest                          从源端拉取并接入（断点续跑）
  GET  /tables/{table}/rows             查看目标端数据（验证用）
"""
from __future__ import annotations

import os
from dataclasses import asdict

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ingestion.engine import IngestionEngine
from ingestion.errors import IngestionError
from ingestion.ledger import BatchLedger
from ingestion.models import Batch, Column
from ingestion.offsets import CheckpointManager
from ingestion.schema import SchemaRegistry
from ingestion.storage import LocalSource, LocalTarget


class ColumnIn(BaseModel):
    name: str
    type: str
    nullable: bool = True
    default: object | None = None


class CreateTableIn(BaseModel):
    key_column: str
    columns: list[ColumnIn]


class EvolveIn(BaseModel):
    kind: str = Field(pattern="^(ADD_COLUMN|DROP_COLUMN|RENAME_COLUMN)$")
    name: str | None = None
    type: str | None = None
    nullable: bool = True
    default: object | None = None
    old_name: str | None = None
    new_name: str | None = None


class BatchIn(BaseModel):
    batch_id: str
    table: str
    start_offset: int
    schema_version: int
    records: list[dict[str, object]]


class SourceRecordIn(BaseModel):
    data: dict[str, object]
    schema_version: int | None = None


def build_engine(data_dir: str) -> IngestionEngine:
    registry = SchemaRegistry(os.path.join(data_dir, "schema"))
    source = LocalSource(os.path.join(data_dir, "source"))
    target = LocalTarget(os.path.join(data_dir, "target"))
    checkpoints = CheckpointManager(os.path.join(data_dir, "checkpoint.json"))
    ledger = BatchLedger(os.path.join(data_dir, "ledger"))
    return IngestionEngine(
        registry, source, target, checkpoints, ledger,
        lock_dir=os.path.join(data_dir, "locks"),
    )


def serialize_result(result) -> dict:
    data = asdict(result)
    data["accepted_offsets"] = list(result.accepted_offsets)
    data["rejected"] = [asdict(r) for r in result.rejected]
    data["accepted_count"] = result.accepted_count
    data["rejected_count"] = result.rejected_count
    return data


def create_app(data_dir: str | None = None) -> FastAPI:
    data_dir = data_dir or os.environ.get("INGESTION_DATA_DIR", "./data")
    engine = build_engine(data_dir)
    app = FastAPI(title="本地增量数据接入", version="0.1.0")

    @app.exception_handler(IngestionError)
    async def ingestion_error_handler(request, exc: IngestionError):
        # 所有流程级错误都带稳定 code，调用方可以据此区分处理
        return JSONResponse(
            status_code=409,
            content={"error": exc.code, "message": str(exc)},
        )

    @app.post("/sources/{table}")
    def append_source(table: str, body: SourceRecordIn):
        sv = body.schema_version or engine.registry.current(table).version
        offset = engine.source.append(table, body.data, sv)
        return {"table": table, "offset": offset}

    @app.post("/tables/{table}/schema")
    def create_table(table: str, body: CreateTableIn):
        cols = [
            Column(c.name, c.type, c.nullable,
                   c.default is not None, c.default)
            for c in body.columns
        ]
        schema = engine.registry.create_table(table, body.key_column, cols)
        return asdict(schema)

    @app.get("/tables/{table}/schema")
    def get_schema(table: str):
        schema = engine.registry.current(table)
        return {
            "current": asdict(schema),
            "history": [asdict(e) for e in engine.registry.history(table)],
        }

    @app.post("/tables/{table}/evolve")
    def evolve(table: str, body: EvolveIn):
        payload = body.model_dump(exclude={"kind"}, exclude_none=True)
        schema = engine.evolve_schema(table, body.kind, **payload)
        return asdict(schema)

    @app.post("/batches")
    def submit_batch(body: BatchIn):
        batch = Batch(
            batch_id=body.batch_id,
            table=body.table,
            start_offset=body.start_offset,
            schema_version=body.schema_version,
            records=tuple(body.records),
        )
        result = engine.submit_batch(batch)
        return serialize_result(result)

    @app.get("/batches/{batch_id}")
    def get_batch(batch_id: str):
        result = engine.ledger.get(batch_id)
        if result is None:
            return JSONResponse(
                status_code=404,
                content={"error": "BATCH_NOT_FOUND", "message": batch_id},
            )
        return serialize_result(result)

    @app.get("/tables/{table}/checkpoint")
    def get_checkpoint(table: str):
        return {"table": table, "next_offset": engine.checkpoints.get(table)}

    @app.post("/ingest")
    def ingest(table: str, max_records: int = 100):
        result = engine.pull_and_ingest(table, max_records)
        if result is None:
            return {"status": "IDLE", "next_offset": engine.checkpoints.get(table)}
        return serialize_result(result)

    @app.get("/tables/{table}/rows")
    def list_rows(table: str):
        return engine.target.all_rows(table)

    return app


app = create_app()
