"""FastAPI HTTP 接入层。

路由
----
POST /tables/{table}/init               建表（定义初始结构）
GET  /tables/{table}                    查询位点 / 结构版本 / 数据快照
POST /tables/{table}/schema             结构变更（add/drop/rename_column）
POST /tables/{table}/batches            提交批次（幂等键 batch_id）
GET  /tables/{table}/batches/{batch_id} 查询批次处理留痕
POST /sources/{table}                   源端写入一条记录（演示用）
POST /tables/{table}/run                从源端拉取一轮（断点续传）

批次级拒绝（位点/结构版本错误）统一返回 409，body 含可区分 reason。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.models import (Batch, BusinessBatch, BusinessOp, ChangeType, Column,
                        OpType, Record, SchemaChange)
from app.pipeline import (BatchRejected, BusinessBatchRejected, Pipeline)
from app.schema_store import SchemaError
from app.source import LocalSource
from app.storage import JsonlStore


class ColumnIn(BaseModel):
    name: str
    typ: Literal["int", "str", "float", "bool"]
    required: bool = True
    default: Any = None


class InitIn(BaseModel):
    columns: list[ColumnIn]
    key_columns: list[str] = []


class SchemaChangeIn(BaseModel):
    change_type: Literal["add_column", "drop_column", "rename_column"]
    column_name: str
    to_name: str | None = None
    column: ColumnIn | None = None


class RecordIn(BaseModel):
    offset: int | None = None
    data: dict[str, Any]
    schema_version: int | None = None


class BatchIn(BaseModel):
    batch_id: str
    schema_version: int
    records: list[RecordIn]


class ProduceIn(BaseModel):
    schema_version: int
    data: dict[str, Any]


class RunIn(BaseModel):
    batch_id: str
    limit: int = 100


class BusinessOpIn(BaseModel):
    table: str
    op_type: Literal["UPSERT", "RETRACT"]
    key: list[Any]                    # 业务键取值，顺序对齐建表声明的 key_columns
    revision: int
    data: dict[str, Any] | None = None
    schema_version: int | None = None


class BusinessBatchIn(BaseModel):
    batch_id: str
    schema_version: int
    ops: list[BusinessOpIn]


def _column(inp: ColumnIn) -> Column:
    return Column(name=inp.name, typ=inp.typ,
                  required=inp.required, default=inp.default)


def create_app(pipeline: Pipeline | None = None,
               source: LocalSource | None = None) -> FastAPI:
    app = FastAPI(title="local-incremental-ingestion")
    pipeline = pipeline or Pipeline()
    if source is None:
        source = LocalSource(JsonlStore(Path(pipeline.root) / "source.jsonl",
                                        lock=pipeline._store_lock))

    def conflict(exc: BatchRejected) -> HTTPException:
        return HTTPException(status_code=409,
                             detail={"reason": exc.reason.value, "message": str(exc)})

    def business_conflict(exc: BusinessBatchRejected) -> HTTPException:
        return HTTPException(status_code=409, detail={
            "reason": exc.reason.value, "message": str(exc),
            "batch_id": exc.batch_id, "table": exc.table,
            "key": list(exc.key) if exc.key is not None else None})

    @app.post("/tables/{table}/init")
    def init_table(table: str, body: InitIn):
        try:
            version = pipeline.init_table(
                table, [_column(c) for c in body.columns],
                key_columns=tuple(body.key_columns))
        except SchemaError as exc:
            raise HTTPException(status_code=409, detail={"message": str(exc)})
        return {"table": table, "schema_version": version,
                "key_columns": body.key_columns}

    @app.get("/tables/{table}")
    def snapshot(table: str):
        try:
            return pipeline.snapshot(table)
        except SchemaError as exc:
            raise HTTPException(status_code=404, detail={"message": str(exc)})

    @app.post("/tables/{table}/schema")
    def alter_schema(table: str, body: SchemaChangeIn):
        change = SchemaChange(
            version=0,
            change_type=ChangeType(body.change_type.upper()),
            column_name=body.column_name,
            to_name=body.to_name,
            column=_column(body.column) if body.column else None,
        )
        try:
            result = pipeline.alter_schema(table, change)
        except SchemaError as exc:
            raise HTTPException(status_code=409, detail={"message": str(exc)})
        return {"table": table, "schema_version": result.version,
                "change_type": result.change_type.value,
                "column_name": result.column_name, "to_name": result.to_name}

    @app.post("/tables/{table}/batches")
    def submit_batch(table: str, body: BatchIn):
        records = [Record(offset=r.offset, data=r.data,
                          schema_version=r.schema_version)
                   for r in body.records]
        # offset 由调用方显式给出（模拟源端位点）；缺失则 422 前置无意义，这里直接校验
        if any(r.offset is None for r in records):
            raise HTTPException(status_code=422,
                                detail={"message": "每条记录必须带源端 offset"})
        batch = Batch(batch_id=body.batch_id, table=table,
                      records=records, schema_version=body.schema_version)
        try:
            result = pipeline.submit_batch(batch)
        except BatchRejected as exc:
            raise conflict(exc)
        return {
            "batch_id": result.batch_id, "table": result.table,
            "schema_version": result.schema_version,
            "accepted_count": result.accepted_count,
            "checkpoint": result.checkpoint, "replay": result.replay,
            "rejected": [{"offset": r.offset,
                          "reason": r.reason.value if r.reason else None,
                          "detail": r.detail} for r in result.rejected],
        }

    @app.get("/tables/{table}/batches/{batch_id}")
    def get_batch(table: str, batch_id: str):
        trace = pipeline.batch_trace(table, batch_id)
        if trace is None:
            raise HTTPException(status_code=404, detail={"message": "批次不存在"})
        return trace

    @app.post("/sources/{table}")
    def produce(table: str, body: ProduceIn):
        rec = source.produce(table, body.data, body.schema_version)
        return {"table": table, "offset": rec.offset,
                "schema_version": rec.schema_version}

    @app.post("/tables/{table}/run")
    def run_once(table: str, body: RunIn):
        try:
            result = pipeline.run_once(table, source, body.batch_id, body.limit)
        except BatchRejected as exc:
            raise conflict(exc)
        except SchemaError as exc:
            raise HTTPException(status_code=404, detail={"message": str(exc)})
        return {"batch_id": result.batch_id, "checkpoint": result.checkpoint,
                "accepted_count": result.accepted_count, "replay": result.replay}

    # ---------- 跨表业务批次（结算：更正 / 撤回） ----------
    @app.post("/business-batches")
    def submit_business_batch(body: BusinessBatchIn):
        ops = [BusinessOp(
            table=o.table, op_type=OpType(o.op_type),
            key=tuple(o.key), revision=o.revision, data=o.data,
            op_schema_version=o.schema_version)
            for o in body.ops]
        batch = BusinessBatch(batch_id=body.batch_id, ops=ops,
                              schema_version=body.schema_version)
        try:
            result = pipeline.submit_business_batch(batch)
        except BusinessBatchRejected as exc:
            raise business_conflict(exc)
        return {
            "batch_id": result.batch_id, "committed": result.committed,
            "sequence": result.sequence, "replay": result.replay,
            "tables": [{"table": t.table, "ops": t.ops,
                        "schema_version": t.schema_version}
                       for t in result.tables],
        }

    @app.get("/business-batches/{batch_id}")
    def get_business_batch(batch_id: str):
        trace = pipeline.business_trace(batch_id)
        if trace is None:
            raise HTTPException(status_code=404, detail={"message": "业务批次不存在"})
        return trace

    @app.get("/tables/{table}/business/current")
    def current_state(table: str, key: str):
        """查询业务键当前最后一次有效结论。key 为 JSON 数组，如 ?key=[1] 或 ?key=[1,"A"]。"""
        import json as _json
        try:
            key_tuple = tuple(_json.loads(key))
        except (ValueError, TypeError):
            raise HTTPException(status_code=422,
                                detail={"message": "key 必须是 JSON 数组"})
        conclusion = pipeline.current_conclusion(table, key_tuple)
        if conclusion is None:
            raise HTTPException(status_code=404,
                                detail={"message": "该业务键尚无结论"})
        return {
            "table": table, "key": list(conclusion.key),
            "revision": conclusion.revision,
            "status": conclusion.status.value,
            "data": conclusion.data,
            "schema_version": conclusion.schema_version,
            "sequence": conclusion.sequence, "batch_id": conclusion.batch_id,
        }

    @app.get("/tables/{table}/business/history")
    def business_history(table: str, key: str):
        """查询业务键经历过的全部更正/撤回轨迹。"""
        import json as _json
        try:
            key_tuple = tuple(_json.loads(key))
        except (ValueError, TypeError):
            raise HTTPException(status_code=422,
                                detail={"message": "key 必须是 JSON 数组"})
        history = pipeline.business_history(table, key_tuple)
        if not history:
            raise HTTPException(status_code=404,
                                detail={"message": "该业务键尚无处理轨迹"})
        return {"table": table, "key": list(key_tuple), "history": history}

    return app
