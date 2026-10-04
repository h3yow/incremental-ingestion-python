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

from app.models import (Batch, BusinessBatch, ChangeType, Column,
                        CorrectionBatch, CorrectionItem, EventKind, Record,
                        SchemaChange, TableBatchEntry)
from app.pipeline import (BatchRejected, BusinessBatchRejected, Pipeline)
from app.schema_store import KeyColumnProtectedError, SchemaError
from app.source import LocalSource
from app.storage import JsonlStore


class ColumnIn(BaseModel):
    name: str
    typ: Literal["int", "str", "float", "bool"]
    required: bool = True
    default: Any = None


class InitIn(BaseModel):
    columns: list[ColumnIn]
    key_col: str | None = None


class SchemaChangeIn(BaseModel):
    change_type: Literal["add_column", "drop_column", "rename_column"]
    column_name: str
    to_name: str | None = None
    column: ColumnIn | None = None
    # 仅在 /schema-campaigns（多表 DDL 战役）中使用：单表接口以路径参数为准
    table: str | None = None


class SchemaCampaignIn(BaseModel):
    """多表结构变更战役：整体校验、整体生效（相对跨表事务不可被插入）。"""

    campaign_id: str
    changes: list[SchemaChangeIn]


def _change_from_in(body: SchemaChangeIn) -> SchemaChange:
    return SchemaChange(
        version=0,
        change_type=ChangeType(body.change_type.upper()),
        column_name=body.column_name,
        to_name=body.to_name,
        column=_column(body.column) if body.column else None,
    )


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


class TableEntryIn(BaseModel):
    table: str
    schema_version: int
    records: list[RecordIn]
    op: Literal["APPEND"] = "APPEND"


class BusinessBatchIn(BaseModel):
    business_batch_id: str
    entries: list[TableEntryIn]


class CorrectionItemIn(BaseModel):
    table: str
    key: Any
    op: Literal["UPSERT", "WITHDRAW"]
    schema_version: int
    data: dict[str, Any] | None = None
    # 上游对同一业务键的更正/撤回发起序号（按发起先后单调递增）；
    # 缺省/null 表示不提供，系统退化为按到达先后生效，不做新旧裁决。
    issued_seq: int | None = None


class CorrectionIn(BaseModel):
    correction_id: str
    items: list[CorrectionItemIn]


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
            "txn_id": exc.txn_id,
            "message": str(exc),
            "failures": [{"table": f.table, "reason": f.reason.value,
                          "detail": f.detail, "offset": f.offset, "key": f.key}
                         for f in exc.failures],
        })

    @app.post("/tables/{table}/init")
    def init_table(table: str, body: InitIn):
        try:
            version = pipeline.init_table(
                table, [_column(c) for c in body.columns], key_col=body.key_col)
        except SchemaError as exc:
            raise HTTPException(status_code=409, detail={"message": str(exc)})
        return {"table": table, "schema_version": version,
                "key_col": pipeline.schemas.key_col(table)}

    @app.get("/tables/{table}")
    def snapshot(table: str):
        try:
            return pipeline.snapshot(table)
        except SchemaError as exc:
            raise HTTPException(status_code=404, detail={"message": str(exc)})

    @app.post("/tables/{table}/schema")
    def alter_schema(table: str, body: SchemaChangeIn):
        try:
            result = pipeline.alter_schema(table, _change_from_in(body))
        except KeyColumnProtectedError as exc:
            # 业务键列保护：给出可区分原因，便于上游按原因处理
            raise HTTPException(status_code=409, detail={
                "reason": "KEY_COLUMN_PROTECTED", "message": str(exc)})
        except SchemaError as exc:
            raise HTTPException(status_code=409, detail={"message": str(exc)})
        return {"table": table, "schema_version": result.version,
                "change_type": result.change_type.value,
                "column_name": result.column_name, "to_name": result.to_name}

    @app.post("/schema-campaigns")
    def alter_schemas(body: SchemaCampaignIn):
        """多表结构变更整体提交（DDL 战役）：全部生效或全部不生效。

        幂等键为 campaign_id；跨表更正/业务事务与战役严格整体在前或在后，
        不会出现一半表新结构、一半表旧结构。
        """
        # SchemaChangeIn 通过可选 table 字段携带目标表
        pairs = []
        for c in body.changes:
            if not c.table:
                raise HTTPException(status_code=422, detail={
                    "message": "DDL 战役中每条变更必须带 table"})
            pairs.append((c.table, _change_from_in(c)))
        try:
            results = pipeline.alter_schemas(body.campaign_id, pairs)
        except KeyColumnProtectedError as exc:
            raise HTTPException(status_code=409, detail={
                "reason": "KEY_COLUMN_PROTECTED", "message": str(exc)})
        except SchemaError as exc:
            raise HTTPException(status_code=409, detail={"message": str(exc)})
        return {"campaign_id": body.campaign_id,
                "changes": [{"table": tbl, "schema_version": r.version,
                             "change_type": r.change_type.value,
                             "column_name": r.column_name,
                             "to_name": r.to_name}
                            for (tbl, _), r in zip(pairs, results)]}

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

    # -------- 跨表业务批次 / 更正 / 撤回 / 当前状态 --------
    @app.post("/business-batches")
    def submit_business_batch(body: BusinessBatchIn):
        entries = []
        for e in body.entries:
            if any(r.offset is None for r in e.records):
                raise HTTPException(status_code=422,
                                    detail={"message": "每条记录必须带源端 offset"})
            records = [Record(offset=r.offset, data=r.data,
                              schema_version=r.schema_version) for r in e.records]
            entries.append(TableBatchEntry(
                table=e.table, records=records, schema_version=e.schema_version))
        try:
            result = pipeline.submit_business_batch(
                BusinessBatch(body.business_batch_id, entries))
        except BusinessBatchRejected as exc:
            raise business_conflict(exc)
        return _business_result_json(result)

    @app.post("/corrections")
    def submit_correction(body: CorrectionIn):
        items = [CorrectionItem(table=i.table, key=i.key,
                                op=EventKind(i.op),
                                schema_version=i.schema_version, data=i.data,
                                issued_seq=i.issued_seq)
                 for i in body.items]
        try:
            result = pipeline.submit_correction(
                CorrectionBatch(body.correction_id, items))
        except BusinessBatchRejected as exc:
            raise business_conflict(exc)
        return _business_result_json(result)

    @app.get("/transactions/{txn_id}")
    def get_transaction(txn_id: str):
        trace = pipeline.business_trace(txn_id)
        if trace is None:
            raise HTTPException(status_code=404, detail={"message": "事务不存在"})
        return trace

    @app.get("/tables/{table}/state")
    def current_state(table: str):
        try:
            state = pipeline.current_state(table)
        except SchemaError as exc:
            raise HTTPException(status_code=404, detail={"message": str(exc)})
        # JSON 对象键必须是字符串
        return {"table": table,
                "state": {str(k): v for k, v in state.items()}}

    @app.get("/tables/{table}/state/history")
    def key_history(table: str, key: str):
        # 业务键类型与业务键列一致：能解析为 int 则按 int 匹配
        lookup: Any = int(key) if key.lstrip("-").isdigit() else key
        try:
            events = pipeline.key_history(table, lookup)
        except SchemaError as exc:
            raise HTTPException(status_code=404, detail={"message": str(exc)})
        if not events:
            raise HTTPException(status_code=404,
                                detail={"message": "该业务键无任何事件"})
        return {"table": table, "key": lookup, "history": events}

    return app


def _business_result_json(result) -> dict:
    return {
        "business_batch_id": result.business_batch_id,
        "committed": result.committed,
        "state": result.state.value,
        "replay": result.replay,
        "entries": [{"table": e.table, "accepted_count": e.accepted_count,
                     "checkpoint": e.checkpoint,
                     "reason": e.reason.value if e.reason else None,
                     "detail": e.detail,
                     "rejected": [{"offset": r.offset,
                                   "reason": r.reason.value if r.reason else None,
                                   "detail": r.detail} for r in e.rejected]}
                    for e in result.entries],
    }
