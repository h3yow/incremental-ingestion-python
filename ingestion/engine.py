"""接入引擎：批次校验、逐条校验/兼容映射、落地、台账与位点推进。

提交流程（全程持有该表的表锁）：
  1. 台账查重：batch_id 已存在
       - 内容指纹相同 -> 返回既有结论（duplicate=True），不重复写目标端；
       - 指纹不同 -> BATCH_CONFLICT，拒绝。
  2. 位点校验：start_offset 必须恰好等于 checkpoint（next_offset），
     回退 -> OFFSET_BACKWARD，越界 -> OFFSET_GAP。
  3. 结构校验：批次携带的 schema_version 不得高于目标端当前版本；
     记录始终按“目标端当前版本”做按列名校验与映射（杜绝删列/改名错位）。
  4. 逐条处理：可拒绝原因见 RejectReason；合法记录 upsert 到目标端（幂等），
     拒绝记录不落地、但进入批次台账，绝不静默丢弃。
  5. 先存不可变台账结论，再推进 checkpoint 到 end_offset。
     崩溃恢复时重放同一批次：upsert 幂等、台账不可变，因此不重复不丢失。
"""
from __future__ import annotations

import logging
from dataclasses import replace

from ingestion.errors import (
    BatchConflictError,
    IncompatibleSchemaChangeError,
    SchemaVersionAheadError,
)
from ingestion.ledger import BatchLedger, fingerprint
from ingestion.locking import TableLock
from ingestion.models import (
    Batch,
    BatchResult,
    Column,
    RejectedRecord,
    Schema,
)
from ingestion.offsets import CheckpointManager
from ingestion.schema import TYPE_MAP, SchemaRegistry
from ingestion.storage import LocalSource, LocalTarget

logger = logging.getLogger("ingestion.engine")


class IngestionEngine:
    def __init__(
        self,
        registry: SchemaRegistry,
        source: LocalSource,
        target: LocalTarget,
        checkpoints: CheckpointManager,
        ledger: BatchLedger,
        lock_dir: str,
    ) -> None:
        self.registry = registry
        self.source = source
        self.target = target
        self.checkpoints = checkpoints
        self.ledger = ledger
        self.lock = TableLock(lock_dir)

    # ------------------------------------------------------------------ #
    # 单条记录的校验与兼容映射
    # ------------------------------------------------------------------ #
    def _check_value(self, col: Column, value: object) -> str | None:
        """返回 None 表示通过，否则返回 RejectReason 值。"""
        expected = TYPE_MAP[col.type]
        if col.type == "int":
            # bool 是 int 的子类，必须显式排除，避免 True 被当成 1
            ok = isinstance(value, int) and not isinstance(value, bool)
        elif col.type == "bool":
            ok = isinstance(value, bool)
        else:
            ok = isinstance(value, expected)
        if not ok:
            return "TYPE_MISMATCH"
        return None

    def _validate_and_project(
        self, schema: Schema, record: dict[str, object]
    ) -> tuple[object, dict[str, object] | None, RejectedRecord | None]:
        """按当前结构把一条记录映射为目标行；失败返回带原因的拒绝记录（offset 后补）。"""
        # 1) 主键
        if schema.key_column not in record:
            return None, None, RejectedRecord(
                offset=-1, key=None,
                reason="MISSING_KEY",
                detail=f"缺少主键列 {schema.key_column}",
            )
        key = record[schema.key_column]
        if key is None:
            return None, None, RejectedRecord(
                offset=-1, key=None, reason="NULL_NOT_ALLOWED",
                detail=f"主键列 {schema.key_column} 不允许为 null",
            )
        key_col = schema.get(schema.key_column)
        assert key_col is not None
        bad = self._check_value(key_col, key)
        if bad is not None:
            return None, None, RejectedRecord(
                offset=-1, key=key, reason="TYPE_MISMATCH",
                detail=f"主键列 {schema.key_column} 类型应为 {key_col.type}，实际 {type(key).__name__}",
            )

        # 2) 未知列：删列 / 改名后，旧数据携带的旧列名在此被明确拒绝（不错位）
        unknown = [name for name in record if schema.get(name) is None]
        if unknown:
            return None, None, RejectedRecord(
                offset=-1, key=key, reason="UNKNOWN_COLUMN",
                detail=f"字段在当前结构 v{schema.version} 中不存在：{sorted(unknown)}",
            )

        # 3) 逐列校验并构造输出行（只输出当前结构中的列）
        row: dict[str, object] = {}
        for col in schema.columns:
            present = col.name in record
            value = record.get(col.name)
            if not present or value is None:
                if present and not col.nullable:
                    return None, None, RejectedRecord(
                        offset=-1, key=key, reason="NULL_NOT_ALLOWED",
                        detail=f"列 {col.name} 为 NOT NULL，记录显式给出 null",
                    )
                if not present:
                    if not col.nullable and not col.has_default:
                        return None, None, RejectedRecord(
                            offset=-1, key=key, reason="MISSING_COLUMN",
                            detail=f"NOT NULL 列 {col.name} 缺失且无缺省值",
                        )
                    # 稳定缺省语义：有 default 用 default，否则 nullable -> None
                    row[col.name] = col.default if col.has_default else None
                else:
                    row[col.name] = None
                continue
            bad_reason = self._check_value(col, value)
            if bad_reason is not None:
                return None, None, RejectedRecord(
                    offset=-1, key=key, reason="TYPE_MISMATCH",
                    detail=f"列 {col.name} 类型应为 {col.type}，实际 {type(value).__name__}",
                )
            row[col.name] = value
        return key, row, None

    # ------------------------------------------------------------------ #
    # 批次提交
    # ------------------------------------------------------------------ #
    def submit_batch(self, batch: Batch) -> BatchResult:
        fp = fingerprint(batch)
        with self.lock.table(batch.table):
            # 1) 台账查重
            existing_doc = self.ledger.get_raw(batch.batch_id)
            if existing_doc is not None:
                if existing_doc.get("fingerprint") != fp:
                    raise BatchConflictError(
                        f"批次 {batch.batch_id} 曾以不同内容提交："
                        f"台账指纹={existing_doc.get('fingerprint')[:12]}，"
                        f"本次指纹={fp[:12]}"
                    )
                prior = self.ledger.get(batch.batch_id)
                assert prior is not None
                dup = replace(prior, duplicate=True)
                logger.info(
                    "[BATCH-DUP] batch=%s table=%s 位点=[%s,%s) 直接返回既有结论 "
                    "accepted=%d rejected=%d（不重复写入）",
                    batch.batch_id, batch.table, dup.start_offset, dup.end_offset,
                    dup.accepted_count, dup.rejected_count,
                )
                return dup

            # 2) 位点必须紧邻
            self.checkpoints.validate_adjacent(batch.table, batch.start_offset)

            # 3) 结构版本
            target_schema = self.registry.current(batch.table)
            if batch.schema_version > target_schema.version:
                raise SchemaVersionAheadError(
                    f"批次 {batch.batch_id} 携带结构版本 v{batch.schema_version}，"
                    f"目标端仅到 v{target_schema.version}，拒绝超前结构的数据"
                )

            end_offset = batch.start_offset + len(batch.records)
            logger.info(
                "[BATCH-BEGIN] batch=%s table=%s 位点=[%s,%s) 批次结构=v%s 目标结构=v%s",
                batch.batch_id, batch.table, batch.start_offset, end_offset,
                batch.schema_version, target_schema.version,
            )

            # 4) 逐条校验 + 幂等落地
            accepted: list[int] = []
            rejected: list[RejectedRecord] = []
            for i, record in enumerate(batch.records):
                off = batch.start_offset + i
                key, row, err = self._validate_and_project(target_schema, record)
                if err is not None:
                    rejected.append(replace(err, offset=off))
                    logger.warning(
                        "[RECORD-REJECT] batch=%s offset=%s key=%s reason=%s detail=%s",
                        batch.batch_id, off, err.key, err.reason, err.detail,
                    )
                    continue
                assert row is not None
                self.target.upsert(batch.table, key, row)
                accepted.append(off)

            result = BatchResult(
                batch_id=batch.batch_id,
                table=batch.table,
                start_offset=batch.start_offset,
                end_offset=end_offset,
                schema_version=batch.schema_version,
                applied_schema_version=target_schema.version,
                accepted_offsets=tuple(accepted),
                rejected=tuple(rejected),
                fingerprint=fp,
            )

            # 5) 台账先落盘（不可变），再推进位点
            self.ledger.save(result, fp)
            self.checkpoints.advance(batch.table, end_offset)

            logger.info(
                "[BATCH-COMMIT] batch=%s table=%s 位点推进 %s->%s accepted=%d rejected=%d "
                "拒绝明细=%s",
                batch.batch_id, batch.table, batch.start_offset, end_offset,
                len(accepted), len(rejected),
                [(r.offset, r.reason) for r in rejected],
            )
            return result

    # ------------------------------------------------------------------ #
    # 断点续跑：从本地源端拉取 checkpoint 之后的数据
    # ------------------------------------------------------------------ #
    def pull_and_ingest(self, table: str, max_records: int = 100) -> BatchResult | None:
        cp = self.checkpoints.get(table)
        rows = self.source.read(table, cp, max_records)
        if not rows:
            logger.info("[PULL] table=%s 位点=%s 源端无新数据", table, cp)
            return None
        schema_version = self.registry.current(table).version
        # 同一批拉取内若源端发生了结构升级则按版本边界拆分
        first_ver = rows[0][1]
        cut = 0
        while cut < len(rows) and rows[cut][1] == first_ver:
            cut += 1
        rows = rows[:cut]
        start = rows[0][0]
        batch_id = f"auto-{table}-{start}-{start + len(rows)}"
        batch = Batch(
            batch_id=batch_id,
            table=table,
            start_offset=start,
            schema_version=first_ver,
            records=tuple(data for _, _, data in rows),
        )
        logger.info(
            "[PULL] table=%s 从位点=%s 拉取 %d 条（源结构 v%s）组装批次 %s",
            table, cp, len(rows), first_ver, batch_id,
        )
        return self.submit_batch(batch)

    # ------------------------------------------------------------------ #
    # 结构变更（与提交共用同一把表锁）
    # ------------------------------------------------------------------ #
    def evolve_schema(
        self, table: str, kind: str, **payload: object
    ) -> Schema:
        with self.lock.table(table):
            before = self.registry.current(table)
            if kind == "ADD_COLUMN":
                col = Column(
                    name=str(payload["name"]),
                    type=str(payload["type"]),
                    nullable=bool(payload.get("nullable", True)),
                    has_default="default" in payload,
                    default=payload.get("default"),
                )
                schema = self.registry.add_column(table, col)
            elif kind == "DROP_COLUMN":
                schema = self.registry.drop_column(table, str(payload["name"]))
            elif kind == "RENAME_COLUMN":
                schema = self.registry.rename_column(
                    table, str(payload["old_name"]), str(payload["new_name"])
                )
            else:
                raise IncompatibleSchemaChangeError(f"未知结构变更类型：{kind}")
            logger.info(
                "[SCHEMA-CHANGE] seq流水 表=%s v%s->v%s kind=%s payload=%s",
                table, before.version, schema.version, kind, payload,
            )
            return schema

