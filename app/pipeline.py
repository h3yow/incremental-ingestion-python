"""增量接入编排。

提交协议（接入协议的服务端实现）
--------------------------------
1. 每张表的所有操作（批次提交 / 结构变更 / 位点推进）在同一把表锁内串行，
   并发提交与结构变更的结论因此确定且可解释。
2. 幂等：以 batch_id 为幂等键。重复提交同一批次 -> 直接回放首次处理结论
   （replay=True），目标端不重复写入，位点不二次推进。
3. 位点连续：批次首条记录 offset 必须 == 当前 checkpoint + 1，
   回退（小于）报 OFFSET_BACKWARD；跳号（大于）报 OFFSET_GAP，
   批次内记录位点必须严格连续 +1。
4. 结构版本：批次 schema_version 不得高于目标当前版本（SCHEMA_VERSION_AHEAD）。
   记录按其生产版本对齐到当前结构。
5. 部分失败不静默：每条记录独立校验；只提交“首个被拒记录之前的连续前缀”，
   之后记录（即使本身合法）本次一律不提交（否则会产生跳过位点的写入），
   结果统一记为 NOT_PROCESSED_AFTER_REJECTION；全部记录结论写入批次留痕。
6. 位点只推进到已提交前缀末尾；调用方修正坏记录后应从 checkpoint+1 重新提交
   （新批次），从而做到不重复、不丢失。
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from app.checkpoint import CheckpointStore, OffsetError
from app.models import (Batch, BatchResult, Column, RecordResult, RejectReason,
                        SchemaChange)
from app.schema_store import SchemaError, SchemaStore
from app.source import LocalSource
from app.storage import JsonlStore, KVStore
from app.target import TargetTable

logger = logging.getLogger("ingestion")

# 连续前缀在首个拒绝处被截断，其后记录的统一可区分原因
NOT_PROCESSED = "NOT_PROCESSED_AFTER_REJECTION"


class BatchRejected(Exception):
    """整批被拒（位点/结构版本级错误，尚未写入任何记录）。"""

    def __init__(self, reason: RejectReason, detail: str):
        self.reason = reason
        super().__init__(f"{reason.value}: {detail}")


def _result_to_dict(r: RecordResult) -> dict:
    return {"offset": r.offset, "accepted": r.accepted,
            "reason": r.reason.value if r.reason else None, "detail": r.detail}


def _result_from_dict(d: dict) -> RecordResult:
    return RecordResult(offset=d["offset"], accepted=d["accepted"],
                        reason=RejectReason(d["reason"]) if d.get("reason") else None,
                        detail=d.get("detail", ""))


class Pipeline:
    def __init__(self, root: str = "data"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # 存储级全局锁：changelog / rows / batches / checkpoints 共享，
        # 配合“同表操作取表锁”的约定，保证并发提交与结构变更可线性化。
        self._store_lock = threading.RLock()
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

        self.checkpoint_kv = KVStore(self.root / "checkpoints.json",
                                     lock=self._store_lock)
        self.checkpoints = CheckpointStore(self.checkpoint_kv)
        self.schemas = SchemaStore(
            JsonlStore(self.root / "changelog.jsonl", lock=self._store_lock))
        self.target = TargetTable(
            JsonlStore(self.root / "rows.jsonl", lock=self._store_lock))
        self._batch_store = JsonlStore(self.root / "batches.jsonl",
                                       lock=self._store_lock)

    def _table_lock(self, table: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(table)
            if lock is None:
                lock = threading.RLock()
                self._locks[table] = lock
            return lock


    # ---------------- 建表 / 结构变更 ----------------
    def init_table(self, table: str, columns: list[Column]) -> int:
        lock = self._table_lock(table)
        with lock:
            version = self.schemas.init_table(table, columns)
            logger.info("[DDL] table=%s action=INIT version=%s columns=%s",
                        table, version, [c.name for c in columns])
            return version

    def alter_schema(self, table: str, change: SchemaChange) -> SchemaChange:
        """在表锁内串行执行结构变更（与批次提交互斥）。"""
        lock = self._table_lock(table)
        with lock:
            from app.models import ChangeType
            if change.change_type == ChangeType.ADD_COLUMN:
                result = self.schemas.add_column(table, change.column)
            elif change.change_type == ChangeType.DROP_COLUMN:
                result = self.schemas.drop_column(table, change.column_name)
            elif change.change_type == ChangeType.RENAME_COLUMN:
                result = self.schemas.rename_column(
                    table, change.column_name, change.to_name)
            else:
                raise SchemaError(f"未知变更类型 {change.change_type}")
            logger.info("[DDL] table=%s action=%s version=%s detail=%s->%s",
                        table, result.change_type.value, result.version,
                        result.column_name, result.to_name)
            return result

    # ---------------- 批次留痕 ----------------
    def _find_batch(self, table: str, batch_id: str) -> dict | None:
        for event in self._batch_store.read_all():
            if event["table"] == table and event["batch_id"] == batch_id:
                return event
        return None

    def _save_batch_trace(self, event: dict) -> None:
        self._batch_store.append(event)

    # ---------------- 批次提交 ----------------
    def submit_batch(self, batch: Batch) -> BatchResult:
        table = batch.table
        lock = self._table_lock(table)
        with lock:
            return self._submit_locked(batch)

    def _submit_locked(self, batch: Batch) -> BatchResult:
        table = batch.table

        if not self.schemas.is_initialized(table):
            raise BatchRejected(RejectReason.UNKNOWN_COLUMN,
                                f"table={table} 不存在")

        # 幂等：同 batch_id 回放首次结论
        existing = self._find_batch(table, batch.batch_id)
        if existing is not None:
            replay = BatchResult(
                batch_id=existing["batch_id"], table=table,
                schema_version=existing["schema_version"],
                accepted_count=existing["accepted_count"],
                rejected=[_result_from_dict(d) for d in existing["results"]
                          if not d["accepted"]],
                checkpoint=existing["checkpoint_after"], replay=True)
            logger.info("[BATCH] table=%s batch=%s REPLAY checkpoint=%s accepted=%s",
                        table, batch.batch_id, replay.checkpoint, replay.accepted_count)
            return replay

        current_version = self.schemas.current_version(table)
        checkpoint = self.checkpoints.get(table)

        # 结构版本不得超前
        if batch.schema_version > current_version:
            raise BatchRejected(
                RejectReason.SCHEMA_VERSION_AHEAD,
                f"batch={batch.batch_id} 基于版本 {batch.schema_version}，"
                f"目标当前仅版本 {current_version}")

        # 位点连续性检查（整批，先于任何写入）
        if not batch.records:
            raise BatchRejected(RejectReason.OFFSET_GAP, "空批次")
        first = batch.records[0].offset
        expected_first = checkpoint + 1
        if first < expected_first:
            raise BatchRejected(
                RejectReason.OFFSET_BACKWARD,
                f"batch={batch.batch_id} 首位点 {first} <= 已提交位点 {checkpoint}，拒绝回退")
        if first > expected_first:
            raise BatchRejected(
                RejectReason.OFFSET_GAP,
                f"batch={batch.batch_id} 首位点 {first}，期望 {expected_first}，存在跳号")
        prev = first - 1
        for rec in batch.records:
            if rec.offset != prev + 1:
                raise BatchRejected(
                    RejectReason.OFFSET_GAP,
                    f"批次内位点不连续：{prev} 之后出现 {rec.offset}")
            prev = rec.offset

        current_columns = self.schemas.columns(table)
        changes = self.schemas.changes(table)

        # 逐条校验 + 对齐；找到首个拒绝点即截断
        results: list[RecordResult] = []
        aligned_rows: list[tuple[int, dict]] = []
        rejected = False
        for rec in batch.records:
            if rejected:
                results.append(RecordResult(
                    rec.offset, False, None,
                    f"{NOT_PROCESSED}：前序记录已失败，位点必须连续，等待从 {checkpoint + 1} 重提"))
                continue
            rec_version = rec.schema_version or batch.schema_version
            if rec_version > current_version:
                results.append(RecordResult(
                    rec.offset, False, RejectReason.SCHEMA_VERSION_AHEAD,
                    f"记录基于版本 {rec_version}，目标当前 {current_version}"))
                rejected = True
                continue
            old_columns = self.schemas.columns_at(table, rec_version)
            err, aligned = self.target.align_record(
                rec.data, rec_version, current_columns, old_columns, changes)
            if err is not None:
                results.append(RecordResult(rec.offset, False, err.reason, err.detail))
                rejected = True
                continue
            aligned_rows.append((rec.offset, aligned))
            results.append(RecordResult(rec.offset, True))

        # 提交连续前缀并推进位点（同一表锁内，崩溃边界外不会观察到中间态）
        new_checkpoint = checkpoint
        for offset, row in aligned_rows:
            self.target.write_row(table, offset, row, current_version)
            new_checkpoint = offset
        self.checkpoints.advance(table, checkpoint, new_checkpoint)

        rejected_results = [r for r in results if not r.accepted]
        trace = {
            "table": table,
            "batch_id": batch.batch_id,
            "schema_version": current_version,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "accepted_count": len(aligned_rows),
            "results": [_result_to_dict(r) for r in results],
        }
        self._save_batch_trace(trace)

        logger.info(
            "[BATCH] table=%s batch=%s version=%s checkpoint=%s->%s accepted=%s rejected=%s reasons=%s",
            table, batch.batch_id, current_version, checkpoint, new_checkpoint,
            len(aligned_rows), len(rejected_results),
            [r.reason.value if r.reason else NOT_PROCESSED for r in rejected_results])

        return BatchResult(
            batch_id=batch.batch_id, table=table, schema_version=current_version,
            accepted_count=len(aligned_rows), rejected=rejected_results,
            checkpoint=new_checkpoint, replay=False)

    # ---------------- 从源端拉取一轮 ----------------
    def run_once(self, table: str, source: LocalSource, batch_id: str,
                 limit: int = 100) -> BatchResult:
        """从当前位点之后从源端拉一批并提交（断点重启后自然续传）。"""
        start = self.checkpoints.get(table) + 1
        records = source.read(table, start, limit)
        if not records:
            return BatchResult(batch_id=batch_id, table=table,
                               schema_version=self.schemas.current_version(table),
                               accepted_count=0, checkpoint=start - 1)
        batch = Batch(batch_id=batch_id, table=table, records=records,
                      schema_version=self.schemas.current_version(table))
        return self.submit_batch(batch)

    # ---------------- 查询辅助 ----------------
    def snapshot(self, table: str) -> dict:
        return {
            "checkpoint": self.checkpoints.get(table),
            "schema_version": self.schemas.current_version(table),
            "rows": self.target.load(table),
        }

    def batch_trace(self, table: str, batch_id: str) -> dict | None:
        return self._find_batch(table, batch_id)
