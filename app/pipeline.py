"""增量接入编排。

提交协议（单表位点通道的服务端实现）
------------------------------------
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

跨表业务批次协议（结算：整体提交 / 更正 / 撤回）
------------------------------------------------
A. 一个 BusinessBatch 可含多张关联表的 BusinessOp（UPSERT=写入/更正，
   RETRACT=撤回），按业务键（建表声明的 key_columns）寻址，每条操作带上游
   严格递增的 revision。
B. 提交时按表名全局排序获取全部相关表锁（与上面的单表通道、DDL 共用同一把锁，
   避免死锁并保证三类操作可线性化），在锁内完成“全部校验 -> 分配全局顺序号
   sequence -> 向 business_batches.jsonl 做且只做一次追加（fsync）”。
   追加成功即所有表同时生效；追加缺失/半行即所有表都不生效，因此不存在
   半张表数据。失败精确定位到 batch_id / table / key / reason，且不占顺序号。
C. 当前状态 = 按 sequence 折叠日志，每个业务键只保留 revision 最大的结论；
   revision 不新于已生效结论 -> STALE_REVISION（旧值不翻案、撤回后不复活）。
   全部 UPSERT/RETRACT 历史保留，可逐键解释更正轨迹。
D. 幂等：batch_id 全局唯一，重复提交回放首次结论；进程在追加前/追加中途中断，
   重放后都收敛到同一最终状态（坏尾行由 SettlementJournal 截断丢弃）。
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from app.checkpoint import CheckpointStore, OffsetError
from app.models import (Batch, BatchResult, BusinessBatch,
                        BusinessBatchResult, BusinessOp, Column, OpType,
                        RecordResult, RejectReason, SchemaChange, TableOutcome)
from app.schema_store import SchemaError, SchemaStore
from app.settlement import Conclusion, LedgerView, SettlementJournal
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


class BusinessBatchRejected(Exception):
    """跨表业务批次整体被拒：校验在任何写入前完成，故无任何表生效。

    失败精确定位到业务批次 / 表 / 业务键 / 数据类别。
    """

    def __init__(self, reason: RejectReason, detail: str, *,
                 batch_id: str = "", table: str = "",
                 key: tuple[object, ...] | None = None):
        self.reason = reason
        self.batch_id = batch_id
        self.table = table
        self.key = key
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
        # 跨表业务批次的提交日志：单条追加即整个业务批次的原子提交点
        self.ledger = SettlementJournal(self.root / "business_batches.jsonl")
        # 测试钩子：在 journal 追加前抛异常，模拟“执行到一半中断”
        self._crash_before_business_commit = False

    def _table_lock(self, table: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(table)
            if lock is None:
                lock = threading.RLock()
                self._locks[table] = lock
            return lock


    # ---------------- 建表 / 结构变更 ----------------
    def init_table(self, table: str, columns: list[Column],
                   key_columns: tuple[str, ...] | list[str] = ()) -> int:
        lock = self._table_lock(table)
        with lock:
            version = self.schemas.init_table(table, columns, key_columns)
            logger.info("[DDL] table=%s action=INIT version=%s columns=%s keys=%s",
                        table, version, [c.name for c in columns],
                        list(key_columns))
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

    # ---------------- 跨表业务批次（结算：更正 / 撤回） ----------------
    def submit_business_batch(self, batch: BusinessBatch) -> BusinessBatchResult:
        """跨表业务批次整体提交：所有表都生效，或所有表都不生效。

        顺序保证：先按表名排序获取全部相关表锁（与单表提交 / DDL 共用同一把锁，
        避免死锁且保证业务提交、正常增量、结构变化三者可线性化）；
        在锁内完成「校验 -> 分配顺序号 -> 单次 journal 追加」，
        journal 追加成功即唯一原子提交点。返回后所有表结论同时可见。
        """
        tables_in_batch = sorted({op.table for op in batch.ops})
        locks = [self._table_lock(t) for t in tables_in_batch]
        for lk in locks:
            lk.acquire()
        try:
            return self._business_locked(batch)
        finally:
            for lk in reversed(locks):
                lk.release()

    def _business_reject(self, batch: BusinessBatch, reason: RejectReason,
                         detail: str, table: str = "",
                         key: tuple[object, ...] | None = None) -> BusinessBatchRejected:
        logger.info("[BIZ-BATCH] batch=%s REJECT table=%s key=%s reason=%s detail=%s",
                    batch.batch_id, table, key, reason.value, detail)
        return BusinessBatchRejected(reason, detail, batch_id=batch.batch_id,
                                     table=table, key=key)

    def _business_locked(self, batch: BusinessBatch) -> BusinessBatchResult:
        # 幂等：同一业务批次（全局 batch_id）重复提交 -> 回放首次结论，不二次追加
        existing = self.ledger.find(batch.batch_id)
        if existing is not None:
            result = self._business_result_from_entry(existing, replay=True)
            logger.info("[BIZ-BATCH] batch=%s REPLAY sequence=%s",
                        batch.batch_id, existing["sequence"])
            return result

        if not batch.ops:
            raise self._business_reject(batch, RejectReason.OFFSET_GAP,
                                        "业务批次为空，拒绝提交")

        view = self.ledger.fold(self.ledger.read_commits())

        # ---- 逐表准备与校验（任何失败都在追加前，无任何写入） ----
        prepared_tables: dict[str, dict] = {}
        seen_keys: set[tuple[str, tuple[object, ...]]] = set()
        for op in batch.ops:
            table = op.table
            if not self.schemas.is_initialized(table):
                raise self._business_reject(
                    batch, RejectReason.UNKNOWN_COLUMN,
                    f"table={table} 不存在", table, op.key)
            key_cols = self.schemas.key_columns(table)
            if not key_cols:
                raise self._business_reject(
                    batch, RejectReason.TABLE_NOT_KEYED,
                    f"table={table} 未声明业务键列，不能按业务键更正/撤回",
                    table, op.key)
            if len(op.key) != len(key_cols):
                raise self._business_reject(
                    batch, RejectReason.KEY_MISMATCH,
                    f"table={table} 业务键应包含 {len(key_cols)} 列 {list(key_cols)}，"
                    f"实际 {len(op.key)} 个值 {list(op.key)}", table, op.key)
            # 键值类型按键列校验，避免 1 与 "1" 错位成两条结果
            col_by_name = {c.name: c for c in self.schemas.columns(table)}
            for name, value in zip(key_cols, op.key):
                self._check_key_value_type(batch, table, op.key,
                                           col_by_name[name], value)
            if op.op_type not in (OpType.UPSERT, OpType.RETRACT):
                raise self._business_reject(
                    batch, RejectReason.INVALID_OPERATION,
                    f"table={table} key={op.key} 未知操作类型 {op.op_type!r}",
                    table, op.key)
            if op.op_type == OpType.RETRACT and op.data:
                raise self._business_reject(
                    batch, RejectReason.INVALID_OPERATION,
                    f"table={table} key={op.key} 撤回操作不得携带数据",
                    table, op.key)

            ident = (table, op.key)
            if ident in seen_keys:
                raise self._business_reject(
                    batch, RejectReason.DUPLICATE_KEY_IN_BATCH,
                    f"table={table} key={op.key} 在同一业务批次内出现多次，"
                    "批内结论顺序无法确定", table, op.key)
            seen_keys.add(ident)

            # 确定性顺序：revision 必须严格新于该键已生效的最后结论，
            # 防止旧更正覆盖新值 / 撤回后旧结论“复活”
            current = view.get(table, op.key)
            if current is not None and op.revision <= current.revision:
                raise self._business_reject(
                    batch, RejectReason.STALE_REVISION,
                    f"table={table} key={op.key} 提交 revision={op.revision}，"
                    f"已生效结论 revision={current.revision}（来自批次 "
                    f"{current.batch_id}），旧版本不得翻案", table, op.key)

            # 结构版本：以提交时刻当前结构为准对齐，整个批次在同一锁点内解释，
            # 不会出现同一批更正一半落在改名前字段、一半落在改名后字段
            cur_version = self.schemas.current_version(table)
            if batch.schema_version > cur_version:
                raise self._business_reject(
                    batch, RejectReason.SCHEMA_VERSION_AHEAD,
                    f"批次基于版本 {batch.schema_version}，table={table} 当前仅 "
                    f"{cur_version}", table, op.key)
            aligned: dict[str, object] | None = None
            if op.op_type == OpType.UPSERT:
                if not op.data:
                    raise self._business_reject(
                        batch, RejectReason.INVALID_OPERATION,
                        f"table={table} key={op.key} 更正/写入缺少数据",
                        table, op.key)
                rec_version = op.op_schema_version or batch.schema_version
                aligned = self._align_business_op(batch, op, rec_version)

            prepared_tables.setdefault(table, {
                "table": table, "schema_version": cur_version, "ops": []})
            prepared_tables[table]["ops"].append({
                "op_type": op.op_type.value,
                "key": list(op.key),
                "revision": op.revision,
                "data": aligned,
                "op_schema_version": op.op_schema_version or batch.schema_version,
            })

        # ---- 原子提交：一次 journal 追加 = 所有表同时生效 ----
        sequence = self.ledger.max_sequence() + 1
        entry = {
            "batch_id": batch.batch_id,
            "sequence": sequence,
            "schema_version_head": batch.schema_version,
            "tables": [prepared_tables[t] for t in sorted(prepared_tables)],
        }
        if self._crash_before_business_commit:
            self._crash_before_business_commit = False
            raise RuntimeError("SIMULATED CRASH before journal append")
        self.ledger.append_commit(entry)

        logger.info(
            "[BIZ-BATCH] batch=%s COMMIT sequence=%s tables=%s ops=%s",
            batch.batch_id, sequence,
            [t["table"] for t in entry["tables"]],
            sum(len(t["ops"]) for t in entry["tables"]))
        return self._business_result_from_entry(entry, replay=False)

    def _check_key_value_type(self, batch: BusinessBatch, table: str,
                              key: tuple[object, ...], col, value: object) -> None:
        from app.target import _TYPE_CHECKS
        expected = _TYPE_CHECKS.get(col.typ)
        if value is None or expected is None or not isinstance(value, expected) or \
                (col.typ == "int" and isinstance(value, bool)):
            raise self._business_reject(
                batch, RejectReason.KEY_MISMATCH,
                f"table={table} 键列 {col.name!r} 期望 {col.typ}，"
                f"实际值 {value!r}（{type(value).__name__}）", table, key)

    def _align_business_op(self, batch: BusinessBatch, op: BusinessOp,
                           rec_version: int) -> dict[str, object]:
        table = op.table
        cur_version = self.schemas.current_version(table)
        if rec_version > cur_version:
            raise self._business_reject(
                batch, RejectReason.SCHEMA_VERSION_AHEAD,
                f"table={table} key={op.key} 操作基于版本 {rec_version}，"
                f"目标当前 {cur_version}", table, op.key)
        current_columns = self.schemas.columns(table)
        old_columns = self.schemas.columns_at(table, rec_version)
        changes = self.schemas.changes(table)
        err, aligned = self.target.align_record(
            dict(op.data or {}), rec_version, current_columns, old_columns, changes)
        if err is not None:
            # 精确到：业务批次 / 表 / 业务键 / 数据类别（reason）
            raise self._business_reject(
                batch, err.reason,
                f"table={table} key={op.key} 数据校验失败：{err.detail}",
                table, op.key)
        return aligned

    @staticmethod
    def _business_result_from_entry(entry: dict,
                                    replay: bool) -> BusinessBatchResult:
        outcomes = [
            TableOutcome(table=t["table"], accepted=True, ops=len(t["ops"]),
                         schema_version=t["schema_version"])
            for t in entry["tables"]]
        return BusinessBatchResult(
            batch_id=entry["batch_id"], committed=True,
            sequence=entry["sequence"], tables=outcomes, replay=replay)

    # ---------------- 业务结论查询 ----------------
    def business_view(self) -> LedgerView:
        return self.ledger.fold(self.ledger.read_commits())

    def current_conclusion(self, table: str,
                           key: tuple[object, ...]) -> Conclusion | None:
        """查询某业务键当前最后一次有效结论；撤回后返回状态为 RETRACT 的结论。"""
        return self.business_view().get(table, key)

    def current_business_rows(self, table: str) -> list[dict]:
        """当前状态：只返回未撤回（最后一次有效结论为 UPSERT）的业务数据。"""
        return [dict(c.data) for c in self.business_view().live_rows(table)]

    def business_history(self, table: str,
                         key: tuple[object, ...]) -> list[dict]:
        """该业务键经历过的全部更正/撤回轨迹（处理痕迹不抹除，可解释）。"""
        hist = self.business_view().history.get((table, key), [])
        return [{
            "batch_id": c.batch_id, "sequence": c.sequence,
            "revision": c.revision, "status": c.status.value,
            "data": c.data, "schema_version": c.schema_version,
        } for c in hist]

    def business_trace(self, batch_id: str) -> dict | None:
        return self.ledger.find(batch_id)

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
