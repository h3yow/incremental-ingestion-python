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
from app.models import (Batch, BatchResult, BusinessBatch, BusinessBatchResult,
                        Column, CorrectionBatch, EntryResult, EventKind,
                        RecordResult, RejectReason, SchemaChange, TxState)
from app.schema_store import SchemaError, SchemaStore
from app.source import LocalSource
from app.state_store import StateStore
from app.storage import JsonlStore, KVStore
from app.target import TargetTable
from app.tx_log import TxLog

logger = logging.getLogger("ingestion")

# 连续前缀在首个拒绝处被截断，其后记录的统一可区分原因
NOT_PROCESSED = "NOT_PROCESSED_AFTER_REJECTION"


class BatchRejected(Exception):
    """整批被拒（位点/结构版本级错误，尚未写入任何记录）。"""

    def __init__(self, reason: RejectReason, detail: str):
        self.reason = reason
        super().__init__(f"{reason.value}: {detail}")


class BusinessBatchRejected(Exception):
    """跨表业务批次 / 更正批次整体被拒（预检阶段，尚未应用任何效果）。

    failures 精确指出：哪个业务批次、哪张表、哪类数据出的问题。
    """

    def __init__(self, txn_id: str, failures: list["EntryFailure"]):
        self.txn_id = txn_id
        self.failures = failures
        super().__init__(
            f"txn={txn_id} 整体中止：" +
            "; ".join(f"{f.table}:{f.reason.value}:{f.detail}" for f in failures))


class EntryFailure:
    """业务批次内单个分片/单条更正的预检失败定位。"""

    def __init__(self, table: str, reason: RejectReason, detail: str,
                 offset: int | None = None, key: object = None):
        self.table = table
        self.reason = reason
        self.detail = detail
        self.offset = offset
        self.key = key


class _MultiLock:
    """按固定顺序持有多把表锁的上下文管理器（排序在外层完成，消除加锁环）。"""

    def __init__(self, locks: list):
        self._locks = locks

    def __enter__(self):
        for lock in self._locks:
            lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        for lock in reversed(self._locks):
            lock.release()
        return False


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
        # 结构变更与跨表事务的全局协调锁：
        # 任何 DDL 与任何“跨多张表”的业务/更正事务互斥，保证跨表事务在其整个
        # 生命周期内看到的是同一个结构快照（不会前几张表按旧字段、后几张按新字段），
        # 同时也消除「跨表事务多锁」与「DDL 单锁」之间的加锁环。
        self._ddl_lock = threading.RLock()
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

        self.checkpoint_kv = KVStore(self.root / "checkpoints.json",
                                     lock=self._store_lock)
        self.checkpoints = CheckpointStore(self.checkpoint_kv)
        self.schemas = SchemaStore(
            JsonlStore(self.root / "changelog.jsonl", lock=self._store_lock))
        rows_store = JsonlStore(self.root / "rows.jsonl", lock=self._store_lock)
        self.target = TargetTable(rows_store)
        self.state = StateStore(
            rows_store,
            KVStore(self.root / "seq.json", lock=self._store_lock))
        self.tx_log = TxLog(
            JsonlStore(self.root / "transactions.jsonl", lock=self._store_lock))
        self._batch_store = JsonlStore(self.root / "batches.jsonl",
                                       lock=self._store_lock)
        self.recover()  # 崩溃恢复：重放仍处于 PREPARED 的业务事务

    def _table_lock(self, table: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(table)
            if lock is None:
                lock = threading.RLock()
                self._locks[table] = lock
            return lock


    # ---------------- 建表 / 结构变更 ----------------
    def init_table(self, table: str, columns: list[Column],
                   key_col: str | None = None) -> int:
        lock = self._table_lock(table)
        with lock:
            version = self.schemas.init_table(table, columns, key_col=key_col)
            logger.info("[DDL] table=%s action=INIT version=%s key_col=%s columns=%s",
                        table, version, self.schemas.key_col(table),
                        [c.name for c in columns])
            return version

    def alter_schema(self, table: str, change: SchemaChange) -> SchemaChange:
        """在 DDL 协调锁 + 表锁内串行执行结构变更。

        协调锁保证：结构变更不会插入到一个跨表业务/更正事务的中间，
        因而跨表事务的所有表分片按同一结构快照解释。
        """
        from app.models import ChangeType
        with self._ddl_lock:
            lock = self._table_lock(table)
            with lock:
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

        # 提交连续前缀并推进位点（同一表锁内，崩溃边界外不会观察到中间态）。
        # 每行分配表级写序号：与跨表批次、更正事件共享同一条全序轴，
        # 多条提交路径交替写同一业务键时，折叠顺序仍等于真实提交顺序。
        new_checkpoint = checkpoint
        for offset, row in aligned_rows:
            seq = self.state.allocate_write_seq(table)
            self.target.write_row(table, offset, row, current_version, seq=seq)
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

    # ---------------- 跨表业务批次 / 更正 / 撤回 ----------------
    def _multi_lock(self, tables: list[str]):
        """按表名排序获取多把表锁：消除跨表加锁环，避免死锁。

        涉及多张表时额外持有 DDL 协调锁（锁序恒为：协调锁 -> 表锁，
        与 alter_schema 一致），从而：
        - 跨表事务期间不会插入结构变更（整批按同一结构快照解释，
          不会前几张表按旧字段、后几张表按新字段）；
        - 协调锁与表锁之间不存在加锁环（单表事务不取协调锁）。
        """
        uniq = sorted(set(tables))
        locks = ([self._ddl_lock] if len(uniq) > 1 else []) \
            + [self._table_lock(t) for t in uniq]
        return _MultiLock(locks)

    def _row_exists(self, table: str, offset: int) -> bool:
        return any(r["table"] == table and r.get("offset") == offset
                   and r.get("kind") is None
                   for r in self.target.rows.read_all())

    def _validate_append_entry(self, entry, failures, plans) -> None:
        """校验并对齐一个 APPEND 分片；成功在 plans 中放入待生效计划，否则记 failure。"""
        table = entry.table
        if not self.schemas.is_initialized(table):
            failures.append(EntryFailure(
                table, RejectReason.TABLE_NOT_INITIALIZED,
                f"txn 涉及未建表的表 {table}"))
            return
        current_version = self.schemas.current_version(table)
        checkpoint = self.checkpoints.get(table)
        if entry.schema_version > current_version:
            failures.append(EntryFailure(
                table, RejectReason.SCHEMA_VERSION_AHEAD,
                f"分片基于版本 {entry.schema_version}，目标当前 {current_version}"))
            return
        if not entry.records:
            failures.append(EntryFailure(table, RejectReason.OFFSET_GAP, "空分片"))
            return
        first = entry.records[0].offset
        expected_first = checkpoint + 1
        if first < expected_first:
            failures.append(EntryFailure(
                table, RejectReason.OFFSET_BACKWARD,
                f"首位点 {first} <= 已提交位点 {checkpoint}"))
            return
        if first > expected_first:
            failures.append(EntryFailure(
                table, RejectReason.OFFSET_GAP,
                f"首位点 {first}，期望 {expected_first}，存在跳号"))
            return
        prev = first - 1
        for rec in entry.records:
            if rec.offset != prev + 1:
                failures.append(EntryFailure(
                    table, RejectReason.OFFSET_GAP,
                    f"分片内位点不连续：{prev} 之后出现 {rec.offset}"))
                return
            prev = rec.offset

        key_col = self.schemas.key_col(table)
        current_columns = self.schemas.columns(table)
        changes = self.schemas.changes(table)
        planned_rows: list[tuple[int, dict]] = []
        for rec in entry.records:
            rec_version = rec.schema_version or entry.schema_version
            if rec_version > current_version:
                failures.append(EntryFailure(
                    table, RejectReason.SCHEMA_VERSION_AHEAD,
                    f"offset={rec.offset} 记录基于版本 {rec_version}，"
                    f"目标当前 {current_version}", offset=rec.offset))
                return
            err, aligned = self.target.align_record(
                rec.data, rec_version, current_columns,
                self.schemas.columns_at(table, rec_version), changes)
            if err is not None:
                failures.append(EntryFailure(
                    table, err.reason, f"offset={rec.offset} {err.detail}",
                    offset=rec.offset))
                return
            if aligned.get(key_col) is None:
                failures.append(EntryFailure(
                    table, RejectReason.MISSING_REQUIRED,
                    f"offset={rec.offset} 业务键列 {key_col!r} 缺失或为空",
                    offset=rec.offset))
                return
            planned_rows.append((rec.offset, aligned))

        plans.append({
            "table": table,
            "checkpoint_before": checkpoint,
            "checkpoint_after": planned_rows[-1][0],
            "rows": planned_rows,
            "schema_version": current_version,
        })

    def _apply_plan(self, plan: dict) -> None:
        """幂等应用一个 APPEND 计划：行按 offset 去重，位点 CAS 推进。"""
        table = plan["table"]
        for item in plan["rows"]:
            # JSONL 往返后为 [offset, row]，此处不依赖 tuple 解包
            offset, row = item[0], item[1]
            if self._row_exists(table, offset):
                continue
            seq = self.state.allocate_write_seq(table)
            self.target.write_row(table, offset, row,
                                  plan["schema_version"], seq=seq)
        self.checkpoints.ensure(table, plan["checkpoint_after"])

    @staticmethod
    def _aborted_from_doc(doc: dict) -> BusinessBatchRejected:
        """从 ABORTED 留痕重建首次拒绝异常（失败结论的幂等回放）。"""
        failures = []
        for e in doc["entries"]:
            if e.get("reason"):
                failures.append(EntryFailure(
                    e["table"], RejectReason(e["reason"]), e.get("detail", ""),
                    key=e.get("key")))
        return BusinessBatchRejected(doc["txn_id"], failures)

    def _result_from_doc(self, doc: dict, replay: bool) -> BusinessBatchResult:
        entries = []
        for e in doc["entries"]:
            rejected = [_result_from_dict(d) for d in e.get("rejected", [])]
            entries.append(EntryResult(
                table=e["table"], accepted_count=e.get("accepted_count", 0),
                checkpoint=e.get("checkpoint", 0), rejected=rejected,
                reason=RejectReason(e["reason"]) if e.get("reason") else None,
                detail=e.get("detail", "")))
        return BusinessBatchResult(
            business_batch_id=doc["txn_id"],
            committed=doc["state"] == TxState.COMMITTED.value,
            entries=entries, state=TxState(doc["state"]), replay=replay)

    def submit_business_batch(self, batch: BusinessBatch) -> BusinessBatchResult:
        """跨表业务批次整体提交：涉及的表全部生效或全部不生效。"""
        if not batch.entries:
            raise BusinessBatchRejected(batch.business_batch_id, [])

        # 幂等：同一业务批次重复提交 -> 回放首次结论（无论提交/中止）
        existing = self.tx_log.find(batch.business_batch_id)
        if existing is not None:
            logger.info("[TXN] txn=%s REPLAY state=%s",
                        batch.business_batch_id, existing["state"])
            if existing["state"] == TxState.ABORTED.value:
                raise self._aborted_from_doc(existing)
            return self._result_from_doc(existing, replay=True)

        tables = [e.table for e in batch.entries]
        with self._multi_lock(tables):
            failures: list[EntryFailure] = []
            plans: list[dict] = []
            # 同表多分片不允许（位点连续协议下会互相冲突，语义不明确）
            if len(set(tables)) != len(tables):
                dup = sorted({t for t in tables if tables.count(t) > 1})
                failures.append(EntryFailure(
                    dup[0], RejectReason.ENTRY_INVALID,
                    f"同一业务批次内表 {dup} 出现多个分片，不允许"))
            else:
                for entry in batch.entries:
                    self._validate_append_entry(entry, failures, plans)
                    if failures:
                        break

            entry_results = []
            if failures:
                # 预检失败：不应用任何效果，写 ABORTED 留痕（精确定位表/数据）
                for e in batch.entries:
                    f = next((x for x in failures if x.table == e.table), None)
                    if f is not None:
                        entry_results.append({
                            "table": e.table, "accepted_count": 0,
                            "checkpoint": self.checkpoints.get(e.table)
                            if self.schemas.is_initialized(e.table) else 0,
                            "rejected": [], "reason": f.reason.value,
                            "detail": f.detail})
                    else:
                        entry_results.append({
                            "table": e.table, "accepted_count": 0,
                            "checkpoint": self.checkpoints.get(e.table),
                            "rejected": [], "reason": None, "detail": ""})
                doc = {"txn_id": batch.business_batch_id, "kind": "BUSINESS",
                       "state": TxState.ABORTED.value,
                       "entries": entry_results, "plans": []}
                self.tx_log.append_state(doc)
                logger.info("[TXN] txn=%s ABORT failures=%s",
                            batch.business_batch_id,
                            [(f.table, f.reason.value, f.detail) for f in failures])
                raise BusinessBatchRejected(batch.business_batch_id, failures)

            # 阶段一：待生效内容（已对齐、已确定）整体落盘
            prepared_entries = [{
                "table": p["table"], "accepted_count": len(p["rows"]),
                "checkpoint": p["checkpoint_after"], "rejected": [],
                "reason": None, "detail": ""} for p in plans]
            doc = {"txn_id": batch.business_batch_id, "kind": "BUSINESS",
                   "state": TxState.PREPARED.value,
                   "entries": prepared_entries, "plans": plans}
            self.tx_log.append_state(doc)

            # 阶段二：应用并提交（崩溃后由 recover() 按同一文档重放）
            self._commit_prepared(doc)
            logger.info("[TXN] txn=%s COMMITTED tables=%s checkpoints=%s",
                        batch.business_batch_id,
                        [p["table"] for p in plans],
                        {p["table"]: (p["checkpoint_before"], p["checkpoint_after"])
                         for p in plans})
            committed = dict(doc, state=TxState.COMMITTED.value)
            return self._result_from_doc(committed, replay=False)

    def _commit_prepared(self, doc: dict) -> None:
        """按 PREPARED 文档幂等应用全部效果并追加 COMMITTED。"""
        for plan in doc["plans"]:
            if plan.get("type", "append") == "append":
                self._apply_plan(plan)
            else:
                self._apply_correction_plan(plan)
        committed = dict(doc, state=TxState.COMMITTED.value)
        self.tx_log.append_state(committed)

    def recover(self) -> None:
        """启动恢复：按落盘顺序重放仍为 PREPARED 的业务事务到同一最终状态。"""
        pending = self.tx_log.prepared_open()
        if not pending:
            return
        tables = sorted({p["table"] for doc in pending for p in doc["plans"]})
        with self._multi_lock(tables):
            for doc in self.tx_log.prepared_open():
                # 重取：前一个事务的恢复不会改变后续文档状态，此处直接提交
                self._commit_prepared(doc)
                logger.info("[RECOVER] txn=%s rolled forward to COMMITTED",
                            doc["txn_id"])

    def business_trace(self, txn_id: str) -> dict | None:
        """查询业务事务留痕（PREPARED/COMMITTED/ABORTED 及逐表结论）。"""
        return self.tx_log.find(txn_id)

    def current_state(self, table: str) -> dict[object, dict]:
        """查询某表当前状态：仅含最后一次有效结论，不含已撤回键。"""
        return self.state.current(table, self.schemas.key_col(table))

    def key_history(self, table: str, key: object) -> list[dict]:
        """解释某条业务结果经历过的全部增量写入与更正/撤回。"""
        return self.state.history(table, key, self.schemas.key_col(table))

    def _validate_correction_item(self, item, failures, plans) -> None:
        """校验单条更正/撤回；成功在 plans 放入待生效事件计划，否则记 failure。"""
        table = item.table
        if not self.schemas.is_initialized(table):
            failures.append(EntryFailure(
                table, RejectReason.TABLE_NOT_INITIALIZED,
                f"key={item.key!r} 涉及未建表的表 {table}", key=item.key))
            return
        current_version = self.schemas.current_version(table)
        if item.schema_version > current_version:
            failures.append(EntryFailure(
                table, RejectReason.SCHEMA_VERSION_AHEAD,
                f"key={item.key!r} 更正基于版本 {item.schema_version}，"
                f"目标当前 {current_version}", key=item.key))
            return
        key_col = self.schemas.key_col(table)
        current = self.state.current(table, key_col)
        if item.key not in current:
            failures.append(EntryFailure(
                table, RejectReason.KEY_NOT_FOUND,
                f"key={item.key!r} 当前无有效结果（从未接入或已撤回），"
                f"无法更正/撤回", key=item.key))
            return

        aligned = None
        if item.op == EventKind.WITHDRAW:
            if item.data is not None:
                failures.append(EntryFailure(
                    table, RejectReason.ENTRY_INVALID,
                    f"key={item.key!r} 撤回不得携带数据", key=item.key))
                return
        else:
            if item.data is None:
                failures.append(EntryFailure(
                    table, RejectReason.ENTRY_INVALID,
                    f"key={item.key!r} 更正必须携带完整数据", key=item.key))
                return
            # 业务键列必须与目标键一致且存在，防止更正落到错误的键上
            if item.data.get(key_col) != item.key:
                failures.append(EntryFailure(
                    table, RejectReason.ENTRY_INVALID,
                    f"key={item.key!r} 与数据中的业务键列 {key_col!r}="
                    f"{item.data.get(key_col)!r} 不一致", key=item.key))
                return
            changes = self.schemas.changes(table)
            err, aligned = self.target.align_record(
                item.data, item.schema_version,
                self.schemas.columns(table),
                self.schemas.columns_at(table, item.schema_version), changes)
            if err is not None:
                failures.append(EntryFailure(
                    table, err.reason, f"key={item.key!r} {err.detail}",
                    key=item.key))
                return

        plans.append({
            "type": "correction",
            "table": table,
            "key": item.key,
            "op": item.op.value,
            "data": aligned,
            "schema_version": current_version,
        })

    def _apply_correction_plan(self, plan: dict) -> None:
        """幂等应用一条更正计划：已应用过的 (correction_id, table, key) 跳过。"""
        # correction_id 由调用处（COMMIT 文档）注入到计划中
        correction_id = plan["correction_id"]
        if self.state.has_event(correction_id, plan["table"], plan["key"]):
            return
        kind = EventKind(plan["op"])
        seq = self.state.allocate_write_seq(plan["table"])
        self.state.append_event(
            plan["table"], plan["key"], kind, plan["data"],
            plan["schema_version"], seq, correction_id)

    def submit_correction(self, correction: CorrectionBatch) -> BusinessBatchResult:
        """历史更正/撤回请求：按业务键覆盖或撤回，可跨表整体生效，幂等。

        - 幂等：相同 correction_id 重复提交直接回放首次结论（不再次应用、不翻转）；
        - 中断重放：PREPARED 已落盘时崩溃，重启自动前滚，(id,table,key) 去重；
        - 整体生效：任一条预检失败则全部不生效，ABORTED 留痕精确定位。
        """
        if not correction.items:
            raise BusinessBatchRejected(correction.correction_id, [])

        existing = self.tx_log.find(correction.correction_id)
        if existing is not None:
            logger.info("[CORRECTION] id=%s REPLAY state=%s",
                        correction.correction_id, existing["state"])
            if existing["state"] == TxState.ABORTED.value:
                raise self._aborted_from_doc(existing)
            return self._result_from_doc(existing, replay=True)

        tables = [i.table for i in correction.items]
        with self._multi_lock(tables):
            failures: list[EntryFailure] = []
            plans: list[dict] = []
            for item in correction.items:
                self._validate_correction_item(item, failures, plans)
                if failures:
                    break

            def item_entry(table: str, key: object) -> dict:
                f = next((x for x in failures if x.table == table and x.key == key),
                         None)
                if f is None:
                    return {"table": table, "accepted_count": 1, "checkpoint": 0,
                            "rejected": [], "reason": None, "detail": "",
                            "key": key}
                return {"table": table, "accepted_count": 0, "checkpoint": 0,
                        "rejected": [], "reason": f.reason.value,
                        "detail": f.detail, "key": key}

            if failures:
                entries = [item_entry(i.table, i.key) for i in correction.items]
                doc = {"txn_id": correction.correction_id, "kind": "CORRECTION",
                       "state": TxState.ABORTED.value,
                       "entries": entries, "plans": []}
                self.tx_log.append_state(doc)
                logger.info("[CORRECTION] id=%s ABORT failures=%s",
                            correction.correction_id,
                            [(f.table, f.key, f.reason.value) for f in failures])
                raise BusinessBatchRejected(correction.correction_id, failures)

            # 在计划内注入 correction_id，供应用期去重
            for plan in plans:
                plan["correction_id"] = correction.correction_id
            entries = [{"table": p["table"], "accepted_count": 1,
                        "checkpoint": 0, "rejected": [], "reason": None,
                        "detail": "", "key": p["key"]} for p in plans]
            doc = {"txn_id": correction.correction_id, "kind": "CORRECTION",
                   "state": TxState.PREPARED.value,
                   "entries": entries, "plans": plans}
            self.tx_log.append_state(doc)

            self._commit_prepared(doc)
            logger.info("[CORRECTION] id=%s COMMITTED items=%s",
                        correction.correction_id,
                        [(p["table"], p["key"], p["op"]) for p in plans])
            committed = dict(doc, state=TxState.COMMITTED.value)
            return self._result_from_doc(committed, replay=False)
