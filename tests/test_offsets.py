"""位点与断点：回退拒绝、越界拒绝、中断重建后不重不丢。"""
from __future__ import annotations

from ingestion.errors import OffsetBackwardError, OffsetGapError
from ingestion.models import Batch
from tests.harness import IngestionHarness, make_engine


def _batch(bid: str, start: int, *records: dict, schema_version: int = 1) -> Batch:
    return Batch(bid, "users", start, schema_version, records)


class OffsetRuleTests(IngestionHarness):
    def test_first_batch_must_start_at_zero(self) -> None:
        with self.assertRaises(OffsetGapError) as ctx:
            self.engine.submit_batch(_batch("b", 5, {"id": 1, "name": "a"}))
        self.assertEqual(ctx.exception.code, "OFFSET_GAP")
        self.assert_cp(0)  # 拒绝后位点不动

    def test_backward_is_rejected(self) -> None:
        self.engine.submit_batch(_batch("b1", 0, {"id": 1, "name": "a"}))
        with self.assertRaises(OffsetBackwardError) as ctx:
            self.engine.submit_batch(_batch("b2", 0, {"id": 2, "name": "b"}))
        self.assertEqual(ctx.exception.code, "OFFSET_BACKWARD")
        self.assert_cp(1)

    def test_gap_is_rejected(self) -> None:
        self.engine.submit_batch(_batch("b1", 0, {"id": 1, "name": "a"}))
        # 跳过 offset=1 直接从 3 开始：越界推进
        with self.assertRaises(OffsetGapError):
            self.engine.submit_batch(_batch("b3", 3, {"id": 3, "name": "c"}))
        self.assert_cp(1)

    def test_overlapping_batch_partially_rejected(self) -> None:
        self.engine.submit_batch(
            _batch("b1", 0, {"id": 1, "name": "a"}, {"id": 2, "name": "b"})
        )
        # 与已提交区间 [0,2) 重叠
        with self.assertRaises(OffsetBackwardError):
            self.engine.submit_batch(_batch("b2", 1, {"id": 9, "name": "x"}))

    def test_adjacent_batches_resume_cleanly(self) -> None:
        self.engine.submit_batch(_batch("b1", 0, {"id": 1, "name": "a"}))
        r = self.engine.submit_batch(_batch("b2", 1, {"id": 2, "name": "b"}))
        self.assertEqual(r.accepted_offsets, (1,))
        self.assert_cp(2)


class ResumeTests(IngestionHarness):
    """模拟流程中断：销毁内存对象、从同一目录重建引擎，验证不重复不丢失。"""

    def _fill_source(self, n: int) -> None:
        for i in range(n):
            self.engine.source.append("users", {"id": i + 1, "name": f"u{i+1}"}, 1)

    def test_resume_after_interruption_no_dup_no_loss(self) -> None:
        self._fill_source(10)

        # 第一轮：只接入前 4 条（模拟随后进程退出）
        r1 = self.engine.pull_and_ingest("users", max_records=4)
        self.assertIsNotNone(r1)
        self.assertEqual(r1.end_offset, 4)
        self.assertEqual(self.engine.target.count("users"), 4)
        self.assert_cp(4)

        # 重建引擎（同一存储目录）
        engine2 = make_engine(self.dir)
        r2 = engine2.pull_and_ingest("users", max_records=3)
        self.assertEqual((r2.start_offset, r2.end_offset), (4, 7))
        r3 = engine2.pull_and_ingest("users", max_records=10)
        self.assertEqual((r3.start_offset, r3.end_offset), (7, 10))
        idle = engine2.pull_and_ingest("users")
        self.assertIsNone(idle)

        # 最终：10 条数据、主键 1..10 全在、无重复
        self.assertEqual(engine2.target.count("users"), 10)
        rows = engine2.target.all_rows("users")
        self.assertEqual({int(k) for k in rows}, set(range(1, 11)))
        self.assert_cp(10)

    def test_resume_with_partial_batch_failure(self) -> None:
        # 源里混有坏记录；中断后重放同一批次仍是同一结论
        for data in (
            {"id": 1, "name": "a"},
            {"id": 2},  # 坏：MISSING_COLUMN
            {"id": 3, "name": "c"},
        ):
            self.engine.source.append("users", data, 1)
        r1 = self.engine.pull_and_ingest("users", max_records=10)
        self.assertEqual(r1.accepted_count, 2)
        self.assertEqual(r1.rejected[0].reason, "MISSING_COLUMN")
        engine2 = make_engine(self.dir)
        self.assertIsNone(engine2.pull_and_ingest("users"))
        self.assertEqual(engine2.target.count("users"), 2)
        ledger_view = engine2.ledger.get(r1.batch_id)
        self.assertEqual(len(ledger_view.rejected), 1)
