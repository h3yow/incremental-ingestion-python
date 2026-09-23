"""批次提交与位点管理测试：
重复提交幂等、部分失败留痕、位点回退/跳号拒绝、断点重启不重不丢。
"""

from app.models import Batch, Record, RejectReason
from app.pipeline import BatchRejected, Pipeline
from tests._helpers import IngestionCase


class BatchAndOffsetTest(IngestionCase):
    def test_duplicate_submit_is_idempotent(self):
        """同一批次重复提交不得重复写入，且回放首次结论。"""
        for i in range(1, 4):
            self.source.produce("orders", {"id": i, "name": f"n{i}"}, 1)
        batch = Batch("b1", "orders", self.source.read("orders", 1), 1)

        r1 = self.pipeline.submit_batch(batch)
        self.assertEqual(r1.accepted_count, 3)
        self.assertEqual(r1.checkpoint, 3)
        self.assertFalse(r1.replay)

        r2 = self.pipeline.submit_batch(batch)
        self.assertTrue(r2.replay, "重复提交必须标记为回放")
        self.assertEqual(r2.accepted_count, 3)
        self.assertEqual(r2.checkpoint, 3)
        self.assertEqual(len(self.pipeline.target.load("orders")), 3,
                         "重复提交不得重复写入")

    def test_partial_failure_is_traced_with_distinct_reasons(self):
        """部分失败不静默丢弃：拒绝原因可区分，成功部分留痕，位点停在连续前缀。"""
        recs = [
            Record(1, {"id": 1, "name": "ok1"}, 1),
            Record(2, {"id": "bad", "name": "bad-type"}, 1),
            Record(3, {"id": 3, "name": "after"}, 1),
            Record(4, {"id": 4, "ghost": 1}, 1),  # 未知列（但位于拒绝点之后）
        ]
        r = self.pipeline.submit_batch(Batch("b1", "orders", recs, 1))
        self.assertEqual(r.accepted_count, 1)
        self.assertEqual(r.checkpoint, 1)
        reasons = [x.reason for x in r.rejected]
        self.assertEqual(reasons[0], RejectReason.TYPE_MISMATCH)
        # 后续记录原因是“首个拒绝后未处理”，与真正校验失败可区分
        self.assertTrue(all("NOT_PROCESSED_AFTER_REJECTION" in x.detail
                            for x in r.rejected[1:]))
        rows = self.pipeline.target.load("orders")
        self.assertEqual([row["id"] for row in rows], [1])

        # 批次留痕可追溯：全部记录的逐条结论都在
        trace = self.pipeline.batch_trace("orders", "b1")
        self.assertEqual(len(trace["results"]), 4)
        self.assertEqual(trace["checkpoint_before"], 0)
        self.assertEqual(trace["checkpoint_after"], 1)

    def test_resume_after_fixing_bad_record_loses_nothing(self):
        """修复坏记录后从 checkpoint+1 重提：不重复也不丢失。"""
        bad = Batch("b1", "orders", [
            Record(1, {"id": 1, "name": "a"}, 1),
            Record(2, {"id": "WRONG", "name": "b"}, 1),
            Record(3, {"id": 3, "name": "c"}, 1),
        ], 1)
        r = self.pipeline.submit_batch(bad)
        self.assertEqual((r.accepted_count, r.checkpoint), (1, 1))

        fixed = Batch("b2", "orders", [
            Record(2, {"id": 2, "name": "b-fixed"}, 1),
            Record(3, {"id": 3, "name": "c"}, 1),
        ], 1)
        r2 = self.pipeline.submit_batch(fixed)
        self.assertEqual((r2.accepted_count, r2.checkpoint), (2, 3))
        self.assertEqual([row["id"] for row in self.pipeline.target.load("orders")],
                         [1, 2, 3])

    def test_offset_backward_rejected(self):
        """位点回退必须明确拒绝，目标端无写入。"""
        self.pipeline.submit_batch(Batch("b1", "orders", [
            Record(1, {"id": 1, "name": "a"}, 1),
            Record(2, {"id": 2, "name": "b"}, 1),
        ], 1))
        with self.assertRaises(BatchRejected) as ctx:
            self.pipeline.submit_batch(Batch("b2", "orders", [
                Record(1, {"id": 1, "name": "dup"}, 1),
            ], 1))
        self.assertEqual(ctx.exception.reason, RejectReason.OFFSET_BACKWARD)
        self.assertEqual(len(self.pipeline.target.load("orders")), 2)

    def test_offset_gap_rejected(self):
        """位点越界 / 跳号推进必须明确拒绝。"""
        with self.assertRaises(BatchRejected) as ctx:
            self.pipeline.submit_batch(Batch("b1", "orders", [
                Record(3, {"id": 3, "name": "jump"}, 1),
            ], 1))
        self.assertEqual(ctx.exception.reason, RejectReason.OFFSET_GAP)
        self.assertEqual(self.pipeline.checkpoints.get("orders"), 0)

        # 批次内不连续
        self.pipeline.submit_batch(Batch("ok", "orders", [
            Record(1, {"id": 1, "name": "a"}, 1),
        ], 1))
        with self.assertRaises(BatchRejected) as ctx:
            self.pipeline.submit_batch(Batch("gap", "orders", [
                Record(2, {"id": 2, "name": "a"}, 1),
                Record(4, {"id": 4, "name": "a"}, 1),
            ], 1))
        self.assertEqual(ctx.exception.reason, RejectReason.OFFSET_GAP)

    def test_restart_resumes_without_duplicate_or_loss(self):
        """流程中断后重建 Pipeline（重放持久化状态），断点续传不重不丢。"""
        for i in range(1, 3):
            self.source.produce("orders", {"id": i, "name": f"n{i}"}, 1)
        self.pipeline.run_once("orders", self.source, "run1", 10)

        restarted = Pipeline(self.root)  # 新实例，模拟进程重启
        self.assertEqual(restarted.checkpoints.get("orders"), 2)
        self.source.produce("orders", {"id": 3, "name": "n3"}, 1)
        r = restarted.run_once("orders", self.source, "run2", 10)
        self.assertEqual((r.accepted_count, r.checkpoint), (1, 3))
        self.assertEqual(
            [row["id"] for row in restarted.target.load("orders")], [1, 2, 3])


if __name__ == "__main__":
    import unittest
    unittest.main()
