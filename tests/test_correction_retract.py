"""历史更正与撤回测试：
只看最后一次有效结论、历史轨迹保留可解释、重复提交幂等、
执行到一半中断后重放收敛到同一最终状态、重启后状态一致。
"""

from app.models import BusinessBatch, BusinessOp, Column, OpType as OT
from app.models import RejectReason
from app.pipeline import BusinessBatchRejected, Pipeline
from tests._helpers import IngestionCase


def bop(table, kind, key, revision, data=None, ver=1):
    return BusinessOp(table, OT(kind), tuple(key), revision, data, ver)


class CorrectionRetractCase(IngestionCase):
    def setUp(self):
        super().setUp()
        self.pipeline.init_table(
            "settle_orders",
            [Column("order_id", "int"), Column("amount", "int"),
             Column("note", "str", required=False)],
            key_columns=("order_id",))

    def _upsert(self, bid, order_id, rev, amount, note=None, ver=1):
        data = {"order_id": order_id, "amount": amount}
        if note is not None:
            data["note"] = note
        return BusinessBatch(bid, [
            bop("settle_orders", "UPSERT", (order_id,), rev, data, ver)], ver)

    def test_current_state_keeps_last_valid_conclusion(self):
        """多次更正：当前状态只看最后一次有效结论。"""
        self.pipeline.submit_business_batch(self._upsert("b1", 1, 1, 100))
        self.pipeline.submit_business_batch(self._upsert("b2", 1, 2, 110))
        self.pipeline.submit_business_batch(self._upsert("b3", 1, 3, 120))
        rows = self.pipeline.current_business_rows("settle_orders")
        self.assertEqual(rows, [{"order_id": 1, "amount": 120, "note": None}])
        cur = self.pipeline.current_conclusion("settle_orders", (1,))
        self.assertEqual(cur.revision, 3)
        self.assertEqual(cur.batch_id, "b3")

    def test_history_is_preserved_and_explainable(self):
        """处理痕迹不抹除：可解释一条结果经历过哪些更正/撤回。"""
        self.pipeline.submit_business_batch(self._upsert("b1", 1, 1, 100))
        self.pipeline.submit_business_batch(self._upsert("b2", 1, 2, 110))
        self.pipeline.submit_business_batch(BusinessBatch("b3", [
            bop("settle_orders", "RETRACT", (1,), 3)], 1))
        hist = self.pipeline.business_history("settle_orders", (1,))
        self.assertEqual([h["revision"] for h in hist], [1, 2, 3])
        self.assertEqual([h["status"] for h in hist],
                         ["UPSERT", "UPSERT", "RETRACT"])
        self.assertEqual([h["batch_id"] for h in hist], ["b1", "b2", "b3"])
        self.assertEqual(hist[0]["data"]["amount"], 100)
        self.assertIsNone(hist[2]["data"])

    def test_retract_hides_from_current_but_keeps_trace(self):
        """撤回后：当前状态查不到该行，但仍能查到最后结论为撤回、轨迹完整。"""
        self.pipeline.submit_business_batch(self._upsert("b1", 7, 1, 100))
        self.pipeline.submit_business_batch(BusinessBatch("r1", [
            bop("settle_orders", "RETRACT", (7,), 2)], 1))
        self.assertEqual(self.pipeline.current_business_rows("settle_orders"), [])
        cur = self.pipeline.current_conclusion("settle_orders", (7,))
        self.assertIsNotNone(cur)
        self.assertEqual(cur.status, OT.RETRACT)
        self.assertEqual(len(self.pipeline.business_history("settle_orders", (7,))), 2)

        # 撤回后允许以更高 revision 重新给出结论（新结论，不是复活旧值）
        self.pipeline.submit_business_batch(self._upsert("b2", 7, 5, 200))
        self.assertEqual(
            self.pipeline.current_business_rows("settle_orders"),
            [{"order_id": 7, "amount": 200, "note": None}])

    def test_duplicate_correction_submit_is_idempotent(self):
        """同一更正请求重复提交：最终状态一致，不产生重复结论。"""
        batch = self._upsert("b1", 1, 1, 100)
        r1 = self.pipeline.submit_business_batch(batch)
        r2 = self.pipeline.submit_business_batch(batch)
        r3 = self.pipeline.submit_business_batch(batch)
        self.assertFalse(r1.replay)
        self.assertTrue(r2.replay and r3.replay)
        self.assertEqual(r2.sequence, r1.sequence)
        self.assertEqual(len(self.pipeline.ledger.read_commits()), 1)
        self.assertEqual(len(self.pipeline.current_business_rows("settle_orders")), 1)
        self.assertEqual(len(self.pipeline.business_history("settle_orders", (1,))), 1)

    def test_stale_revision_is_rejected_before_and_after_retract(self):
        """旧 revision 不能翻案：旧值不覆盖新值；撤回后旧更正也不会冒出来。"""
        self.pipeline.submit_business_batch(self._upsert("new", 1, 3, 100))
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_business_batch(self._upsert("old", 1, 2, 1))
        self.assertEqual(ctx.exception.reason, RejectReason.STALE_REVISION)
        # 值不变
        self.assertEqual(
            self.pipeline.current_business_rows("settle_orders")[0]["amount"], 100)

        self.pipeline.submit_business_batch(BusinessBatch("retract", [
            bop("settle_orders", "RETRACT", (1,), 4)], 1))
        # 新批次但携带旧 revision=3 的更正 -> 拒绝，撤回结论不被复活
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_business_batch(self._upsert("zombie", 1, 3, 999))
        self.assertEqual(ctx.exception.reason, RejectReason.STALE_REVISION)
        self.assertEqual(self.pipeline.current_business_rows("settle_orders"), [])

        # 而原更正请求（同一 batch_id）重复提交只是幂等回放，最终状态仍是已撤回
        replay = self.pipeline.submit_business_batch(self._upsert("new", 1, 3, 100))
        self.assertTrue(replay.replay)
        self.assertEqual(self.pipeline.current_business_rows("settle_orders"), [])

    def test_crash_before_commit_then_replay_converges(self):
        """执行到一半中断（提交点之前崩溃）后重放：最终状态与一次成功完全相同。"""
        batch = self._upsert("b1", 1, 1, 100)
        self.pipeline._crash_before_business_commit = True
        with self.assertRaises(RuntimeError):
            self.pipeline.submit_business_batch(batch)
        # 崩溃后没有任何结论
        self.assertEqual(self.pipeline.current_business_rows("settle_orders"), [])
        self.assertEqual(self.pipeline.ledger.read_commits(), [])

        # 用同一幂等键重放 -> 正常提交
        r = self.pipeline.submit_business_batch(batch)
        self.assertTrue(r.committed)
        self.assertFalse(r.replay)
        self.assertEqual(r.sequence, 1)
        self.assertEqual(
            self.pipeline.current_business_rows("settle_orders"),
            [{"order_id": 1, "amount": 100, "note": None}])

    def test_interrupted_torn_tail_line_is_discarded_and_replay_converges(self):
        """journal 末行写入不完整（追加中途断电）：截断丢弃后重放，状态一致。"""
        self.pipeline.submit_business_batch(self._upsert("b1", 1, 1, 100))
        path = self.pipeline.ledger.path
        with path.open("a", encoding="utf-8") as f:
            f.write('{"batch_id": "torn", "sequence": 2, "tab')  # 无换行、不完整
        # 读取时坏尾行被截断；已提交的 b1 仍在
        commits = self.pipeline.ledger.read_commits()
        self.assertEqual([c["batch_id"] for c in commits], ["b1"])
        with path.open("r", encoding="utf-8") as f:
            self.assertNotIn("torn", f.read())
        # 重放 torn 批次（视为从未提交）-> 成功且顺序号为 2
        r = self.pipeline.submit_business_batch(self._upsert("torn", 2, 1, 50))
        self.assertEqual(r.sequence, 2)
        self.assertFalse(r.replay)
        self.assertEqual(len(self.pipeline.ledger.read_commits()), 2)

    def test_restart_rebuilds_same_final_state(self):
        """进程重启重建 Pipeline：从提交日志重放出同一最终状态与历史。"""
        self.pipeline.submit_business_batch(self._upsert("b1", 1, 1, 100))
        self.pipeline.submit_business_batch(self._upsert("b2", 1, 2, 110))
        self.pipeline.submit_business_batch(BusinessBatch("r1", [
            bop("settle_orders", "RETRACT", (1,), 3)], 1))

        restarted = Pipeline(self.root)
        self.assertEqual(restarted.ledger.max_sequence(), 3)
        self.assertEqual(restarted.current_business_rows("settle_orders"), [])
        cur = restarted.current_conclusion("settle_orders", (1,))
        self.assertEqual(cur.status, OT.RETRACT)
        self.assertEqual(len(restarted.business_history("settle_orders", (1,))), 3)
        # 重启后幂等回放仍成立
        replay = restarted.submit_business_batch(self._upsert("b2", 1, 2, 110))
        self.assertTrue(replay.replay)
        self.assertEqual(replay.sequence, 2)


if __name__ == "__main__":
    import unittest
    unittest.main()
