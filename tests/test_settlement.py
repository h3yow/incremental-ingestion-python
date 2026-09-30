"""跨表业务批次测试（结算链路）：
整体成功、中途失败全表不生效且可定位（业务批次/表/键/数据类别）、
失败修复后重试不产生半张表数据。
"""

from app.models import (Batch, BusinessBatch, BusinessOp, Column, OpType,
                        Record, RejectReason)
from app.pipeline import BusinessBatchRejected
from tests._helpers import IngestionCase


def op(table, kind, key, revision, data=None, ver=1):
    return BusinessOp(table, OpType(kind) if isinstance(kind, str) else kind,
                      tuple(key), revision, data, ver)


class CrossTableCase(IngestionCase):
    def setUp(self):
        super().setUp()
        # orders 是单表增量老路径；新增两张带业务键的结算表
        self.order_cols = [Column("order_id", "int"), Column("amount", "int")]
        self.item_cols = [Column("order_id", "int"), Column("sku", "str"),
                          Column("qty", "int")]
        self.pipeline.init_table("settle_orders", self.order_cols,
                                 key_columns=("order_id",))
        self.pipeline.init_table("settle_items", self.item_cols,
                                 key_columns=("order_id", "sku"))

    def _settle_batch(self, bid, amount=100, qty=2, ver=1):
        return BusinessBatch(bid, [
            op("settle_orders", "UPSERT", (1,), 1,
               {"order_id": 1, "amount": amount}, ver),
            op("settle_items", "UPSERT", (1, "A"), 1,
               {"order_id": 1, "sku": "A", "qty": qty}, ver),
            op("settle_items", "UPSERT", (1, "B"), 1,
               {"order_id": 1, "sku": "B", "qty": 1}, ver),
        ], ver)

    def test_cross_table_batch_commits_atomically(self):
        """多表批次整体成功：所有表同时生效，返回每张表的结果。"""
        r = self.pipeline.submit_business_batch(self._settle_batch("s1"))
        self.assertTrue(r.committed)
        self.assertEqual(r.sequence, 1)
        self.assertFalse(r.replay)
        by_table = {t.table: t for t in r.tables}
        self.assertEqual(by_table["settle_orders"].ops, 1)
        self.assertEqual(by_table["settle_items"].ops, 2)

        self.assertEqual(
            self.pipeline.current_business_rows("settle_orders"),
            [{"order_id": 1, "amount": 100}])
        self.assertEqual(
            self.pipeline.current_business_rows("settle_items"),
            [{"order_id": 1, "sku": "A", "qty": 2},
             {"order_id": 1, "sku": "B", "qty": 1}])
        # journal 只追加一条
        commits = self.pipeline.ledger.read_commits()
        self.assertEqual(len(commits), 1)
        self.assertEqual({t["table"] for t in commits[0]["tables"]},
                         {"settle_orders", "settle_items"})

    def test_mid_table_failure_applies_nothing_and_is_locatable(self):
        """明细表数据非法 -> 整个业务批次不生效，任何表都没有半张表数据。"""
        bad = BusinessBatch("s-bad", [
            op("settle_orders", "UPSERT", (2,), 1,
               {"order_id": 2, "amount": 50}),
            op("settle_items", "UPSERT", (2, "A"), 1,
               {"order_id": 2, "sku": "A", "qty": "WRONG"}),  # 类型错误
        ], 1)
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_business_batch(bad)
        exc = ctx.exception
        # 精确定位：哪个业务批次、哪张表、哪条业务键、哪类数据
        self.assertEqual(exc.batch_id, "s-bad")
        self.assertEqual(exc.table, "settle_items")
        self.assertEqual(exc.key, (2, "A"))
        self.assertEqual(exc.reason, RejectReason.TYPE_MISMATCH)

        # 订单表也不允许先写进去：没有任何表生效
        self.assertEqual(self.pipeline.current_business_rows("settle_orders"), [])
        self.assertEqual(self.pipeline.current_business_rows("settle_items"), [])
        self.assertIsNone(self.pipeline.business_trace("s-bad"),
                          "失败批次不得占用提交顺序号 / 不留提交痕迹")

    def test_retry_after_fix_has_no_half_table(self):
        """失败后修正数据，用同一业务批次重试 -> 完整生效，且不产生半成品。"""
        bad = BusinessBatch("s1", [
            op("settle_orders", "UPSERT", (1,), 1,
               {"order_id": 1, "amount": 100}),
            op("settle_items", "UPSERT", (1, "A"), 1,
               {"order_id": 1, "sku": "A", "qty": "BAD"}),
        ], 1)
        with self.assertRaises(BusinessBatchRejected):
            self.pipeline.submit_business_batch(bad)

        good = self._settle_batch("s1")
        r = self.pipeline.submit_business_batch(good)
        self.assertTrue(r.committed)
        self.assertFalse(r.replay, "此前未提交成功，重试不是回放而是首次生效")
        self.assertEqual(r.sequence, 1, "失败尝试不占用顺序号")
        self.assertEqual(len(self.pipeline.current_business_rows("settle_items")), 2)
        self.assertEqual(len(self.pipeline.current_business_rows("settle_orders")), 1)

    def test_validation_errors_are_distinguishable(self):
        """各类业务批次前置错误原因可区分，且全部无副作用。"""
        cases = [
            # 未声明业务键的表（老的单表增量表 orders）
            (BusinessBatch("k1", [op("orders", "UPSERT", (1,), 1,
                                     {"id": 1, "name": "x"})], 1),
             RejectReason.TABLE_NOT_KEYED),
            # 键个数不符
            (BusinessBatch("k2", [op("settle_orders", "UPSERT", (1, 9), 1,
                                     {"order_id": 1, "amount": 1})], 1),
             RejectReason.KEY_MISMATCH),
            # 键类型不符
            (BusinessBatch("k3", [op("settle_orders", "UPSERT", ("1",), 1,
                                     {"order_id": 1, "amount": 1})], 1),
             RejectReason.KEY_MISMATCH),
            # 撤回携带数据
            (BusinessBatch("k4", [op("settle_orders", "RETRACT", (1,), 1,
                                     {"order_id": 1})], 1),
             RejectReason.INVALID_OPERATION),
            # 批内同表同键重复
            (BusinessBatch("k5", [
                op("settle_orders", "UPSERT", (1,), 1,
                   {"order_id": 1, "amount": 1}),
                op("settle_orders", "UPSERT", (1,), 2,
                   {"order_id": 1, "amount": 2}),
            ], 1), RejectReason.DUPLICATE_KEY_IN_BATCH),
        ]
        for batch, reason in cases:
            with self.subTest(batch=batch.batch_id):
                with self.assertRaises(BusinessBatchRejected) as ctx:
                    self.pipeline.submit_business_batch(batch)
                self.assertEqual(ctx.exception.reason, reason)
                self.assertEqual(
                    self.pipeline.ledger.read_commits(), [],
                    f"{batch.batch_id} 被拒后不得有任何提交")

    def test_single_table_ingestion_still_works_alongside(self):
        """老的单表增量路径与结算表共存且互不影响。"""
        self.pipeline.submit_batch(Batch("old-1", "orders", [
            Record(1, {"id": 1, "name": "n1"}, 1),
        ], 1))
        self.pipeline.submit_business_batch(self._settle_batch("s1"))
        self.assertEqual(len(self.pipeline.target.load("orders")), 1)
        self.assertEqual(self.pipeline.checkpoints.get("orders"), 1)
        self.assertEqual(len(self.pipeline.current_business_rows("settle_orders")), 1)


if __name__ == "__main__":
    import unittest
    unittest.main()
