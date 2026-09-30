"""并发确定性顺序测试：
更正互相覆盖、撤回与更正竞争、业务批次撞上正常增量与结构变化、
跨表批次与 DDL 竞争 —— 结论必须有确定顺序且重启后收敛到同一最终状态。
"""

import threading

from app.models import (Batch, BusinessBatch, BusinessOp, ChangeType, Column,
                        OpType, Record, RejectReason, SchemaChange)
from app.pipeline import BusinessBatchRejected, Pipeline
from tests._helpers import IngestionCase


def up(table, key, rev, data, ver=1):
    return BusinessOp(table, OpType.UPSERT, tuple(key), rev, data, ver)


def retract(table, key, rev, ver=1):
    return BusinessOp(table, OpType.RETRACT, tuple(key), rev, None, ver)


def batch(bid, ops, ver=1):
    return BusinessBatch(bid, ops, ver)


class BusinessConcurrencyCase(IngestionCase):
    def setUp(self):
        super().setUp()
        self.order_cols = [Column("order_id", "int"), Column("amount", "int")]
        self.item_cols = [Column("order_id", "int"), Column("sku", "str"),
                          Column("qty", "int")]
        self.pipeline.init_table("so", self.order_cols,
                                 key_columns=("order_id",))
        self.pipeline.init_table("si", self.item_cols,
                                 key_columns=("order_id", "sku"))

    def test_concurrent_corrections_resolve_by_revision(self):
        """同一业务键并发更正：无论线程调度如何，revision 最大者胜出，低版本翻案被拒。"""
        self.pipeline.submit_business_batch(
            batch("base", [up("so", (1,), 1, {"order_id": 1, "amount": 1})]))
        errors = []
        committed: list[int] = []
        lock = threading.Lock()

        def submit(rev):
            try:
                self.pipeline.submit_business_batch(
                    batch(f"r{rev}", [up("so", (1,), rev,
                                         {"order_id": 1, "amount": rev})]))
                with lock:
                    committed.append(rev)
            except BusinessBatchRejected as exc:
                with lock:
                    errors.append(exc)

        # 故意把 revision 顺序打乱：最大 revision 一定生效，晚到的低版本被拒
        threads = [threading.Thread(target=submit, args=(rev,))
                   for rev in (5, 2, 4, 3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 先提交的低版本会被后到的高版本覆盖；任何“高版本已生效后低版本才到”都被拒。
        # 最终生效的是提交顺序中的严格递增链，末尾必为 5；被拒数随调度变化，但
        # 所有错误都只能是 STALE_REVISION，且最终结论确定。
        self.assertTrue(all(e.reason == RejectReason.STALE_REVISION
                            for e in errors))
        self.assertEqual(len(committed) + len(errors), 4)
        cur = self.pipeline.current_conclusion("so", (1,))
        self.assertEqual(cur.revision, 5)
        self.assertEqual(cur.data["amount"], 5)
        # 历史恰为实际生效的严格递增链
        hist = self.pipeline.business_history("so", (1,))
        revs = [h["revision"] for h in hist]
        self.assertEqual(revs[0], 1)
        self.assertEqual(revs[-1], 5)
        self.assertEqual(revs, sorted(revs))

    def test_concurrent_correction_and_retract_are_linearizable(self):
        """并发撤回/更正：最终状态由 revision 顺序唯一决定，撤回后不会冒回来。"""
        self.pipeline.submit_business_batch(
            batch("base", [up("so", (9,), 1, {"order_id": 9, "amount": 1})]))
        errors = []

        def do(bid, op):
            try:
                self.pipeline.submit_business_batch(batch(bid, [op]))
            except BusinessBatchRejected as exc:
                errors.append(exc)

        ops = [
            ("retract-r5", retract("so", (9,), 5)),
            ("fix-r3", up("so", (9,), 3, {"order_id": 9, "amount": 3})),
            ("fix-r4", up("so", (9,), 4, {"order_id": 9, "amount": 4})),
            ("fix-r6", up("so", (9,), 6, {"order_id": 9, "amount": 6})),
        ]
        threads = [threading.Thread(target=do, args=args) for args in ops]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # rev 3/4 过期（无论当时存活的是 rev1 还是 rev5）；rev5 撤回、rev6 更正生效
        self.assertEqual(sorted(e.reason for e in errors),
                         [RejectReason.STALE_REVISION,
                          RejectReason.STALE_REVISION])
        cur = self.pipeline.current_conclusion("so", (9,))
        self.assertEqual((cur.revision, cur.status, cur.data["amount"]),
                         (6, OpType.UPSERT, 6))
        self.assertEqual(len(self.pipeline.current_business_rows("so")), 1)

    def test_business_batch_and_normal_ingest_do_not_half_apply(self):
        """跨表业务批次与正常单表增量并发：各自串行，无跨表半生效、无重复写。"""
        errors = []

        def settle():
            try:
                for i in range(1, 51):
                    self.pipeline.submit_business_batch(batch(f"s{i}", [
                        up("so", (i,), 1, {"order_id": i, "amount": i}),
                        up("si", (i, "A"), 1,
                           {"order_id": i, "sku": "A", "qty": i}),
                    ]))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def ingest():
            try:
                for i in range(1, 51):
                    self.pipeline.submit_batch(Batch(
                        f"o{i}", "orders",
                        [Record(i, {"id": i, "name": f"n{i}"}, 1)], 1))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=settle)
        t2 = threading.Thread(target=ingest)
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.pipeline.current_business_rows("so")), 50)
        self.assertEqual(len(self.pipeline.current_business_rows("si")), 50)
        # 每个结算批次要么整批在（两张表各一条），要么整批不在 —— 数量恒等
        self.assertEqual(
            len(self.pipeline.current_business_rows("so")),
            len(self.pipeline.current_business_rows("si")))
        self.assertEqual(self.pipeline.checkpoints.get("orders"), 50)
        self.assertEqual(len(self.pipeline.target.load("orders")), 50)

    def test_business_batch_concurrent_with_schema_change_uses_one_schema(self):
        """业务批次撞上结构变化：线性化到 DDL 前或后，不会半批落在不同字段上。"""
        self.pipeline.submit_business_batch(
            batch("base", [up("so", (1,), 1, {"order_id": 1, "amount": 1})]))
        barrier = threading.Barrier(2)
        errors = []

        def rename():
            try:
                barrier.wait()
                self.pipeline.alter_schema("so", SchemaChange(
                    0, ChangeType.RENAME_COLUMN, "amount", to_name="total"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def correct():
            try:
                barrier.wait()
                # 更正使用旧字段名 amount、按版本 1 对齐；
                # 若先于 DDL 生效 -> 直接写入；若后于 DDL -> 通过改名链对齐为 total，
                # 无论哪种都只在一个结构版本上解释，绝不半批错位。
                self.pipeline.submit_business_batch(
                    batch("corr", [up("so", (1,), 2,
                                      {"order_id": 1, "amount": 42}, 1)]))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        tr = threading.Thread(target=rename)
        tc = threading.Thread(target=correct)
        tr.start(); tc.start()
        tr.join(); tc.join()
        self.assertEqual(errors, [])

        cur = self.pipeline.current_conclusion("so", (1,))
        self.assertEqual(cur.revision, 2)
        # 当前结构下值唯一承载在 total（或改名前的 amount），二者恰有其一
        live = self.pipeline.current_business_rows("so")[0]
        self.assertIn(42, live.values())
        self.assertEqual(live.get("total", live.get("amount")), 42)
        self.assertNotEqual(live.get("total"), live.get("amount"))

    def test_restart_after_concurrent_mix_converges(self):
        """混合并发后重启：从已确认提交边界（sequence）重放到同一最终状态。"""
        errors = []

        def worker(prefix, rev0):
            try:
                for i in range(20):
                    rev = rev0 + i
                    self.pipeline.submit_business_batch(
                        batch(f"{prefix}{rev}",
                              [up("so", (1,), rev,
                                  {"order_id": 1, "amount": rev})]))
            except BusinessBatchRejected:
                pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=("a", 1)),
                   threading.Thread(target=worker, args=("b", 5))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        before = {
            "max_rev": self.pipeline.current_conclusion("so", (1,)).revision,
            "seq": self.pipeline.ledger.max_sequence(),
            "rows": self.pipeline.current_business_rows("so"),
        }
        restarted = Pipeline(self.root)
        after = {
            "max_rev": restarted.current_conclusion("so", (1,)).revision,
            "seq": restarted.ledger.max_sequence(),
            "rows": restarted.current_business_rows("so"),
        }
        self.assertEqual(before, after)
        # 两个工作线程 revision 区间重叠 [1..24]，最终必为最大 revision 24
        self.assertEqual(before["max_rev"], 24)


if __name__ == "__main__":
    import unittest
    unittest.main()
