"""跨表业务批次与历史更正/撤回测试：

- 跨表整体成功、中途失败精确定位且无半张表生效；
- 业务批次重复提交回放、PREPARED 中断后重启前滚到同一最终状态；
- 更正重复提交 / 中断重放结论一致、撤回后当前状态、更正历史可解释；
- 并发更正 / 正常增量 / 结构变更确定顺序；跨表更正撞结构变更仍整体一致。
"""

import tempfile
import threading
import unittest

from app.models import (BusinessBatch, BusinessOp, ChangeType, Column,
                        CorrectionBatch, CorrectionItem, EventKind, Record,
                        RejectReason, SchemaChange, TableBatchEntry)
from app.pipeline import BusinessBatchRejected, Pipeline
from tests._helpers import configure_logging  # noqa: F401

configure_logging()


def add_column(name, typ, required=True, default=None):
    return SchemaChange(0, ChangeType.ADD_COLUMN, name,
                        column=Column(name, typ, required=required,
                                      default=default))


class _SettlementCase(unittest.TestCase):
    """订单表 + 明细表的结算链路底座。"""

    def setUp(self) -> None:
        # pylint: disable=consider-using-with
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.pipeline = Pipeline(self.root)
        self.pipeline.init_table(
            "orders", [Column("id", "int"), Column("amount", "int")])
        self.pipeline.init_table(
            "details", [Column("id", "int"), Column("qty", "int")])

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _settle(self, txn_id, amount, qty, amount2=None, qty2=None):
        entries = [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": amount}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": qty}, 1)], 1),
        ]
        return self.pipeline.submit_business_batch(
            BusinessBatch(txn_id, entries))


class CrossTableBatchTest(_SettlementCase):
    def test_cross_table_batch_commits_all_or_nothing_on_success(self):
        r = self._settle("set-1", 100, 2)
        self.assertTrue(r.committed)
        self.assertFalse(r.replay)
        self.assertEqual(
            sorted(e.table for e in r.entries), ["details", "orders"])
        self.assertEqual(self.pipeline.checkpoints.get("orders"), 1)
        self.assertEqual(self.pipeline.checkpoints.get("details"), 1)
        self.assertEqual(self.pipeline.current_state("orders")[1]["amount"], 100)
        self.assertEqual(self.pipeline.current_state("details")[1]["qty"], 2)

    def test_mid_table_failure_aborts_all_tables_with_precise_location(self):
        # orders 合法、details 类型错误 -> 两张表都不得生效
        bad = BusinessBatch("set-bad", [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": 100}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": "BAD"}, 1)], 1),
        ])
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_business_batch(bad)
        failures = ctx.exception.failures
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].table, "details")
        self.assertEqual(failures[0].offset, 1)
        self.assertEqual(failures[0].reason, RejectReason.TYPE_MISMATCH)

        # 没有半张表：orders 也没有数据，位点不动
        self.assertEqual(self.pipeline.target.load("orders"), [])
        self.assertEqual(self.pipeline.target.load("details"), [])
        self.assertEqual(self.pipeline.checkpoints.get("orders"), 0)
        self.assertEqual(self.pipeline.checkpoints.get("details"), 0)

        # ABORTED 留痕可按业务批次定位逐表结论
        trace = self.pipeline.business_trace("set-bad")
        self.assertEqual(trace["state"], "ABORTED")
        by_table = {e["table"]: e for e in trace["entries"]}
        self.assertIsNone(by_table["orders"]["reason"])
        self.assertEqual(by_table["details"]["reason"], "TYPE_MISMATCH")

        # 修复后重试（新业务批次）整体成功
        ok = self._settle("set-fixed", 100, 2)
        self.assertTrue(ok.committed)
        self.assertEqual(self.pipeline.checkpoints.get("details"), 1)

    def test_duplicate_business_batch_replays_first_outcome(self):
        bb = BusinessBatch("set-1", [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": 100}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": 2}, 1)], 1),
        ])
        first = self.pipeline.submit_business_batch(bb)
        replay = self.pipeline.submit_business_batch(bb)
        self.assertTrue(replay.replay)
        self.assertEqual(replay.committed, first.committed)
        self.assertEqual(len(self.pipeline.target.load("orders")), 1)
        self.assertEqual(len(self.pipeline.target.load("details")), 1)

        # 失败批次的重复提交同样回放（不再重写任何东西）
        bad = BusinessBatch("set-bad", [
            TableBatchEntry("orders", [
                Record(2, {"id": 2, "amount": 1}, 1)], 1),
            TableBatchEntry("details", [
                Record(2, {"id": 2, "qty": "X"}, 1)], 1),
        ])
        with self.assertRaises(BusinessBatchRejected):
            self.pipeline.submit_business_batch(bad)
        with self.assertRaises(BusinessBatchRejected) as ctx2:
            self.pipeline.submit_business_batch(bad)
        self.assertEqual(ctx2.exception.failures[0].table, "details")
        # 回放路径没有新增任何行
        self.assertEqual(len(self.pipeline.target.load("orders")), 1)

    def test_prepared_interrupted_then_restart_rolls_forward(self):
        """PREPARED 已落盘、应用到一半崩溃：重启后幂等前滚到同一最终状态。"""
        self._settle("set-1", 100, 2)
        # 手工构造一个含两表、各两行的 PREPARED（模拟 commit 阶段崩溃前的留痕）
        plans = [
            {"table": "orders", "checkpoint_before": 1, "checkpoint_after": 3,
             "schema_version": 1,
             "rows": [[2, {"id": 2, "amount": 200}],
                      [3, {"id": 3, "amount": 300}]]},
            {"table": "details", "checkpoint_before": 1, "checkpoint_after": 3,
             "schema_version": 1,
             "rows": [[2, {"id": 2, "qty": 20}],
                      [3, {"id": 3, "qty": 30}]]},
        ]
        doc = {"txn_id": "set-crash", "kind": "BUSINESS",
               "state": "PREPARED",
               "entries": [{"table": "orders", "accepted_count": 2,
                            "checkpoint": 3, "rejected": [], "reason": None,
                            "detail": ""},
                           {"table": "details", "accepted_count": 2,
                            "checkpoint": 3, "rejected": [], "reason": None,
                            "detail": ""}],
               "plans": plans}
        self.pipeline.tx_log.append_state(doc)
        # 模拟“应用到一半”：只把 orders 的第一行落地，位点不推进
        seq = self.pipeline.state.allocate_write_seq("orders")
        self.pipeline.target.write_row(
            "orders", 2, {"id": 2, "amount": 200}, 1, seq=seq)

        restarted = Pipeline(self.root)  # 启动恢复
        self.assertEqual(restarted.business_trace("set-crash")["state"],
                         "COMMITTED")
        self.assertEqual(restarted.checkpoints.get("orders"), 3)
        self.assertEqual(restarted.checkpoints.get("details"), 3)
        self.assertEqual(
            [r["id"] for r in restarted.target.load("orders")], [1, 2, 3])
        self.assertEqual(
            [r["id"] for r in restarted.target.load("details")], [1, 2, 3])
        # 再次重启不重复
        again = Pipeline(self.root)
        self.assertEqual(
            [r["id"] for r in again.target.load("orders")], [1, 2, 3])
        self.assertEqual(
            [r["id"] for r in again.target.load("details")], [1, 2, 3])


class CorrectionAndWithdrawTest(_SettlementCase):
    def _ingest_one(self, txn_id="set-1", amount=100, qty=2):
        return self.pipeline.submit_business_batch(BusinessBatch(txn_id, [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": amount}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": qty}, 1)], 1),
        ]))

    def test_correction_updates_current_but_keeps_history(self):
        self._ingest_one()
        r = self.pipeline.submit_correction(CorrectionBatch("corr-1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 150})]))
        self.assertTrue(r.committed)
        self.assertEqual(self.pipeline.current_state("orders")[1]["amount"], 150)
        # 历史结论不被抹掉：先 100 后 150，可解释
        hist = self.pipeline.key_history("orders", 1)
        self.assertEqual([e["kind"] for e in hist], ["UPSERT", "UPSERT"])
        self.assertEqual([e["data"]["amount"] for e in hist], [100, 150])
        self.assertEqual(hist[-1]["correction_id"], "corr-1")

    def test_duplicate_correction_replays_without_flipping(self):
        self._ingest_one()
        corr = CorrectionBatch("corr-1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 150})])
        first = self.pipeline.submit_correction(corr)
        self.assertFalse(first.replay)
        replay = self.pipeline.submit_correction(corr)
        self.assertTrue(replay.replay)
        # 只有一条更正事件，没有第二次取号/写入 -> 结论不翻转
        upserts = [e for e in self.pipeline.key_history("orders", 1)
                   if e["correction_id"] == "corr-1"]
        self.assertEqual(len(upserts), 1)
        self.assertEqual(self.pipeline.current_state("orders")[1]["amount"], 150)

    def test_correction_interrupted_then_replayed_reaches_same_state(self):
        """更正执行到一半崩溃（PREPARED 已落盘）：重启前滚，重放结论一致。"""
        self._ingest_one()
        doc = {"txn_id": "corr-crash", "kind": "CORRECTION",
               "state": "PREPARED",
               "entries": [{"table": "orders", "accepted_count": 1,
                            "checkpoint": 0, "rejected": [], "reason": None,
                            "detail": "", "key": 1}],
               "plans": [{"type": "correction", "table": "orders", "key": 1,
                          "op": "UPSERT",
                          "data": {"id": 1, "amount": 777},
                          "schema_version": 1,
                          "correction_id": "corr-crash"}]}
        self.pipeline.tx_log.append_state(doc)
        seq_before_boot = self.pipeline.state.get_write_seq("orders")

        restarted = Pipeline(self.root)
        self.assertEqual(restarted.current_state("orders")[1]["amount"], 777)
        self.assertEqual(restarted.business_trace("corr-crash")["state"],
                         "COMMITTED")
        self.assertEqual(restarted.state.get_write_seq("orders"),
                         seq_before_boot + 1)
        # 再重启 + 显式重复提交，都不产生第二条事件
        again = Pipeline(self.root)
        replay = again.submit_correction(CorrectionBatch("corr-crash", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 777})]))
        self.assertTrue(replay.replay)
        self.assertEqual(again.state.get_write_seq("orders"),
                         seq_before_boot + 1)
        corr_events = [e for e in again.key_history("orders", 1)
                       if e["correction_id"] == "corr-crash"]
        self.assertEqual(len(corr_events), 1)

    def test_withdraw_hides_from_current_state_but_history_remains(self):
        self._ingest_one()
        r = self.pipeline.submit_correction(CorrectionBatch("wd-1", [
            CorrectionItem("details", 1, EventKind.WITHDRAW, 1)]))
        self.assertTrue(r.committed)
        self.assertEqual(self.pipeline.current_state("details"), {})
        hist = self.pipeline.key_history("details", 1)
        self.assertEqual([e["kind"] for e in hist], ["UPSERT", "WITHDRAW"])
        # 撤回后不能再更正（当前无有效结论）
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_correction(CorrectionBatch("corr-after", [
                CorrectionItem("details", 1, EventKind.UPSERT, 1,
                               {"id": 1, "qty": 9})]))
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.KEY_NOT_FOUND)

    def test_cross_table_correction_is_atomic(self):
        self._ingest_one()
        # 一条合法更正 + 一条撤回不存在的键 -> 整批不生效
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_correction(CorrectionBatch("mix", [
                CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                               {"id": 1, "amount": 500}),
                CorrectionItem("details", 99, EventKind.WITHDRAW, 1)]))
        self.assertEqual(ctx.exception.failures[0].table, "details")
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.KEY_NOT_FOUND)
        # orders 的更正不得半生效
        self.assertEqual(self.pipeline.current_state("orders")[1]["amount"], 100)
        self.assertEqual(self.pipeline.business_trace("mix")["state"],
                         "ABORTED")

    def test_withdraw_data_mismatch_rejected(self):
        self._ingest_one()
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_correction(CorrectionBatch("bad", [
                CorrectionItem("orders", 1, EventKind.WITHDRAW, 1,
                               {"id": 1, "amount": 1})]))
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.ENTRY_INVALID)


class ConcurrencyDeterminismTest(_SettlementCase):
    def test_concurrent_corrections_have_determined_order(self):
        """对同一键的并发更正：表锁串行化，当前值等于最后获胜者，历史全序可解释。"""
        self.pipeline.submit_business_batch(BusinessBatch("set-1", [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": 100}, 1)], 1)]))
        errors = []

        def correct(cid, amount):
            try:
                self.pipeline.submit_correction(CorrectionBatch(cid, [
                    CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                                   {"id": 1, "amount": amount})]))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=correct, args=(f"c{i}", i * 10))
                   for i in range(1, 21)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        # 20 条更正全部成功且各占一个全序序号；当前值是某一次更正的值（非初始 100），
        # 历史长度 21（1 次增量 + 20 次更正），无翻转空洞
        hist = self.pipeline.key_history("orders", 1)
        self.assertEqual(len(hist), 21)
        seqs = [e["seq"] for e in hist]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), len(seqs))
        current = self.pipeline.current_state("orders")[1]["amount"]
        self.assertIn(current, [i * 10 for i in range(1, 21)])
        self.assertEqual(hist[-1]["data"]["amount"], current)

    def test_correction_normal_ingest_and_ddl_serialize(self):
        """更正、正常增量提交、加列 DDL 并发：全部可线性化，状态与版本自洽。"""
        # 初始两单
        self.pipeline.submit_business_batch(BusinessBatch("set-1", [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": 100}, 1)], 1)]))
        errors = []
        barrier = threading.Barrier(3)

        def do_correction():
            barrier.wait()
            try:
                for i in range(1, 11):
                    self.pipeline.submit_correction(CorrectionBatch(f"c{i}", [
                        CorrectionItem("orders", 1, EventKind.UPSERT,
                                       self.pipeline.schemas.current_version(
                                           "orders"),
                                       {"id": 1, "amount": 100 + i})]))
            except Exception as exc:  # noqa: BLE001
                errors.append(("corr", exc))

        def do_ingest():
            barrier.wait()
            try:
                from app.models import Batch
                for i in range(2, 12):
                    # 用单表增量路径追 10 条；位点冲突时只有赢家写入
                    try:
                        self.pipeline.submit_batch(Batch(
                            f"ing-{i}", "orders",
                            [Record(i, {"id": i, "amount": i}, 1)], 1))
                    except Exception as exc:  # noqa: BLE001
                        errors.append(("ingest", exc))
            except Exception as exc:  # noqa: BLE001
                errors.append(("ingest-outer", exc))

        def do_ddl():
            barrier.wait()
            try:
                self.pipeline.alter_schema(
                    "orders", add_column("tag", "str", required=False))
            except Exception as exc:  # noqa: BLE001
                errors.append(("ddl", exc))

        ts = [threading.Thread(target=do_correction),
              threading.Thread(target=do_ingest),
              threading.Thread(target=do_ddl)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

        # 位点连续协议 + 表锁：ingest 从 offset 2 起串行成功 10 条；
        # 每个 batch_id 唯一，不会撞幂等键
        real_errors = [e for e in errors if not (
            isinstance(e[1], object) and getattr(e[1], "reason", None)
            in (RejectReason.OFFSET_BACKWARD, RejectReason.OFFSET_GAP))]
        self.assertEqual(real_errors, [], errors)
        snap = self.pipeline.snapshot("orders")
        state = self.pipeline.current_state("orders")
        self.assertEqual(snap["schema_version"], 2)
        # 10 条新增量各有其键；键 1 的当前值是最后一次更正（值 101..110 之一）
        self.assertEqual({k for k in state if k != 1}, set(range(2, 12)))
        self.assertIn(state[1]["amount"], list(range(101, 111)))
        # 无论更正落在 DDL 前后，当前状态折叠后键 1 一定带有加列后的结构对齐字段
        self.assertIn("tag", state[1])

    def test_correction_batch_interleaved_with_ddl_aligns_to_one_version(self):
        """跨表更正与加列 DDL 并发：整批更正要么在 DDL 前、要么在其后，
        所有分片按同一个结构快照解释，不会落到不同字段集合。"""
        self.pipeline.submit_business_batch(BusinessBatch("set-1", [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": 100}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": 2}, 1)], 1),
        ]))
        errors = []
        barrier = threading.Barrier(2)

        # 一批基于 v1 的跨表更正：只带 v1 字段
        correction = CorrectionBatch("corr-cross", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 222}),
            CorrectionItem("details", 1, EventKind.UPSERT, 1,
                           {"id": 1, "qty": 22}),
        ])

        def do_corr():
            barrier.wait()
            try:
                self.pipeline.submit_correction(correction)
            except Exception as exc:  # noqa: BLE001
                errors.append(("corr", exc))

        def do_ddl():
            barrier.wait()
            try:
                # 两张表同时加可空列
                self.pipeline.alter_schema(
                    "orders", add_column("tag", "str", required=False))
                self.pipeline.alter_schema(
                    "details", add_column("tag", "str", required=False))
            except Exception as exc:  # noqa: BLE001
                errors.append(("ddl", exc))

        t1 = threading.Thread(target=do_corr)
        t2 = threading.Thread(target=do_ddl)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(errors, [])

        orders = self.pipeline.current_state("orders")
        details = self.pipeline.current_state("details")
        trace = self.pipeline.business_trace("corr-cross")
        self.assertEqual(trace["state"], "COMMITTED")

        # 关键：两表 tag 字段的有无必须一致——同批更正按同一结构快照对齐。
        # 更正整体在 DDL 之前：两表都无 tag（之后的 DDL 不回改已存数据）；
        # 更正整体在 DDL 之后：两表都补齐 tag=None。
        orders_has_tag = "tag" in orders[1]
        details_has_tag = "tag" in details[1]
        self.assertEqual(
            orders_has_tag, details_has_tag,
            f"同批更正落到了不同字段集合：orders.tag={orders_has_tag}, "
            f"details.tag={details_has_tag}")
        self.assertEqual(orders[1]["amount"], 222)
        self.assertEqual(details[1]["qty"], 22)

    def test_mixed_legacy_and_correction_paths_share_one_total_order(self):
        """单表增量、更正交替写同一业务键：当前值按真实提交顺序折叠，不回翻。"""
        from app.models import Batch
        # 单表路径接入 key=1 amount=100
        self.pipeline.submit_batch(Batch("ing-1", "orders", [
            Record(1, {"id": 1, "amount": 100}, 1)], 1))
        # 更正为 200
        self.pipeline.submit_correction(CorrectionBatch("c1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 200})]))
        # 单表路径无法重写 offset=1（位点已推进）；改为验证折叠全序：
        # 再通过跨表业务批次接入 key=2 并更正、再增量，顺序始终单调
        self.pipeline.submit_business_batch(BusinessBatch("bb2", [
            TableBatchEntry("orders", [
                Record(2, {"id": 2, "amount": 10}, 1)], 1)]))
        self.pipeline.submit_correction(CorrectionBatch("c2", [
            CorrectionItem("orders", 2, EventKind.UPSERT, 1,
                           {"id": 2, "amount": 20})]))
        state = self.pipeline.current_state("orders")
        self.assertEqual(state[1]["amount"], 200)
        self.assertEqual(state[2]["amount"], 20)
        # 两个键的历史 seq 均严格递增，且全表只有一条单调序号轴
        for key in (1, 2):
            seqs = [e["seq"] for e in self.pipeline.key_history("orders", key)]
            self.assertEqual(seqs, sorted(seqs))
            self.assertTrue(all(s is not None for s in seqs))


if __name__ == "__main__":
    unittest.main()
