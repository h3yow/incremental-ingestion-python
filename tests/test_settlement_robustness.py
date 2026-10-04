"""结算链路健壮性回归（对应线上实测暴露的三类问题）：

业务一：业务键列结构变更（改名/删除）后，当前状态查询与更正/撤回定位必须一致
        —— 改名后已接入数据依然查得到、改得动；删除业务键列在 DDL 阶段被挡住。
业务二：跨表更正撞上多表结构变更 —— 多表原子 DDL 下，同一批更正要么整体落在
        变更前、要么整体落在变更后，重启恢复后结论不变。
业务三：更正/撤回乱序到达与重复重放 —— 最终结论按发起先后（issued_at）确定，
        更旧的请求不能翻掉更新的结论；重复提交/中断重放结论与首次完全一致。

每个用例运行时在日志打印 [SCENARIO] 业务 / 用例 / 结果，便于核对覆盖情况。
"""

import logging
import tempfile
import threading
import unittest

from app.models import (Batch, BusinessBatch, ChangeType, Column,
                        CorrectionBatch, CorrectionItem, EventKind, Record,
                        RejectReason, SchemaChange, TableBatchEntry)
from app.pipeline import BusinessBatchRejected, Pipeline
from app.schema_store import SchemaError
from tests._helpers import configure_logging  # noqa: F401

configure_logging()
logger = logging.getLogger("ingestion")


def add_column(name, typ="str", required=False, default=None):
    return SchemaChange(0, ChangeType.ADD_COLUMN, name,
                        column=Column(name, typ, required=required,
                                      default=default))


class _ScenarioCase(unittest.TestCase):
    """打印 业务/用例/结果 的用例基类。"""

    BUSINESS = "未命名业务"

    def run(self, result=None):
        logger.info("[SCENARIO] 业务=%s 用例=%s 开始",
                    self.BUSINESS, self._testMethodName)
        if result is None:
            return super().run(result)
        before = len(result.failures) + len(result.errors)
        outcome = super().run(result)
        ok = len(result.failures) + len(result.errors) == before
        logger.info("[SCENARIO] 业务=%s 用例=%s 结果=%s",
                    self.BUSINESS, self._testMethodName,
                    "通过" if ok else "失败")
        return outcome

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

    def _ingest(self, txn="set-1", amount=100, qty=2):
        return self.pipeline.submit_business_batch(BusinessBatch(txn, [
            TableBatchEntry("orders", [
                Record(1, {"id": 1, "amount": amount}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"id": 1, "qty": qty}, 1)], 1),
        ]))


class KeyColumnSchemaChangeTest(_ScenarioCase):
    """业务一：业务键列改名/删除对当前状态与更正/撤回的影响。"""

    BUSINESS = "业务键列结构变更"

    def test_rename_key_col_keeps_current_state_complete(self):
        """键列改名后：新数据接得进来也查得到，历史数据不丢。"""
        self._ingest()
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "id", to_name="order_id"))
        # 改名后按新结构接入第二条
        r = self.pipeline.submit_batch(Batch("b2", "orders", [
            Record(2, {"order_id": 2, "amount": 200}, 2)], 2))
        self.assertEqual(r.accepted_count, 1)
        state = self.pipeline.current_state("orders")
        # 改名前接入的键 1 与改名后接入的键 2 都必须可见
        self.assertEqual(sorted(state.keys()), [1, 2])
        self.assertEqual(state[1]["amount"], 100)
        self.assertEqual(state[2]["amount"], 200)
        # 历史可解释：键 1 的增量写入仍能被定位
        hist = self.pipeline.key_history("orders", 1)
        self.assertEqual(len(hist), 1)
        self.assertEqual(hist[0]["data"]["id"], 1)

    def test_correction_and_withdraw_after_key_col_rename(self):
        """键列改名后：按新名/旧名发起的更正都能定位历史结果，撤回也正常。"""
        self._ingest()
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "id", to_name="order_id"))
        # 按改名后的新结构（v2）更正
        self.pipeline.submit_correction(CorrectionBatch("c-new", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 2,
                           {"order_id": 1, "amount": 150})]))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 150)
        # 按改名前的旧结构（v1，数据里仍叫 id）更正：对齐后同样生效
        self.pipeline.submit_correction(CorrectionBatch("c-old", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 180})]))
        state = self.pipeline.current_state("orders")
        self.assertEqual(state[1]["amount"], 180)
        self.assertEqual(state[1]["order_id"], 1)
        # 撤回同样按键定位
        self.pipeline.submit_correction(CorrectionBatch("c-wd", [
            CorrectionItem("orders", 1, EventKind.WITHDRAW, 2)]))
        self.assertEqual(self.pipeline.current_state("orders"), {})
        kinds = [e["kind"] for e in self.pipeline.key_history("orders", 1)]
        self.assertEqual(kinds, ["UPSERT", "UPSERT", "UPSERT", "WITHDRAW"])

    def test_business_batch_append_after_key_col_rename(self):
        """跨表业务批次在键列改名后仍能整体提交且当前状态可见。"""
        self._ingest()
        self.pipeline.alter_schemas([
            ("orders", SchemaChange(0, ChangeType.RENAME_COLUMN, "id",
                                    to_name="order_id")),
            ("details", SchemaChange(0, ChangeType.RENAME_COLUMN, "id",
                                     to_name="line_id")),
        ])
        r = self.pipeline.submit_business_batch(BusinessBatch("set-2", [
            TableBatchEntry("orders", [
                Record(2, {"order_id": 2, "amount": 300}, 2)], 2),
            TableBatchEntry("details", [
                Record(2, {"line_id": 2, "qty": 9}, 2)], 2),
        ]))
        self.assertTrue(r.committed)
        self.assertEqual(sorted(self.pipeline.current_state("orders")), [1, 2])
        self.assertEqual(sorted(self.pipeline.current_state("details")), [1, 2])

    def test_drop_key_col_rejected_with_readable_reason(self):
        """删除业务键列：DDL 阶段直接拒绝，原因可读，结构版本不变。"""
        self._ingest()
        before = self.pipeline.schemas.current_version("orders")
        with self.assertRaises(SchemaError) as ctx:
            self.pipeline.alter_schema("orders", SchemaChange(
                0, ChangeType.DROP_COLUMN, "id"))
        self.assertIn("业务键列", str(ctx.exception))
        # 拒绝后无副作用：版本不变、当前状态完整、仍可更正
        self.assertEqual(self.pipeline.schemas.current_version("orders"),
                         before)
        self.assertEqual(sorted(self.pipeline.current_state("orders")), [1])
        self.pipeline.submit_correction(CorrectionBatch("c1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 150})]))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 150)

    def test_drop_renamed_key_col_also_rejected(self):
        """键列改名后，它的新名字依然是业务键：删除同样被拒绝。"""
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "id", to_name="order_id"))
        with self.assertRaises(SchemaError) as ctx:
            self.pipeline.alter_schema("orders", SchemaChange(
                0, ChangeType.DROP_COLUMN, "order_id"))
        self.assertIn("业务键列", str(ctx.exception))

    def test_drop_normal_column_still_allowed(self):
        """普通列（非业务键）删除不受影响：历史行不改写，新行按新结构对齐。"""
        self._ingest()
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.DROP_COLUMN, "amount"))
        self.assertEqual(self.pipeline.schemas.current_version("orders"), 2)
        # 历史行不改写：删除前接入的数据仍完整可见
        self.assertEqual(self.pipeline.current_state("orders")[1],
                         {"id": 1, "amount": 100})
        # 删除后接入的行按新结构对齐，不再携带被删列
        self.pipeline.submit_batch(Batch("b2", "orders", [
            Record(2, {"id": 2}, 2)], 2))
        self.assertEqual(self.pipeline.current_state("orders")[2], {"id": 2})


class CrossTableCorrectionVsDdlTest(_ScenarioCase):
    """业务二：跨表更正撞上多表结构变更的整体一致性。"""

    BUSINESS = "跨表更正vs多表结构变更"

    def test_correction_lands_entirely_before_or_after_atomic_ddl(self):
        """多表原子 DDL 与跨表更正并发 50 轮：每轮两表结构快照一致，
        且重启恢复后结论不变。"""
        for round_no in range(50):
            with tempfile.TemporaryDirectory() as root:
                p = Pipeline(root)
                p.init_table("orders",
                             [Column("id", "int"), Column("amount", "int")])
                p.init_table("details",
                             [Column("id", "int"), Column("qty", "int")])
                p.submit_business_batch(BusinessBatch("s1", [
                    TableBatchEntry("orders", [
                        Record(1, {"id": 1, "amount": 100}, 1)], 1),
                    TableBatchEntry("details", [
                        Record(1, {"id": 1, "qty": 2}, 1)], 1),
                ]))
                barrier = threading.Barrier(2)
                errors = []

                def corr():
                    barrier.wait()
                    try:
                        p.submit_correction(CorrectionBatch("c1", [
                            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                                           {"id": 1, "amount": 222}),
                            CorrectionItem("details", 1, EventKind.UPSERT, 1,
                                           {"id": 1, "qty": 22}),
                        ]))
                    except Exception as exc:  # noqa: BLE001
                        errors.append(exc)

                def ddl():
                    barrier.wait()
                    try:
                        p.alter_schemas([
                            ("orders", add_column("tag")),
                            ("details", add_column("tag")),
                        ])
                    except Exception as exc:  # noqa: BLE001
                        errors.append(exc)

                t1, t2 = threading.Thread(target=corr), threading.Thread(target=ddl)
                t1.start(); t2.start(); t1.join(); t2.join()
                self.assertEqual(errors, [])
                with self.subTest(round=round_no):
                    orders = p.current_state("orders")[1]
                    details = p.current_state("details")[1]
                    # 同一批更正：两表要么都带新字段、要么都不带
                    self.assertEqual("tag" in orders, "tag" in details)
                    self.assertEqual(orders["amount"], 222)
                    self.assertEqual(details["qty"], 22)
                    # 重启恢复后落到同一个结果
                    restarted = Pipeline(root)
                    self.assertEqual(restarted.current_state("orders")[1],
                                     orders)
                    self.assertEqual(restarted.current_state("details")[1],
                                     details)

    def test_atomic_multi_table_ddl_is_all_or_nothing(self):
        """多表原子 DDL：任一表预检失败则全部不生效，拒绝原因可读。"""
        before_o = self.pipeline.schemas.current_version("orders")
        before_d = self.pipeline.schemas.current_version("details")
        with self.assertRaises(SchemaError) as ctx:
            self.pipeline.alter_schemas([
                ("orders", add_column("tag")),
                # details 上删除业务键列 -> 整批必须失败
                ("details", SchemaChange(0, ChangeType.DROP_COLUMN, "id")),
            ])
        self.assertIn("业务键列", str(ctx.exception))
        # orders 的加列也不得生效
        self.assertEqual(self.pipeline.schemas.current_version("orders"),
                         before_o)
        self.assertEqual(self.pipeline.schemas.current_version("details"),
                         before_d)

    def test_recovery_applies_prepared_correction_regardless_of_later_ddl(self):
        """更正 PREPARED 落盘后崩溃、之后结构才变更：重启恢复仍按原快照应用，
        最终状态与「更正整体发生在 DDL 前」一致。"""
        self._ingest()
        doc = {"txn_id": "corr-crash", "kind": "CORRECTION",
               "state": "PREPARED",
               "entries": [{"table": "orders", "accepted_count": 1,
                            "checkpoint": 0, "rejected": [], "reason": None,
                            "detail": "", "key": 1}],
               "plans": [{"type": "correction", "table": "orders", "key": 1,
                          "op": "UPSERT",
                          "data": {"id": 1, "amount": 777},
                          "schema_version": 1,
                          "correction_id": "corr-crash",
                          "issued_at": None}]}
        self.pipeline.tx_log.append_state(doc)
        # 崩溃后、重启前，结构发生了变更（加列）
        self.pipeline.alter_schema("orders", add_column("tag"))
        # 重启：恢复按 PREPARED 文档（v1 对齐的数据）前滚，不受后续 DDL 影响
        restarted = Pipeline(self.root)
        state = restarted.current_state("orders")[1]
        self.assertEqual(state, {"id": 1, "amount": 777})
        self.assertNotIn("tag", state)
        self.assertEqual(restarted.business_trace("corr-crash")["state"],
                         "COMMITTED")
        # 再次重启结论不变
        again = Pipeline(self.root)
        self.assertEqual(again.current_state("orders")[1], state)


class CorrectionOrderingTest(_ScenarioCase):
    """业务三：更正/撤回乱序到达与重复重放。"""

    BUSINESS = "更正撤回乱序与重放"

    def test_stale_correction_does_not_override_newer(self):
        """后送达但更早发起的更正：留痕但不翻掉已生效的更新结论。"""
        self._ingest()
        self.pipeline.submit_correction(CorrectionBatch("corr-new", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 300})], issued_at=2))
        self.pipeline.submit_correction(CorrectionBatch("corr-old", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 200})], issued_at=1))
        # 最终结论按发起先后：issued_at=2 的 300 生效
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 300)
        # 旧请求仍完整留痕（可审计），且带有各自的发起时间
        hist = self.pipeline.key_history("orders", 1)
        self.assertEqual([e["data"]["amount"] for e in hist],
                         [100, 300, 200])
        self.assertEqual([e["issued_at"] for e in hist], [None, 2, 1])

    def test_stale_withdraw_does_not_hide_newer_upsert(self):
        """更早发起的撤回后送达：不能把更新发起的更正结论撤回掉。"""
        self._ingest()
        self.pipeline.submit_correction(CorrectionBatch("corr-up", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 300})], issued_at=2))
        self.pipeline.submit_correction(CorrectionBatch("wd-stale", [
            CorrectionItem("orders", 1, EventKind.WITHDRAW, 1)], issued_at=1))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 300)

    def test_newer_withdraw_wins_even_if_older_upsert_arrives_later(self):
        """更新发起的撤回先生效后，更旧发起的更正再送达也救不回该键。"""
        self._ingest()
        self.pipeline.submit_correction(CorrectionBatch("wd-new", [
            CorrectionItem("orders", 1, EventKind.WITHDRAW, 1)], issued_at=2))
        self.assertEqual(self.pipeline.current_state("orders"), {})
        # 更旧发起的更正后送达：当前无有效结论，按 KEY_NOT_FOUND 拒绝，
        # 无论如何不会把已生效的撤回翻回去
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.pipeline.submit_correction(CorrectionBatch("corr-old", [
                CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                               {"id": 1, "amount": 200})], issued_at=1))
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.KEY_NOT_FOUND)
        self.assertEqual(self.pipeline.current_state("orders"), {})

    def test_duplicate_and_replay_keep_first_conclusion(self):
        """同一请求重复提交、重启重放：最终状态与第一次完全一致。"""
        self._ingest()
        corr = CorrectionBatch("corr-1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 300})], issued_at=2)
        first = self.pipeline.submit_correction(corr)
        self.assertFalse(first.replay)
        # 重复提交：回放首次结论，不再写事件
        replay = self.pipeline.submit_correction(corr)
        self.assertTrue(replay.replay)
        # 乱序的旧请求夹在重复提交之间也不翻转
        self.pipeline.submit_correction(CorrectionBatch("corr-0", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 111})], issued_at=1))
        again = self.pipeline.submit_correction(corr)
        self.assertTrue(again.replay)
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 300)
        # 重启后结论一致；事件只有一条
        restarted = Pipeline(self.root)
        self.assertEqual(
            restarted.current_state("orders")[1]["amount"], 300)
        events = [e for e in restarted.key_history("orders", 1)
                  if e["correction_id"] == "corr-1"]
        self.assertEqual(len(events), 1)

    def test_crash_mid_correction_recovers_to_same_state(self):
        """更正执行到一半中断（PREPARED 已落盘）：重启前滚，结论与首次一致。"""
        self._ingest()
        doc = {"txn_id": "corr-crash", "kind": "CORRECTION",
               "state": "PREPARED",
               "entries": [{"table": "orders", "accepted_count": 1,
                            "checkpoint": 0, "rejected": [], "reason": None,
                            "detail": "", "key": 1}],
               "plans": [{"type": "correction", "table": "orders", "key": 1,
                          "op": "UPSERT",
                          "data": {"id": 1, "amount": 555},
                          "schema_version": 1,
                          "correction_id": "corr-crash",
                          "issued_at": 7}]}
        self.pipeline.tx_log.append_state(doc)
        seq_before = self.pipeline.state.get_write_seq("orders")
        restarted = Pipeline(self.root)
        self.assertEqual(
            restarted.current_state("orders")[1]["amount"], 555)
        self.assertEqual(restarted.state.get_write_seq("orders"),
                         seq_before + 1)
        # 恢复后再乱序送达更旧的请求、再重启，结论都不翻转
        restarted.submit_correction(CorrectionBatch("corr-stale", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 111})], issued_at=3))
        again = Pipeline(self.root)
        self.assertEqual(again.current_state("orders")[1]["amount"], 555)
        self.assertEqual(again.state.get_write_seq("orders"), seq_before + 2)

    def test_corrections_without_issued_at_keep_arrival_order(self):
        """不携带发起时间的旧式请求：保持按到达顺序生效（兼容旧行为）。"""
        self._ingest()
        self.pipeline.submit_correction(CorrectionBatch("c1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 200})]))
        self.pipeline.submit_correction(CorrectionBatch("c2", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 1,
                           {"id": 1, "amount": 300})]))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["amount"], 300)


if __name__ == "__main__":
    unittest.main()
