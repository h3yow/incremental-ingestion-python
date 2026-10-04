"""结算链路缺陷修复回归测试（bugfix-b）。

按业务域分组，覆盖三类实测缺陷及其修复语义：

A. 业务键列结构变更（改名 / 删除）后当前状态与更正/撤回定位
   - 改名：当前状态完整、新数据可查、历史结果仍可更正/撤回、重启保持；
   - 删除：DDL 阶段以 KEY_COLUMN_PROTECTED 明确拒绝、无副作用、可继续接入。

B. 跨表更正与多表结构变更并发的整体一致性（DDL 战役）
   - 确定性交错 / 高并发：同批更正整体落在变更前或后的同一结构快照；
   - 战役内任一表失败则全部表不变；战役幂等回放；战役崩溃恢复；
   - 旧的「逐表单次 DDL」若把跨表事务夹在中间，会被新协议挡在整体边界外
     （跨表事务在 ddl_lock 上排队，逐表 DDL 之间无法被插入——由战役保证，
     单表 DDL 只保证与跨表事务不交错，不保证多次单表 DDL 的原子性）。

C. 更正/撤回发起先后裁决（乱序 / 重复 / 重放）
   - 更旧发起序号的请求不得翻掉更新结论（含撤回后旧更正晚到）；
   - 同一条请求重复提交 / PREPARED 中断重放，结论与首次完全一致；
   - 不提供发起序号时退化为原有「按到达先后」语义（兼容）。
"""

import tempfile
import threading
import unittest

from app.models import (Batch, BusinessBatch, ChangeType, Column,
                        CorrectionBatch, CorrectionItem, EventKind, Record,
                        RejectReason, SchemaChange, TableBatchEntry)
from app.pipeline import BusinessBatchRejected, Pipeline
from app.schema_store import KeyColumnProtectedError
from tests._helpers import configure_logging  # noqa: F401

configure_logging()


def add_nullable_col(name, typ="str"):
    return SchemaChange(0, ChangeType.ADD_COLUMN, name,
                        column=Column(name, typ, required=False))


class _Settlement(unittest.TestCase):
    """订单表 + 明细表结算链路底座（业务键列均为 settle_id）。"""

    def setUp(self) -> None:
        # pylint: disable=consider-using-with
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.p = Pipeline(self.root)
        self.p.init_table(
            "orders", [Column("settle_id", "str"), Column("amount", "int")],
            key_col="settle_id")
        self.p.init_table(
            "details", [Column("settle_id", "str"), Column("qty", "int")],
            key_col="settle_id")
        self.p.submit_business_batch(BusinessBatch("S1", [
            TableBatchEntry("orders", [
                Record(1, {"settle_id": "S1", "amount": 100}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"settle_id": "S1", "qty": 2}, 1)], 1),
        ]))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def corr(self, cid, table, value, seq=None,
             op=EventKind.UPSERT, key="S1", version=None, key_col=None):
        """提交一条更正/撤回。数据按「声明版本当时的键列名」承载业务键。"""
        ver = version or self.p.schemas.current_version(table)
        kc = key_col or self.p.schemas.key_col_at(table, ver)
        col = "amount" if table == "orders" else "qty"
        data = None if op == EventKind.WITHDRAW else {kc: key, col: value}
        return self.p.submit_correction(CorrectionBatch(cid, [
            CorrectionItem(table, key, op, ver, data=data, issued_seq=seq)]))

    def correction_event_keys(self, cid, table):
        """读取某条已落盘更正事件的数据字段名集合（判定物理落盘对齐到哪个版本）。"""
        for r in self.p.state.rows.read_all():
            if r.get("kind") and r.get("correction_id") == cid \
                    and r.get("table") == table:
                return set(r["data"]) if r.get("data") is not None else set()
        return None


# ========================= A. 业务键列结构变更 =========================

class KeyColumnRenameStateAndCorrectionTest(_Settlement):
    """A1：业务键列改名后当前状态完整、可查可改可撤回。"""

    def test_state_complete_after_key_column_rename_new_rows_visible(self):
        self.p.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "settle_id", to_name="biz_id"))
        # 改名后新接入的数据：成功 + 位点推进 + 当前状态可查（修复前会丢）
        r = self.p.submit_batch(Batch("b2", "orders", [
            Record(2, {"biz_id": "S2", "amount": 200}, 2)], 2))
        self.assertEqual(r.accepted_count, 1)
        state = self.p.current_state("orders")
        self.assertEqual(set(state), {"S1", "S2"})
        self.assertEqual(state["S2"]["amount"], 200)
        # 老版本行在当前状态里按当前结构呈现（旧列名映射到新列名）
        self.assertEqual(set(state["S1"]), {"biz_id", "amount"})
        self.assertEqual(state["S1"]["biz_id"], "S1")

    def test_history_result_correctable_and_withdrawable_after_rename(self):
        self.p.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "settle_id", to_name="biz_id"))
        # 用新列名对改名前接入的历史结果更正（修复前误判 ENTRY_INVALID）
        self.corr("fix-s1", "orders", 120, version=2)
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 120)
        # 仍可撤回，撤回后当前状态消失、历史保留
        self.corr("wd-s1", "orders", None, op=EventKind.WITHDRAW, version=2)
        self.assertNotIn("S1", self.p.current_state("orders"))
        kinds = [e["kind"] for e in self.p.key_history("orders", "S1")]
        self.assertEqual(kinds, ["UPSERT", "UPSERT", "WITHDRAW"])

    def test_rename_then_restart_state_and_correction_persist(self):
        self.p.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "settle_id", to_name="biz_id"))
        self.corr("fix-s1", "orders", 120, version=2)
        p2 = Pipeline(self.root)  # 重启：改名链与更正事件均持久化
        self.assertEqual(p2.current_state("orders")["S1"]["biz_id"], "S1")
        self.assertEqual(p2.current_state("orders")["S1"]["amount"], 120)

    def test_chained_rename_multiple_times_still_locates_history(self):
        self.p.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "settle_id", to_name="k1"))
        self.p.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "k1", to_name="k2"))
        self.corr("fix", "orders", 300, version=3)
        self.assertEqual(self.p.current_state("orders")["S1"]["k2"], "S1")
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 300)


class KeyColumnDropProtectedTest(_Settlement):
    """A2：删除业务键列在结构变更阶段被明确挡住。"""

    def test_drop_key_column_rejected_with_clear_reason(self):
        with self.assertRaises(KeyColumnProtectedError) as ctx:
            self.p.alter_schema("orders", SchemaChange(
                0, ChangeType.DROP_COLUMN, "settle_id"))
        self.assertEqual(ctx.exception.reason,
                         RejectReason.KEY_COLUMN_PROTECTED.value)

    def test_drop_rejected_leaves_no_side_effect_and_ingest_works(self):
        with self.assertRaises(KeyColumnProtectedError):
            self.p.alter_schema("orders", SchemaChange(
                0, ChangeType.DROP_COLUMN, "settle_id"))
        # 版本不推进、当前状态不变、可继续接入与更正
        self.assertEqual(self.p.schemas.current_version("orders"), 1)
        self.assertEqual(set(self.p.current_state("orders")), {"S1"})
        self.corr("fix", "orders", 9, version=1)
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 9)

    def test_drop_rejected_inside_campaign_aborts_all_tables(self):
        """DDL 战役中某表试图删业务键列：整个战役中止，所有表版本不变。"""
        with self.assertRaises(KeyColumnProtectedError):
            self.p.alter_schemas("bad-campaign", [
                ("orders", SchemaChange(
                    0, ChangeType.DROP_COLUMN, "settle_id")),
                ("details", add_nullable_col("tag")),
            ])
        self.assertEqual(self.p.schemas.current_version("orders"), 1)
        self.assertEqual(self.p.schemas.current_version("details"), 1)


# =================== B. 跨表更正 vs 多表结构变更 ===================

class CrossTableCorrectionVsDdlCampaignTest(_Settlement):
    """B1：同批更正整体落在变更前或后的同一结构快照。"""

    def _campaign(self, cid="ddl-1"):
        return self.p.alter_schemas(cid, [
            ("orders", add_nullable_col("tag")),
            ("details", add_nullable_col("tag")),
        ])

    def _correction(self, cid="corr-1"):
        # 更正数据按提交时各表当前版本的键列名（战后加了 tag，但它可空，
        # 可不携带——对齐时补确定缺省 None）
        ov = self.p.schemas.current_version("orders")
        dv = self.p.schemas.current_version("details")
        okc = self.p.schemas.key_col_at("orders", ov)
        dkc = self.p.schemas.key_col_at("details", dv)
        return self.p.submit_correction(CorrectionBatch(cid, [
            CorrectionItem("orders", "S1", EventKind.UPSERT, ov,
                           {okc: "S1", "amount": 222}),
            CorrectionItem("details", "S1", EventKind.UPSERT, dv,
                           {dkc: "S1", "qty": 22}),
        ]))

    def _assert_one_snapshot(self):
        o = self.p.current_state("orders")["S1"]
        d = self.p.current_state("details")["S1"]
        self.assertEqual(
            "tag" in o, "tag" in d,
            f"同批更正落到不同字段集合：orders.tag={'tag' in o}, "
            f"details.tag={'tag' in d}")
        return o, d

    def test_correction_entirely_before_campaign(self):
        self._correction()
        self._campaign()
        # 物理落盘：更正事件对齐到变更前结构，两表都不带 tag（战役不回改落盘）
        self.assertNotIn("tag", self.correction_event_keys("corr-1", "orders"))
        self.assertNotIn("tag", self.correction_event_keys("corr-1", "details"))
        # 当前状态：两表都被归一化到战后当前结构（tag 以确定缺省 None 呈现）
        o, d = self._assert_one_snapshot()
        self.assertIsNone(o["tag"])
        self.assertIsNone(d["tag"])
        self.assertEqual(o["amount"], 222)
        self.assertEqual(d["qty"], 22)

    def test_correction_entirely_after_campaign(self):
        self._campaign()
        self._correction()
        o, d = self._assert_one_snapshot()
        self.assertIn("tag", o)  # 两表都按战后结构补齐缺省
        self.assertIn("tag", d)
        self.assertEqual(o["amount"], 222)

    def test_split_single_table_ddls_expose_midpoint_but_campaign_does_not(self):
        """事故现场与正确用法的对照。

        - 两次**独立的单表 DDL**之间天然存在「orders 已新、details 仍旧」的
          中间态；若跨表更正此刻提交，两张表的落盘字段集合会不一致——
          这正是用户实测的一半新一半旧。
        - 正确的多表结构变更必须通过 alter_schemas() 战役提交：战役整体
          持锁，跨表更正只能排在战役整体之前或之后。

        本测试确定性地先复现中间态，再验证战役消除了它。
        """
        # 1) 复现中间态：orders 加列后、details 加列前
        self.p.alter_schema("orders", add_nullable_col("tag"))
        self.assertEqual(self.p.schemas.current_version("orders"), 2)
        self.assertEqual(self.p.schemas.current_version("details"), 1)

        # 2) 战役要求把「一组关联表的结构变更」作为一个单元提交。
        #    details 补加列必须单独再做；此时 orders.tag 已存在，
        #    因此正确的多表战役在事故发生前就应整体提交，而不是事后拼装。
        # 用全新环境演示战役的正确用法：战役期间不存在中间态可被跨表更正观察到。
        root = f"{self.root}/clean"
        p2 = Pipeline(root)
        p2.init_table("orders",
                      [Column("settle_id", "str"), Column("amount", "int")])
        p2.init_table("details",
                      [Column("settle_id", "str"), Column("qty", "int")])
        p2.submit_business_batch(BusinessBatch("S1", [
            TableBatchEntry("orders", [
                Record(1, {"settle_id": "S1", "amount": 100}, 1)], 1),
            TableBatchEntry("details", [
                Record(1, {"settle_id": "S1", "qty": 2}, 1)], 1),
        ]))

        midpoint_seen = threading.Event()

        def campaign_while_watch():
            def watch():
                # 在战役提交完成前反复读两表版本：战役整体持锁期间，
                # 任何时刻两表要么都是 v1、要么都是 v2，不会出现 (2,1)
                while not midpoint_seen.is_set():
                    ov = p2.schemas.current_version("orders")
                    dv = p2.schemas.current_version("details")
                    assert (ov, dv) in ((1, 1), (2, 2)), (ov, dv)

            watcher = threading.Thread(target=watch)
            watcher.start()
            p2.alter_schemas("atomic", [
                ("orders", add_nullable_col("tag")),
                ("details", add_nullable_col("tag")),
            ])
            midpoint_seen.set()
            watcher.join()

        campaign_while_watch()
        self.assertEqual(
            (p2.schemas.current_version("orders"),
             p2.schemas.current_version("details")), (2, 2))

    def test_concurrent_campaign_and_correction_never_split(self):
        """高并发 50 轮：战役与跨表更正同时发起，从不出现一半新一半旧。"""
        for run in range(50):
            root = f"{self.root}/iter{run}"
            pp = Pipeline(root)
            pp.init_table("orders",
                          [Column("settle_id", "str"), Column("amount", "int")])
            pp.init_table("details",
                          [Column("settle_id", "str"), Column("qty", "int")])
            pp.submit_business_batch(BusinessBatch(f"S{run}", [
                TableBatchEntry("orders", [
                    Record(1, {"settle_id": "S1", "amount": 100}, 1)], 1),
                TableBatchEntry("details", [
                    Record(1, {"settle_id": "S1", "qty": 2}, 1)], 1),
            ]))
            barrier = threading.Barrier(2)
            errs = []

            def ddl():
                barrier.wait()
                try:
                    pp.alter_schemas(f"d{run}", [
                        ("orders", add_nullable_col("tag")),
                        ("details", add_nullable_col("tag"))])
                except Exception as exc:  # noqa: BLE001
                    errs.append(exc)

            def corr():
                barrier.wait()
                try:
                    pp.submit_correction(CorrectionBatch(f"c{run}", [
                        CorrectionItem("orders", "S1", EventKind.UPSERT, 1,
                                       {"settle_id": "S1", "amount": 222}),
                        CorrectionItem("details", "S1", EventKind.UPSERT, 1,
                                       {"settle_id": "S1", "qty": 22})]))
                except Exception as exc:  # noqa: BLE001
                    errs.append(exc)

            ts = [threading.Thread(target=ddl), threading.Thread(target=corr)]
            for x in ts:
                x.start()
            for x in ts:
                x.join()
            self.assertEqual(errs, [])
            o = pp.current_state("orders")["S1"]
            d = pp.current_state("details")["S1"]
            self.assertEqual("tag" in o, "tag" in d)

    def test_campaign_idempotent_replay(self):
        self._campaign("ddl-1")
        results = self.p.alter_schemas("ddl-1", [
            ("orders", add_nullable_col("tag")),
            ("details", add_nullable_col("tag")),
        ])
        # 回放不二次分配版本
        self.assertEqual(
            [r.version for r in results], [2, 2])
        self.assertEqual(self.p.schemas.current_version("orders"), 2)
        self.assertEqual(self.p.schemas.current_version("details"), 2)

    def test_campaign_concurrent_same_id_commits_once(self):
        """相同 campaign_id 并发提交：只有一次生效，另一路幂等回放，版本不重复。"""
        barrier = threading.Barrier(2)
        errs = []

        def submit():
            barrier.wait()
            try:
                self.p.alter_schemas("same-id", [
                    ("orders", add_nullable_col("tag")),
                    ("details", add_nullable_col("tag"))])
            except Exception as exc:  # noqa: BLE001
                errs.append(exc)

        ts = [threading.Thread(target=submit) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errs, [])
        self.assertEqual(self.p.schemas.current_version("orders"), 2)
        self.assertEqual(self.p.schemas.current_version("details"), 2)
        # 每个 (table,version) 只有一条结构流水
        events = [(e["table"], e["version"])
                  for e in self.p.schemas.changelog.read_all()
                  if e["change_type"] == "ADD_COLUMN"]
        self.assertEqual(sorted(events), [("details", 2), ("orders", 2)])
        docs = [d for d in self.p._ddl_campaign_store.read_all()
                if d["campaign_id"] == "same-id"]
        self.assertEqual([d["state"] for d in docs].count("PREPARED"), 1)

    def test_campaign_crash_midway_restart_completes(self):
        """PREPARED 战役已落盘、结构事件只写了一半：重启整体补全，
        随后恢复的跨表事务面对同一结构。"""
        events = []
        for tbl in ("orders", "details"):
            events.append(self.p.schemas.plan_change(
                tbl, add_nullable_col("tag")))
        self.p._ddl_campaign_store.append({
            "campaign_id": "dc-crash", "state": "PREPARED", "events": events})
        # 崩溃现场：只有 orders 的事件进了结构流水
        self.p.schemas.changelog.append(events[0])

        p2 = Pipeline(self.root)
        self.assertEqual(p2.schemas.current_version("orders"), 2)
        self.assertEqual(p2.schemas.current_version("details"), 2)
        self.assertEqual(p2._find_ddl_campaign("dc-crash")["state"],
                         "COMMITTED")
        # 恢复后两表已是同一快照，跨表更正整体落在战后结构
        p2.submit_correction(CorrectionBatch("after", [
            CorrectionItem("orders", "S1", EventKind.UPSERT, 2,
                           {"settle_id": "S1", "amount": 5, "tag": "x"}),
            CorrectionItem("details", "S1", EventKind.UPSERT, 2,
                           {"settle_id": "S1", "qty": 5, "tag": "y"})]))
        self.assertEqual(
            p2.current_state("orders")["S1"]["tag"], "x")
        self.assertEqual(
            p2.current_state("details")["S1"]["tag"], "y")

    def test_recovery_completes_campaign_before_prepared_transaction(self):
        """崩溃现场同时存在：未完成 DDL 战役 + 未提交跨表更正。
        重启必须先补全结构、再前滚事务，最终两表结论按同一战后结构解释。"""
        # 战役 PREPARED（两表加 tag），结构事件一条都还没写
        events = [self.p.schemas.plan_change(t, add_nullable_col("tag"))
                  for t in ("orders", "details")]
        self.p._ddl_campaign_store.append({
            "campaign_id": "dc-pending", "state": "PREPARED",
            "events": events})
        # 跨表更正 PREPARED（按 v1 对齐，不带 tag），尚未应用
        self.p.tx_log.append_state({
            "txn_id": "corr-pending", "kind": "CORRECTION",
            "state": "PREPARED",
            "entries": [
                {"table": "orders", "accepted_count": 1, "checkpoint": 0,
                 "rejected": [], "reason": None, "detail": "", "key": "S1"},
                {"table": "details", "accepted_count": 1, "checkpoint": 0,
                 "rejected": [], "reason": None, "detail": "", "key": "S1"}],
            "plans": [
                {"type": "correction", "table": "orders", "key": "S1",
                 "op": "UPSERT", "data": {"settle_id": "S1", "amount": 321},
                 "schema_version": 1, "issued_seq": None,
                 "correction_id": "corr-pending"},
                {"type": "correction", "table": "details", "key": "S1",
                 "op": "UPSERT", "data": {"settle_id": "S1", "qty": 32},
                 "schema_version": 1, "issued_seq": None,
                 "correction_id": "corr-pending"}]})

        p2 = Pipeline(self.root)
        # 结构先到战后
        self.assertEqual(p2.schemas.current_version("orders"), 2)
        self.assertEqual(p2.schemas.current_version("details"), 2)
        # 更正事务随后前滚；落盘保留其生产时 v1 形态（无 tag，不可变事件），
        # 当前状态统一对齐到战后结构（tag 以确定缺省呈现），两表结论一致
        self.assertEqual(p2.business_trace("corr-pending")["state"],
                         "COMMITTED")
        o = p2.current_state("orders")["S1"]
        d = p2.current_state("details")["S1"]
        self.assertEqual(o["amount"], 321)
        self.assertEqual(d["qty"], 32)
        # 两表当前状态都按战后结构呈现：业务键列 + 数值列 + tag 三字段
        self.assertEqual(set(o), {"settle_id", "amount", "tag"})
        self.assertEqual(set(d), {"settle_id", "qty", "tag"})


# =================== C. 乱序 / 重复 / 重放 ===================

class CorrectionOrderingByIdempotencyTest(_Settlement):
    def test_older_issued_seq_cannot_overwrite_newer_conclusion(self):
        self.corr("new-first", "orders", 150, seq=10)
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.corr("old-late", "orders", 110, seq=5)
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.STALE_CORRECTION)
        # 当前结论保持更新的 150；被拒的旧请求不取号、不产生更正事件
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 150)
        self.assertIsNone(self.correction_event_keys("old-late", "orders"))
        # 失败本身有 ABORTED 留痕可追溯
        self.assertEqual(self.p.business_trace("old-late")["state"],
                         "ABORTED")

    def test_newer_issued_seq_applies_then_older_withdraw_rejected(self):
        self.corr("c10", "orders", 150, seq=10)
        self.corr("c11", "orders", 160, seq=11)
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 160)
        # 更早发起的撤回晚到：不得撤回更新结论
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.corr("wd-old", "orders", None, seq=9,
                      op=EventKind.WITHDRAW)
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.STALE_CORRECTION)
        self.assertIn("S1", self.p.current_state("orders"))

    def test_withdraw_newer_then_older_correction_late_is_stale(self):
        self.corr("wd", "orders", None, seq=20, op=EventKind.WITHDRAW)
        self.assertEqual(self.p.current_state("orders"), {})
        # 撤回（seq=20）之后，更早的更正（seq=15）晚到：
        # 优先按发起先后判 STALE，而不是 KEY_NOT_FOUND，结论不得复活
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.corr("ups-late", "orders", 999, seq=15)
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.STALE_CORRECTION)
        self.assertEqual(self.p.current_state("orders"), {})

    def test_equal_issued_seq_is_rejected_but_same_id_replays(self):
        self.corr("c10", "orders", 150, seq=10)
        # 不同请求、相同发起序号：视为不更新的请求拒绝，避免平级翻转
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.corr("c10-dup-content", "orders", 1, seq=10)
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.STALE_CORRECTION)
        # 同一条请求（相同 correction_id）重复提交：幂等回放，不二次取号
        first = self.corr("c10", "orders", 150, seq=10)
        self.assertTrue(first.replay)
        seqs = [e["seq"] for e in self.p.key_history("orders", "S1")]
        self.assertEqual(len(seqs), 2)  # 1 次增量 + 1 次更正

    def test_duplicate_submission_and_interrupted_replay_same_outcome(self):
        """同一条更正重复提交 + PREPARED 中断重放，最终状态与首次完全一致。"""
        cid = "corr-once"
        item = CorrectionItem("orders", "S1", EventKind.UPSERT, 1,
                              {"settle_id": "S1", "amount": 777}, issued_seq=3)
        self.p.submit_correction(CorrectionBatch(cid, [item]))
        amount = self.p.current_state("orders")["S1"]["amount"]
        seq_after = self.p.state.get_write_seq("orders")

        # 显式重复提交：回放
        replay = self.p.submit_correction(CorrectionBatch(cid, [item]))
        self.assertTrue(replay.replay)
        self.assertEqual(self.p.state.get_write_seq("orders"), seq_after)

        # 模拟崩溃后重放：手工放一条带相同 correction_id 的 PREPARED，
        # 重启恢复时按 (id,table,key) 去重，不得再产生事件或翻转
        doc = {"txn_id": cid, "kind": "CORRECTION", "state": "PREPARED",
               "entries": [{"table": "orders", "accepted_count": 1,
                            "checkpoint": 0, "rejected": [], "reason": None,
                            "detail": "", "key": "S1"}],
               "plans": [{"type": "correction", "table": "orders", "key": "S1",
                          "op": "UPSERT",
                          "data": {"settle_id": "S1", "amount": 777},
                          "schema_version": 1, "issued_seq": 3,
                          "correction_id": cid}]}
        # 真实场景中同一 txn_id 不会同时有 COMMITTED 与 PREPARED；
        # 此处仅验证去重原语：构造一个新 id 但复用事件 correction_id
        doc["txn_id"] = "corr-once-replay-prepared"
        doc["plans"][0]["correction_id"] = cid
        self.p.tx_log.append_state(doc)
        p2 = Pipeline(self.root)
        self.assertEqual(p2.current_state("orders")["S1"]["amount"], amount)
        self.assertEqual(p2.state.get_write_seq("orders"), seq_after)
        events = [e for e in p2.key_history("orders", "S1")
                  if e["correction_id"] == cid]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["issued_seq"], 3)

    def test_cross_table_stale_item_aborts_whole_batch(self):
        """跨表更正中一条是旧请求：整批中止，另一张表也不生效。"""
        self.corr("d-new", "details", 9, seq=10)
        with self.assertRaises(BusinessBatchRejected) as ctx:
            self.p.submit_correction(CorrectionBatch("mix", [
                CorrectionItem("orders", "S1", EventKind.UPSERT, 1,
                               {"settle_id": "S1", "amount": 500}, issued_seq=2),
                CorrectionItem("details", "S1", EventKind.UPSERT, 1,
                               {"settle_id": "S1", "qty": 8}, issued_seq=5)]))
        self.assertEqual(ctx.exception.failures[0].reason,
                         RejectReason.STALE_CORRECTION)
        # orders 未半生效；details 保持 seq=10 的结论 9
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 100)
        self.assertEqual(self.p.current_state("details")["S1"]["qty"], 9)

    def test_without_issued_seq_falls_back_to_arrival_order(self):
        """不提供发起序号：保持原有按到达（提交）先后生效的兼容语义。"""
        self.corr("a", "orders", 111)
        self.corr("b", "orders", 222)
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 222)
        # 混用：无序号请求不参与新旧裁决，也不会挡住后续带序号的更新请求
        self.corr("c", "orders", 333, seq=1)
        self.assertEqual(self.p.current_state("orders")["S1"]["amount"], 333)


if __name__ == "__main__":
    unittest.main()
