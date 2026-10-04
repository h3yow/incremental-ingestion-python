"""结构变更兼容与并发测试：
加列稳定缺省、删列/改名不错位、业务键列改名可定位/删列被保护、结构版本超前拒绝、
并发提交与变更结论一致。
"""

from app.models import (Batch, ChangeType, Column, Record, RejectReason,
                        SchemaChange)
from app.pipeline import BatchRejected, Pipeline
from app.schema_store import KeyColumnProtectedError, SchemaError
from tests._helpers import IngestionCase
import threading


def add_column(name, typ, required=True, default=None):
    return SchemaChange(0, ChangeType.ADD_COLUMN, name,
                        column=Column(name, typ, required=required,
                                      default=default))


class SchemaEvolutionTest(IngestionCase):
    def test_added_required_column_needs_explicit_default(self):
        """新增必填列不允许缺省语义不确定：DDL 阶段拒绝。"""
        with self.assertRaises(SchemaError):
            self.pipeline.alter_schema(
                "orders", add_column("amount", "float", required=True))

    def test_old_records_align_with_stable_default_and_rename_drop(self):
        """旧版本记录：加列补稳定缺省；改名不错位；删列不写入。"""
        self.pipeline.submit_batch(Batch("v1", "orders", [
            Record(1, {"id": 1, "name": "alice"}, 1),
        ], 1))
        # v2 加必填列 amount，缺省 0.0；v3 加可空列 note；v4 name->customer；
        # v5 业务键列 id->biz_id（允许改名，历史结果仍可定位/更正）；v6 删非键列 note
        self.pipeline.alter_schema("orders", add_column("amount", "float",
                                                        True, 0.0))
        self.pipeline.alter_schema(
            "orders", add_column("note", "str", required=False))
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "name", to_name="customer"))
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "id", to_name="biz_id"))
        self.pipeline.alter_schema(
            "orders", SchemaChange(0, ChangeType.DROP_COLUMN, "note"))
        self.assertEqual(self.pipeline.schemas.current_version("orders"), 6)

        # 版本 1 的老数据对齐到版本 6（含业务键列改名 id->biz_id）
        r = self.pipeline.submit_batch(Batch("old", "orders", [
            Record(2, {"id": 2, "name": "bob"}, 1),
        ], 6))
        self.assertEqual(r.accepted_count, 1)
        rows = self.pipeline.target.load("orders")
        self.assertEqual(rows[-1],
                         {"biz_id": 2, "customer": "bob", "amount": 0.0})
        # 已删的 note/旧名 id/name 不再出现，改名后的 biz_id/customer 正确承载原值
        self.assertNotIn("id", rows[-1])
        self.assertNotIn("name", rows[-1])
        self.assertNotIn("note", rows[-1])

        # 关键：业务键列改名后，当前状态仍完整（新老版本行归并到同一键空间），
        # 老结果仍可按原业务键值更正
        state = self.pipeline.current_state("orders")
        self.assertEqual(set(state.keys()), {1, 2})
        self.assertEqual(state[1]["customer"], "alice")
        self.assertEqual(state[2]["customer"], "bob")
        from app.models import (CorrectionBatch, CorrectionItem, EventKind)
        self.pipeline.submit_correction(CorrectionBatch("fix-1", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 6,
                           {"biz_id": 1, "customer": "ALICE", "amount": 9.0})]))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["customer"], "ALICE")

        # 未知列 / 类型不符 / 必填缺失 原因可区分。
        # 注意：按记录自身版本（v1）判定字段合法性——
        # id 是 v1 的合法旧列（改名后仅映射到 biz_id），不算 UNKNOWN_COLUMN；
        # ghost 从不属于任何版本，才是 UNKNOWN_COLUMN。
        cases = [
            ({"id": 3, "name": "x", "ghost": 1}, 1, RejectReason.UNKNOWN_COLUMN),
            ({"id": "3", "name": "x"}, 1, RejectReason.TYPE_MISMATCH),
            ({"id": 4}, 1, RejectReason.MISSING_REQUIRED),
        ]
        # 记录级失败返回 BatchResult（不整批抛错），位点不推进；
        # 位点/结构版本级错误才抛 BatchRejected。
        for i, (data, ver, reason) in enumerate(cases, start=1):
            # 每个原因一个全新 Pipeline 与表结构，避免批次留痕相互干扰
            sub = Pipeline(f"{self.root}/sub{i}")
            sub.init_table("orders", self.cols)
            sub.alter_schema("orders", add_column("amount", "float",
                                                  True, 0.0))
            sub.alter_schema("orders", SchemaChange(
                0, ChangeType.RENAME_COLUMN, "name", to_name="customer"))
            sub.alter_schema("orders", SchemaChange(
                0, ChangeType.RENAME_COLUMN, "id", to_name="biz_id"))
            result = sub.submit_batch(Batch(
                f"case{i}", "orders",
                [Record(1, data, ver)], 4))
            self.assertEqual(result.accepted_count, 0)
            self.assertEqual(sub.checkpoints.get("orders"), 0,
                             "记录被拒时位点不得推进")
            self.assertEqual(result.rejected[0].reason, reason)

    def test_dropping_key_column_is_rejected_but_rename_keeps_history_live(self):
        """业务键列：删除在 DDL 阶段以可区分原因拒绝；改名后历史结果仍完整可更正。"""
        self.pipeline.submit_batch(Batch("v1", "orders", [
            Record(1, {"id": 1, "name": "alice"}, 1),
        ], 1))
        # 删除业务键列被挡住（原因明确，不是笼统的 SchemaError）
        with self.assertRaises(KeyColumnProtectedError) as ctx:
            self.pipeline.alter_schema(
                "orders", SchemaChange(0, ChangeType.DROP_COLUMN, "id"))
        self.assertEqual(ctx.exception.reason,
                         RejectReason.KEY_COLUMN_PROTECTED.value)
        # 结构流水未推进
        self.assertEqual(self.pipeline.schemas.current_version("orders"), 1)
        # 被挡之后该表仍能正常接入（失败操作不留任何副作用）
        self.pipeline.submit_batch(Batch("after-block", "orders", [
            Record(2, {"id": 2, "name": "bob"}, 1),
        ], 1))
        self.assertEqual(set(self.pipeline.current_state("orders")), {1, 2})

        # 改名业务键列后：新数据可接入、当前状态完整、老结果可更正/撤回/查历史
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "id", to_name="biz_id"))
        self.pipeline.submit_batch(Batch("v2", "orders", [
            Record(3, {"biz_id": 3, "name": "carol"}, 2),
        ], 2))
        state = self.pipeline.current_state("orders")
        self.assertEqual(set(state), {1, 2, 3})
        self.assertEqual(state[3]["biz_id"], 3)
        from app.models import (CorrectionBatch, CorrectionItem, EventKind)
        self.pipeline.submit_correction(CorrectionBatch("corr-old", [
            CorrectionItem("orders", 1, EventKind.UPSERT, 2,
                           {"biz_id": 1, "name": "ALICE"})]))
        self.assertEqual(
            self.pipeline.current_state("orders")[1]["name"], "ALICE")
        # 更正前的原值仍在历史留痕中可解释（含其生产时版本）
        hist = self.pipeline.key_history("orders", 1)
        self.assertEqual([e["data"]["name"] for e in hist], ["alice", "ALICE"])
        # 撤回改名前接入的老结果同样能定位
        self.pipeline.submit_correction(CorrectionBatch("wd-2", [
            CorrectionItem("orders", 2, EventKind.WITHDRAW, 2)]))
        self.assertEqual(set(self.pipeline.current_state("orders")), {1, 3})
        # 改名后再尝试删除新名的业务键列，同样被挡
        with self.assertRaises(KeyColumnProtectedError):
            self.pipeline.alter_schema(
                "orders", SchemaChange(0, ChangeType.DROP_COLUMN, "biz_id"))

    def test_schema_version_ahead_rejected(self):
        """批次基于比目标更新的结构版本：整批拒绝，无写入。"""
        with self.assertRaises(BatchRejected) as ctx:
            self.pipeline.submit_batch(Batch("future", "orders", [
                Record(1, {"id": 1, "name": "x"}, 9),
            ], 9))
        self.assertEqual(ctx.exception.reason,
                         RejectReason.SCHEMA_VERSION_AHEAD)
        self.assertEqual(self.pipeline.target.load("orders"), [])


class ConcurrencyTest(IngestionCase):
    def test_concurrent_submits_and_schema_change_are_consistent(self):
        """并发提交与结构变更：表锁串行化，结论一致且可解释。

        两个 worker 竞争消费同一源端：每次先读当前 checkpoint，再从
        checkpoint+1 拉取一段提交；若该段已被对方抢先提交
        （OFFSET_BACKWARD/OFFSET_GAP），则重读 checkpoint 重试，直到源端耗尽。
        位点连续协议因此收敛到确定结论：200 行、位点 200、无重复无丢失。
        """
        # 预产 200 条源数据
        for i in range(1, 201):
            self.source.produce("orders", {"id": i, "name": f"n{i}"}, 1)

        errors: list[Exception] = []
        total = 200
        import itertools
        ids = itertools.count(1)

        def consume(worker: str):
            # 每个 worker 每次抢 20 条；抢不到（被对方领先）就重读位点重来
            while True:
                cp = self.pipeline.checkpoints.get("orders")
                if cp >= total:
                    return
                start = cp + 1
                recs = self.source.read("orders", start, 20)
                if not recs:
                    return
                bid = f"{worker}-chunk-{next(ids)}"
                try:
                    self.pipeline.submit_batch(
                        Batch(bid, "orders", recs, 1))
                except BatchRejected as exc:
                    # 对方先提交：这是位点连续协议下的预期竞争结果，重试即可
                    if exc.reason in (RejectReason.OFFSET_BACKWARD,
                                      RejectReason.OFFSET_GAP):
                        continue
                    errors.append(exc)
                    return
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
                    return

        def add_col():
            try:
                self.pipeline.alter_schema(
                    "orders", add_column("tag", "str", required=False))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=consume, args=("w1",))
        t2 = threading.Thread(target=consume, args=("w2",))
        td = threading.Thread(target=add_col)
        t1.start(); td.start(); t2.start()
        t1.join(); td.join(); t2.join()

        self.assertEqual(errors, [])
        snap = self.pipeline.snapshot("orders")
        # 无论如何交错，两 worker 的位点区间连续 -> 最终 200 行，位点 200
        self.assertEqual(snap["checkpoint"], 200)
        self.assertEqual(len(snap["rows"]), 200, "并发不得重复或丢失")
        self.assertEqual([r["offset"] for r in
                          self.pipeline.target.rows.read_all()
                          if r["table"] == "orders"],
                         list(range(1, 201)))
        # 结构变更必然发生在某个可解释的版本上
        self.assertIn(snap["schema_version"], (1, 2))
        self.assertEqual(
            [c.name for c in self.pipeline.schemas.columns("orders")][-1],
            "tag")


if __name__ == "__main__":
    import unittest
    unittest.main()
