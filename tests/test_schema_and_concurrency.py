"""结构变更兼容与并发测试：
加列稳定缺省、删列/改名不错位、结构版本超前拒绝、并发提交与变更结论一致。
"""

from app.models import (Batch, ChangeType, Column, Record, RejectReason,
                        SchemaChange)
from app.pipeline import BatchRejected, Pipeline
from app.schema_store import SchemaError
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
        # v2 加必填列 amount，缺省 0.0；v3 加可空列 note；v4 name->customer；v5 删 id
        self.pipeline.alter_schema("orders", add_column("amount", "float",
                                                        True, 0.0))
        self.pipeline.alter_schema(
            "orders", add_column("note", "str", required=False))
        self.pipeline.alter_schema("orders", SchemaChange(
            0, ChangeType.RENAME_COLUMN, "name", to_name="customer"))
        self.pipeline.alter_schema(
            "orders", SchemaChange(0, ChangeType.DROP_COLUMN, "id"))
        self.assertEqual(self.pipeline.schemas.current_version("orders"), 5)

        # 版本 1 的老数据对齐到版本 5
        r = self.pipeline.submit_batch(Batch("old", "orders", [
            Record(2, {"id": 2, "name": "bob"}, 1),
        ], 5))
        self.assertEqual(r.accepted_count, 1)
        rows = self.pipeline.target.load("orders")
        self.assertEqual(rows[-1],
                         {"customer": "bob", "amount": 0.0, "note": None})
        # 已删的 id 不再出现，改名后的 customer 正确承载原值（无错位）
        self.assertNotIn("id", rows[-1])
        self.assertNotIn("name", rows[-1])

        # 未知列 / 类型不符 / 必填缺失 原因可区分。
        # 注意：按记录自身版本（v1）判定字段合法性——
        # id 是 v1 的合法旧列（删列后仅丢弃），不算 UNKNOWN_COLUMN；
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
                0, ChangeType.DROP_COLUMN, "id"))
            result = sub.submit_batch(Batch(
                f"case{i}", "orders",
                [Record(1, data, ver)], 4))
            self.assertEqual(result.accepted_count, 0)
            self.assertEqual(sub.checkpoints.get("orders"), 0,
                             "记录被拒时位点不得推进")
            self.assertEqual(result.rejected[0].reason, reason)

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
        """并发提交与结构变更：表锁串行化，结论一致且可解释。"""
        # 预产 200 条源数据
        for i in range(1, 201):
            self.source.produce("orders", {"id": i, "name": f"n{i}"}, 1)

        errors: list[Exception] = []

        def ingest(start: int, bid: str):
            try:
                recs = self.source.read("orders", start, 100)
                self.pipeline.submit_batch(
                    Batch(bid, "orders", recs, 1))
            except Exception as exc:  # noqa: BLE001 - 收集线程错误
                errors.append(exc)

        def add_col():
            try:
                self.pipeline.alter_schema(
                    "orders", add_column("tag", "str", required=False))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=ingest, args=(1, "chunk-1"))
        t2 = threading.Thread(target=ingest, args=(101, "chunk-2"))
        td = threading.Thread(target=add_col)
        t1.start(); td.start(); t2.start()
        t1.join(); td.join(); t2.join()

        self.assertEqual(errors, [])
        snap = self.pipeline.snapshot("orders")
        # 无论谁先执行，两批位点区间连续 -> 最终 200 行，位点 200
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
