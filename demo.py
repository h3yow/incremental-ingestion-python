"""端到端演示：本地存储模拟源端/目标端的增量接入。

运行：uv run python demo.py
会在 ./data_demo 下生成源记录、目标行、位点、批次留痕与结构变更流水。
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.models import (Batch, BusinessBatch, ChangeType, Column,
                        CorrectionBatch, CorrectionItem, EventKind, Record,
                        RejectReason, SchemaChange, TableBatchEntry)
from app.pipeline import BatchRejected, BusinessBatchRejected, Pipeline
from app.source import LocalSource
from app.storage import JsonlStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path("data_demo")


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    p = Pipeline(str(ROOT))
    source = LocalSource(JsonlStore(ROOT / "source.jsonl",
                                    lock=p._store_lock))

    # 1) 建表
    p.init_table("orders", [Column("id", "int"), Column("name", "str")])

    # 2) 源端产生 3 条数据，其中第 2 条是坏数据
    source.produce("orders", {"id": 1, "name": "alice"}, 1)
    source.produce("orders", {"id": "BAD-ID", "name": "bad"}, 1)
    source.produce("orders", {"id": 3, "name": "carol"}, 1)

    # 3) 提交批次：只落地连续前缀，失败原因留痕
    result = p.run_once("orders", source, "batch-001")
    print("首次提交：", result)

    # 4) 重复提交同一批次（幂等回放）
    same = Batch("batch-001", "orders", source.read("orders", 1), 1)
    print("重复提交：replay =", p.submit_batch(same).replay)

    # 5) 修复坏记录后，从 checkpoint+1 重新提交新批次
    fixed = Batch("batch-002", "orders", [
        Record(2, {"id": 2, "name": "bob-fixed"}, 1),
        Record(3, {"id": 3, "name": "carol"}, 1),
    ], schema_version=1)
    print("修复重提：", p.submit_batch(fixed))

    # 6) 结构变更：加列(带缺省)、加可空列、普通列改名、业务键列改名、删非键列。
    #    业务键列（id）只允许改名、不允许删除——改名后历史结果仍可定位与更正。
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.ADD_COLUMN, "amount",
        column=Column("amount", "float", required=True, default=0.0)))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.ADD_COLUMN, "note",
        column=Column("note", "str", required=False)))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.RENAME_COLUMN, "name", to_name="customer"))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.RENAME_COLUMN, "id", to_name="biz_id"))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.DROP_COLUMN, "note"))
    # 尝试删除业务键列会被明确拒绝（KEY_COLUMN_PROTECTED），且无副作用
    try:
        p.alter_schema("orders", SchemaChange(
            0, ChangeType.DROP_COLUMN, "biz_id"))
    except Exception as exc:  # noqa: BLE001
        print("删除业务键列被拒绝：", getattr(exc, "reason", type(exc).__name__),
              "-", exc)
    # 改名键列后新数据照常接入，老数据在当前状态中仍完整
    source.produce("orders", {"id": 4, "name": "dave"}, 1)
    result = p.run_once("orders", source, "batch-003")
    print("结构变更后提交：", result)
    print("改名键列后的当前状态：",
          {k: v for k, v in p.current_state("orders").items()})

    # 7) 位点回退 / 跳号被明确拒绝（当前为版本 6，字段为 biz_id/customer/amount）
    for bad_batch in [
        Batch("back", "orders",
              [Record(2, {"biz_id": 2, "customer": "x", "amount": 1.0})], 6),
        Batch("jump", "orders",
              [Record(99, {"biz_id": 99, "customer": "y", "amount": 2.0})], 6),
    ]:
        try:
            p.submit_batch(bad_batch)
        except BatchRejected as exc:
            print("拒绝位点操作：", exc.reason.value, "-", exc)

    print("最终快照：", p.snapshot("orders"))


def settlement_demo(p: Pipeline) -> None:
    """结算链路：跨表业务批次整体提交 + 历史更正/撤回。"""
    print("\n===== 结算链路演示 =====")
    p.init_table("settle_orders",
                 [Column("settle_id", "str"), Column("amount", "int")],
                 key_col="settle_id")
    p.init_table("settle_details",
                 [Column("line_id", "str"), Column("settle_id", "str"),
                  Column("item", "str"), Column("qty", "int")],
                 key_col="line_id")

    # 1) 一次结算同时影响订单表与明细表：整体生效
    txn = BusinessBatch("S2026093001", [
        TableBatchEntry("settle_orders", [
            Record(1, {"settle_id": "S001", "amount": 100}, 1)], 1),
        TableBatchEntry("settle_details", [
            Record(1, {"line_id": "S001-book", "settle_id": "S001",
                       "item": "book", "qty": 1}, 1),
            Record(2, {"line_id": "S001-pen", "settle_id": "S001",
                       "item": "pen", "qty": 2}, 1)], 1),
    ])
    print("跨表结算提交：", p.submit_business_batch(txn).committed)
    print("重复提交回放：", p.submit_business_batch(txn).replay)

    # 2) 明细表中途出错：两表都不生效，留痕精确定位
    bad = BusinessBatch("S2026093002", [
        TableBatchEntry("settle_orders", [
            Record(2, {"settle_id": "S002", "amount": 200}, 1)], 1),
        TableBatchEntry("settle_details", [
            Record(3, {"line_id": "S002-bad", "settle_id": "S002",
                       "item": "bad", "qty": "X"}, 1)], 1),
    ])
    try:
        p.submit_business_batch(bad)
    except BusinessBatchRejected as exc:
        print("跨表失败定位：",
              [(f.table, f.reason.value, f.detail) for f in exc.failures])
    print("失败后订单表仍只有：",
          [r["settle_id"] for r in p.target.load("settle_orders")])

    # 3) 上游对历史结果 S001 发起更正（金额 100 -> 120，明细数量 1 -> 3），跨表一起更正
    corr = CorrectionBatch("FIX-S001-1", [
        CorrectionItem("settle_orders", "S001", EventKind.UPSERT, 1,
                       {"settle_id": "S001", "amount": 120}),
        CorrectionItem("settle_details", "S001-book", EventKind.UPSERT, 1,
                       {"line_id": "S001-book", "settle_id": "S001",
                        "item": "book", "qty": 3}),
    ])
    print("历史更正提交：", p.submit_correction(corr).committed)
    print("更正重复提交回放：", p.submit_correction(corr).replay)
    print("当前订单状态：", p.current_state("settle_orders"))
    print("S001 经历过的结论：",
          [(e["kind"], e["data"]) for e in p.key_history("settle_orders", "S001")])

    # 4) 撤回历史结果 S001：订单与其明细行当前状态不可见，留痕仍在
    wd = CorrectionBatch("WD-S001", [
        CorrectionItem("settle_orders", "S001", EventKind.WITHDRAW, 1),
        CorrectionItem("settle_details", "S001-book", EventKind.WITHDRAW, 1),
        CorrectionItem("settle_details", "S001-pen", EventKind.WITHDRAW, 1),
    ])
    p.submit_correction(wd)
    print("撤回后当前状态：orders=", p.current_state("settle_orders"),
          "details=", p.current_state("settle_details"))
    print("撤回后历史仍可解释：",
          [e["kind"] for e in p.key_history("settle_orders", "S001")])


def settlement_bugfix_demo(p: Pipeline) -> None:
    """bugfix-b：键列改名定位、多表 DDL 战役、更正发起先后裁决。"""
    print("\n===== 结算链路缺陷修复演示 =====")
    p.init_table("fix_orders",
                 [Column("settle_id", "str"), Column("amount", "int")],
                 key_col="settle_id")
    p.init_table("fix_details",
                 [Column("settle_id", "str"), Column("qty", "int")],
                 key_col="settle_id")
    p.submit_business_batch(BusinessBatch("T1", [
        TableBatchEntry("fix_orders", [
            Record(1, {"settle_id": "S001", "amount": 100}, 1)], 1),
        TableBatchEntry("fix_details", [
            Record(1, {"settle_id": "S001", "qty": 2}, 1)], 1),
    ]))

    # 1) 发起先后裁决：同一笔结果的两条更正乱序送达（带 issued_seq）
    p.submit_correction(CorrectionBatch("FIX-NEW", [
        CorrectionItem("fix_orders", "S001", EventKind.UPSERT, 1,
                       {"settle_id": "S001", "amount": 150}, issued_seq=100)]))
    try:
        # 更早发起（seq=90）、更晚送达的旧更正：不得翻掉 150
        p.submit_correction(CorrectionBatch("FIX-OLD-LATE", [
            CorrectionItem("fix_orders", "S001", EventKind.UPSERT, 1,
                           {"settle_id": "S001", "amount": 110},
                           issued_seq=90)]))
    except BusinessBatchRejected as exc:
        print("乱序旧更正被拒绝：",
              [(f.reason.value, f.detail[:30]) for f in exc.failures])
    print("乱序裁决后当前金额（保持更新结论 150）：",
          p.current_state("fix_orders")["S001"]["amount"])

    # 2) 业务键列改名 settle_id -> biz_id：改名后新数据可接入、老结果可更正
    p.alter_schema("fix_orders", SchemaChange(
        0, ChangeType.RENAME_COLUMN, "settle_id", to_name="biz_id"))
    p.submit_correction(CorrectionBatch("FIX-AFTER-RENAME", [
        CorrectionItem("fix_orders", "S001", EventKind.UPSERT, 2,
                       {"biz_id": "S001", "amount": 160})]))
    print("键列改名后更正老结果，当前金额：",
          p.current_state("fix_orders")["S001"]["amount"])
    try:
        p.alter_schema("fix_orders", SchemaChange(
            0, ChangeType.DROP_COLUMN, "biz_id"))
    except Exception as exc:  # noqa: BLE001
        print("删除业务键列被拒绝：", getattr(exc, "reason", type(exc).__name__))

    # 3) 多表 DDL 战役：两张表一起加列，跨表更正整体落在同一结构快照
    p.alter_schemas("DDL-1", [
        ("fix_orders", SchemaChange(
            0, ChangeType.ADD_COLUMN, "tag",
            column=Column("tag", "str", required=False))),
        ("fix_details", SchemaChange(
            0, ChangeType.ADD_COLUMN, "tag",
            column=Column("tag", "str", required=False))),
    ])
    p.submit_correction(CorrectionBatch("FIX-CROSS", [
        CorrectionItem("fix_orders", "S001", EventKind.UPSERT, 3,
                       {"biz_id": "S001", "amount": 170, "tag": "ok"}),
        CorrectionItem("fix_details", "S001", EventKind.UPSERT, 2,
                       {"settle_id": "S001", "qty": 3, "tag": "ok"}),
    ]))
    print("DDL 战役后跨表更正，两表 tag：",
          p.current_state("fix_orders")["S001"].get("tag"),
          p.current_state("fix_details")["S001"].get("tag"))


if __name__ == "__main__":
    main()
    # 结算链路使用独立目录，避免覆盖上面的单表演示数据
    demo_root = Path("data_demo/settlement")
    import shutil
    shutil.rmtree(demo_root, ignore_errors=True)
    sp = Pipeline(str(demo_root))
    settlement_demo(sp)
    settlement_bugfix_demo(sp)
