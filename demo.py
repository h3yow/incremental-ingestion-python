"""端到端演示：本地存储模拟源端/目标端的增量接入。

运行：uv run python demo.py
会在 ./data_demo 下生成源记录、目标行、位点、批次留痕与结构变更流水。
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.models import (Batch, ChangeType, Column, Record, RejectReason,
                        SchemaChange)
from app.pipeline import BatchRejected, Pipeline
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

    # 6) 结构变更：加列(带缺省)、改名、删列，旧版本记录仍可对齐
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.ADD_COLUMN, "amount",
        column=Column("amount", "float", required=True, default=0.0)))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.RENAME_COLUMN, "name", to_name="customer"))
    p.alter_schema("orders", SchemaChange(
        0, ChangeType.DROP_COLUMN, "id"))
    source.produce("orders", {"id": 4, "name": "dave"}, 1)
    result = p.run_once("orders", source, "batch-003")
    print("结构变更后提交：", result)

    # 7) 位点回退 / 跳号被明确拒绝
    for bad_batch in [
        Batch("back", "orders", [Record(2, {"customer": "x", "amount": 1.0})], 4),
        Batch("jump", "orders", [Record(99, {"customer": "y", "amount": 2.0})], 4),
    ]:
        try:
            p.submit_batch(bad_batch)
        except BatchRejected as exc:
            print("拒绝位点操作：", exc.reason.value, "-", exc)

    print("最终快照：", p.snapshot("orders"))


if __name__ == "__main__":
    main()
