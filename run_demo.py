"""本地演示：用本地存储模拟源端/目标端，走完整增量接入流程。

运行：.venv/bin/python run_demo.py
默认使用 ./demo_data，重复运行会先清空该目录。
"""
from __future__ import annotations

import logging
import os
import shutil

from ingestion.api import build_engine
from ingestion.errors import IngestionError
from ingestion.models import Batch, Column
from ingestion.schema import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("demo")

DATA_DIR = "./demo_data"


def main() -> None:
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    engine = build_engine(DATA_DIR)
    reg: SchemaRegistry = engine.registry

    # 0) 建表
    reg.create_table("orders", "order_id", [
        Column("order_id", "int", False),
        Column("customer", "str", False),
        Column("amount", "float", True),
    ])
    LOG.info("表 orders 已创建，结构 v1：%s", reg.current("orders").names())

    # 1) 源端产生 6 条日志（含 2 条坏数据）
    source_rows = [
        {"order_id": 1, "customer": "alice", "amount": 10.5},
        {"order_id": 2, "customer": "bob"},                     # amount 缺省 None
        {"order_id": 3, "customer": None},                      # NULL_NOT_ALLOWED
        {"order_id": 4, "amount": 9.0},                         # MISSING_COLUMN
        {"order_id": 5, "customer": "carol", "amount": "x"},    # TYPE_MISMATCH
        {"order_id": 6, "customer": "dave", "amount": 6.0,
         "ghost": True},                                        # UNKNOWN_COLUMN
    ]
    for row in source_rows:
        engine.source.append("orders", row, schema_version=1)

    # 2) 模拟进程只处理了前 3 条就退出（断点）
    first = engine.pull_and_ingest("orders", max_records=3)
    LOG.info("第一轮接入：accepted=%s rejected=%s 位点=%s",
             first.accepted_offsets,
             [(r.offset, r.reason) for r in first.rejected],
             engine.checkpoints.get("orders"))

    # 3) 新进程从同一存储恢复，继续接入
    engine_restarted = build_engine(DATA_DIR)
    second = engine_restarted.pull_and_ingest("orders", max_records=10)
    LOG.info("断点续跑：accepted=%s rejected=%s 位点=%s",
             second.accepted_offsets,
             [(r.offset, r.reason) for r in second.rejected],
             engine_restarted.checkpoints.get("orders"))

    # 4) 重复提交首批次：返回同一结论，目标端不重复
    dup = engine_restarted.submit_batch(Batch(
        batch_id=first.batch_id, table="orders", start_offset=0,
        schema_version=1, records=tuple(source_rows[:3]),
    ))
    LOG.info("重复提交 %s：duplicate=%s 目标行数=%d",
             first.batch_id, dup.duplicate,
             engine_restarted.target.count("orders"))

    # 5) 结构变更：新增 NOT NULL 带缺省列；随后改名
    engine_restarted.evolve_schema(
        "orders", "ADD_COLUMN",
        name="region", type="str", nullable=False, default="CN",
    )
    engine_restarted.evolve_schema(
        "orders", "RENAME_COLUMN", old_name="customer", new_name="buyer",
    )
    LOG.info("结构变更流水：%s",
             [(e.seq, e.kind, e.from_version, e.to_version)
              for e in engine_restarted.registry.history("orders")])

    # 6) 老结构数据继续到达：新列按缺省补，旧列名被明确拒绝
    engine_restarted.source.append(
        "orders", {"order_id": 7, "customer": "erin"}, schema_version=2)
    engine_restarted.source.append(
        "orders", {"order_id": 8, "buyer": "frank", "amount": 1.0},
        schema_version=3)
    tail = engine_restarted.pull_and_ingest("orders", max_records=10)
    LOG.info("结构变更后接入：accepted=%s rejected=%s",
             tail.accepted_offsets,
             [(r.offset, r.reason) for r in tail.rejected])

    # 7) 异常位点演示
    for label, start in (("回退", 0), ("越界", 999)):
        try:
            engine_restarted.submit_batch(Batch(
                batch_id=f"bad-{label}", table="orders", start_offset=start,
                schema_version=3,
                records=({"order_id": 100, "buyer": "x", "region": "CN"},),
            ))
        except IngestionError as e:
            LOG.info("位点%s被拒绝：code=%s message=%s", label, e.code, e)

    # 8) 最终状态汇总
    LOG.info("最终位点 next_offset=%s",
             engine_restarted.checkpoints.get("orders"))
    LOG.info("目标端数据：%s",
             engine_restarted.target.all_rows("orders"))
    LOG.info("批次台账文件：%s",
             sorted(os.listdir(os.path.join(DATA_DIR, "ledger"))))


if __name__ == "__main__":
    main()
