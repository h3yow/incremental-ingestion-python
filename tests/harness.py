"""测试共享夹具：构建一套完全隔离的本地接入环境，并打印批次/位点/结构流水日志。"""
from __future__ import annotations

import logging
import os
import shutil
import tempfile
import unittest

from ingestion.engine import IngestionEngine
from ingestion.ledger import BatchLedger
from ingestion.models import Column
from ingestion.offsets import CheckpointManager
from ingestion.schema import SchemaRegistry
from ingestion.storage import LocalSource, LocalTarget

# 单测日志：批次、位点、结构变更流水必须可见
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def make_engine(data_dir: str) -> IngestionEngine:
    reg = SchemaRegistry(os.path.join(data_dir, "schema"))
    return IngestionEngine(
        registry=reg,
        source=LocalSource(os.path.join(data_dir, "source")),
        target=LocalTarget(os.path.join(data_dir, "target")),
        checkpoints=CheckpointManager(os.path.join(data_dir, "checkpoint.json")),
        ledger=BatchLedger(os.path.join(data_dir, "ledger")),
        lock_dir=os.path.join(data_dir, "locks"),
    )


class IngestionHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="ing-test-")
        self.engine = make_engine(self.dir)
        reg = self.engine.registry
        # 标准表：id 主键 NOT NULL，name NOT NULL 无缺省，age nullable
        reg.create_table(
            "users", "id",
            [Column("id", "int", False),
             Column("name", "str", False),
             Column("age", "int", True)],
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    # -- 便捷断言 --
    def assert_cp(self, expected: int, table: str = "users") -> None:
        cp = self.engine.checkpoints.get(table)
        self.assertEqual(cp, expected, f"位点应为 {expected}，实际 {cp}")
