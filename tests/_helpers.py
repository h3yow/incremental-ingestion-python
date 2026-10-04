"""测试公共工具：临时目录 + 统一日志（打印批次、位点、变更流水）。"""

import logging
import tempfile
import unittest

from app.models import Column
from app.pipeline import Pipeline
from app.source import LocalSource
from app.storage import JsonlStore


def configure_logging() -> None:
    logger = logging.getLogger("ingestion")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


configure_logging()


class IngestionCase(unittest.TestCase):
    def setUp(self) -> None:
        # pylint: disable=consider-using-with
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.pipeline = Pipeline(self.root)
        self.source = LocalSource(
            JsonlStore(f"{self.root}/source.jsonl",
                       lock=self.pipeline._store_lock))
        self.cols = [Column("id", "int"), Column("name", "str")]
        # 业务键显式取 name：删列用例演示的是“删除普通列”，
        # 业务键列是更正/撤回与当前状态的定位锚点，不允许删除
        self.pipeline.init_table("orders", self.cols, key_col="name")

    def tearDown(self) -> None:
        self._tmp.cleanup()
