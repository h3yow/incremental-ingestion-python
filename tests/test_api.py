"""HTTP 接入层测试：幂等、位点拒绝、结构变更与快照查询。"""

import tempfile
import unittest

from fastapi.testclient import TestClient

from app.api import create_app
from app.pipeline import Pipeline
from app.source import LocalSource
from app.storage import JsonlStore
from tests._helpers import configure_logging  # noqa: F401

configure_logging()


class ApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        pipeline = Pipeline(root)
        self.source = LocalSource(
            JsonlStore(f"{root}/source.jsonl", lock=pipeline._store_lock))
        self.client = TestClient(create_app(pipeline, self.source))
        r = self.client.post("/tables/orders/init", json={
            "columns": [{"name": "id", "typ": "int"},
                        {"name": "name", "typ": "str"}]})
        self.assertEqual(r.status_code, 200)

    def tearDown(self):
        self._tmp.cleanup()

    def test_duplicate_batch_replay_over_http(self):
        body = {"batch_id": "b1", "schema_version": 1,
                "records": [{"offset": 1, "data": {"id": 1, "name": "a"}}]}
        r1 = self.client.post("/tables/orders/batches", json=body)
        r2 = self.client.post("/tables/orders/batches", json=body)
        self.assertFalse(r1.json()["replay"])
        self.assertTrue(r2.json()["replay"], "重复提交应回放")
        self.assertEqual(len(self.client.get("/tables/orders").json()["rows"]), 1)
        # 留痕可查
        trace = self.client.get("/tables/orders/batches/b1")
        self.assertEqual(trace.status_code, 200)
        self.assertEqual(trace.json()["checkpoint_after"], 1)

    def test_offset_violations_return_distinguishable_409(self):
        self.client.post("/tables/orders/batches", json={
            "batch_id": "b1", "schema_version": 1,
            "records": [{"offset": 1, "data": {"id": 1, "name": "a"}}]})
        backward = self.client.post("/tables/orders/batches", json={
            "batch_id": "b2", "schema_version": 1,
            "records": [{"offset": 1, "data": {"id": 9, "name": "dup"}}]})
        self.assertEqual(backward.status_code, 409)
        self.assertEqual(backward.json()["detail"]["reason"], "OFFSET_BACKWARD")

        gap = self.client.post("/tables/orders/batches", json={
            "batch_id": "b3", "schema_version": 1,
            "records": [{"offset": 9, "data": {"id": 9, "name": "jump"}}]})
        self.assertEqual(gap.json()["detail"]["reason"], "OFFSET_GAP")

        ahead = self.client.post("/tables/orders/batches", json={
            "batch_id": "b4", "schema_version": 99,
            "records": [{"offset": 2, "data": {"id": 2, "name": "future"}}]})
        self.assertEqual(ahead.json()["detail"]["reason"],
                         "SCHEMA_VERSION_AHEAD")

    def test_schema_change_then_run_pulls_and_aligns(self):
        for i in range(1, 3):
            self.client.post("/sources/orders",
                             json={"schema_version": 1,
                                   "data": {"id": i, "name": f"n{i}"}})
        # 加一个带稳定缺省的必填列
        r = self.client.post("/tables/orders/schema", json={
            "change_type": "add_column", "column_name": "amount",
            "column": {"name": "amount", "typ": "float",
                       "required": True, "default": 1.5}})
        self.assertEqual(r.json()["schema_version"], 2)
        run = self.client.post("/tables/orders/run",
                               json={"batch_id": "run-1"})
        self.assertEqual(run.json()["accepted_count"], 2)
        rows = self.client.get("/tables/orders").json()["rows"]
        self.assertEqual(rows[0]["amount"], 1.5)
        self.assertNotIn("unknown", rows[0])


if __name__ == "__main__":
    unittest.main()
