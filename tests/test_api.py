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


class BusinessBatchApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        pipeline = Pipeline(root)
        self.source = LocalSource(
            JsonlStore(f"{root}/source.jsonl", lock=pipeline._store_lock))
        self.client = TestClient(create_app(pipeline, self.source))
        self.pipeline = pipeline
        for table, cols, keys in [
            ("so", [{"name": "order_id", "typ": "int"},
                    {"name": "amount", "typ": "int"}], ["order_id"]),
            ("si", [{"name": "order_id", "typ": "int"},
                    {"name": "sku", "typ": "str"},
                    {"name": "qty", "typ": "int"}], ["order_id", "sku"]),
        ]:
            r = self.client.post(f"/tables/{table}/init",
                                 json={"columns": cols, "key_columns": keys})
            self.assertEqual(r.status_code, 200, r.text)

    def tearDown(self):
        self._tmp.cleanup()

    def test_cross_table_commit_replay_and_failure_over_http(self):
        body = {"batch_id": "s1", "schema_version": 1, "ops": [
            {"table": "so", "op_type": "UPSERT", "key": [1], "revision": 1,
             "data": {"order_id": 1, "amount": 100}},
            {"table": "si", "op_type": "UPSERT", "key": [1, "A"], "revision": 1,
             "data": {"order_id": 1, "sku": "A", "qty": 2}},
        ]}
        r1 = self.client.post("/business-batches", json=body)
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r1.json()["sequence"], 1)
        # 重复提交 -> 回放
        r2 = self.client.post("/business-batches", json=body)
        self.assertTrue(r2.json()["replay"])
        self.assertEqual(r2.json()["sequence"], 1)

        # 明细表失败：409，且定位到业务批次/表/键/原因，订单表无数据
        bad = {"batch_id": "s-bad", "schema_version": 1, "ops": [
            {"table": "so", "op_type": "UPSERT", "key": [2], "revision": 1,
             "data": {"order_id": 2, "amount": 50}},
            {"table": "si", "op_type": "UPSERT", "key": [2, "A"], "revision": 1,
             "data": {"order_id": 2, "sku": "A", "qty": "X"}},
        ]}
        rj = self.client.post("/business-batches", json=bad)
        self.assertEqual(rj.status_code, 409)
        detail = rj.json()["detail"]
        self.assertEqual(detail["reason"], "TYPE_MISMATCH")
        self.assertEqual(detail["batch_id"], "s-bad")
        self.assertEqual(detail["table"], "si")
        self.assertEqual(detail["key"], [2, "A"])
        self.assertEqual(self.client.get("/business-batches/s-bad").status_code, 404)

    def test_correction_retract_current_and_history_over_http(self):
        def upsert(bid, rev, amount):
            return self.client.post("/business-batches", json={
                "batch_id": bid, "schema_version": 1, "ops": [
                    {"table": "so", "op_type": "UPSERT", "key": [1],
                     "revision": rev, "data": {"order_id": 1, "amount": amount}}]})

        upsert("b1", 1, 100)
        upsert("b2", 2, 120)
        cur = self.client.get("/tables/so/business/current?key=[1]")
        self.assertEqual(cur.json()["data"]["amount"], 120)

        # 旧 revision 翻案 -> 409 STALE_REVISION
        stale = upsert("old", 1, 1)
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.json()["detail"]["reason"], "STALE_REVISION")

        # 撤回
        r = self.client.post("/business-batches", json={
            "batch_id": "r1", "schema_version": 1, "ops": [
                {"table": "so", "op_type": "RETRACT", "key": [1], "revision": 3}]})
        self.assertEqual(r.status_code, 200, r.text)
        cur_after = self.client.get("/tables/so/business/current?key=[1]").json()
        self.assertEqual(cur_after["status"], "RETRACT")
        self.assertIsNone(cur_after["data"])
        # 历史轨迹完整可解释
        hist = self.client.get("/tables/so/business/history?key=[1]").json()
        self.assertEqual([h["revision"] for h in hist["history"]], [1, 2, 3])
        self.assertEqual([h["status"] for h in hist["history"]],
                         ["UPSERT", "UPSERT", "RETRACT"])


if __name__ == "__main__":
    unittest.main()