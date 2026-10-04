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


class BusinessAndCorrectionApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        pipeline = Pipeline(root)
        self.source = LocalSource(
            JsonlStore(f"{root}/source.jsonl", lock=pipeline._store_lock))
        self.client = TestClient(create_app(pipeline, self.source))
        for table, col in [("orders", "amount"), ("details", "qty")]:
            r = self.client.post(f"/tables/{table}/init", json={
                "columns": [{"name": "id", "typ": "int"},
                            {"name": col, "typ": "int"}]})
            self.assertEqual(r.status_code, 200)
        # 一个跨表业务批次
        r = self.client.post("/business-batches", json={
            "business_batch_id": "set-1", "entries": [
                {"table": "orders", "schema_version": 1,
                 "records": [{"offset": 1, "data": {"id": 1, "amount": 100}}]},
                {"table": "details", "schema_version": 1,
                 "records": [{"offset": 1, "data": {"id": 1, "qty": 2}}]}]})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["committed"])

    def tearDown(self):
        self._tmp.cleanup()

    def test_cross_table_failure_returns_locating_409(self):
        r = self.client.post("/business-batches", json={
            "business_batch_id": "set-bad", "entries": [
                {"table": "orders", "schema_version": 1,
                 "records": [{"offset": 2, "data": {"id": 2, "amount": 1}}]},
                {"table": "details", "schema_version": 1,
                 "records": [{"offset": 2, "data": {"id": 2, "qty": "X"}}]}]})
        self.assertEqual(r.status_code, 409)
        failure = r.json()["detail"]["failures"][0]
        self.assertEqual(failure["table"], "details")
        self.assertEqual(failure["reason"], "TYPE_MISMATCH")
        self.assertEqual(failure["offset"], 2)
        # 未半生效：orders 快照仍只有 1 行
        self.assertEqual(len(self.client.get("/tables/orders").json()["rows"]), 1)

    def test_correction_withdraw_state_and_history_over_http(self):
        # 更正重复提交回放
        body = {"correction_id": "c1", "items": [
            {"table": "orders", "key": 1, "op": "UPSERT", "schema_version": 1,
             "data": {"id": 1, "amount": 999}}]}
        self.assertFalse(self.client.post("/corrections", json=body).json()["replay"])
        self.assertTrue(self.client.post("/corrections", json=body).json()["replay"])
        state = self.client.get("/tables/orders/state").json()["state"]
        self.assertEqual(state, {"1": {"id": 1, "amount": 999}})

        # 撤回 details 1 后当前状态消失，但历史仍可解释
        r = self.client.post("/corrections", json={
            "correction_id": "w1", "items": [
                {"table": "details", "key": 1, "op": "WITHDRAW",
                 "schema_version": 1}]})
        self.assertTrue(r.json()["committed"])
        self.assertEqual(self.client.get("/tables/details/state").json()["state"],
                         {})
        hist = self.client.get(
            "/tables/details/state/history?key=1").json()["history"]
        self.assertEqual([e["kind"] for e in hist], ["UPSERT", "WITHDRAW"])

        # 撤回不存在的键：409 + KEY_NOT_FOUND，事务留痕为 ABORTED
        r = self.client.post("/corrections", json={
            "correction_id": "w2", "items": [
                {"table": "details", "key": 99, "op": "WITHDRAW",
                 "schema_version": 1}]})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["detail"]["failures"][0]["reason"],
                         "KEY_NOT_FOUND")
        self.assertEqual(self.client.get("/transactions/w2").json()["state"],
                         "ABORTED")


class BugfixApiTest(unittest.TestCase):
    """bugfix-b 新增 HTTP 语义：发起序号裁决、DDL 战役、业务键列保护、改名定位。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        pipeline = Pipeline(root)
        self.source = LocalSource(
            JsonlStore(f"{root}/source.jsonl", lock=pipeline._store_lock))
        self.client = TestClient(create_app(pipeline, self.source))
        self.client.post("/tables/orders/init", json={
            "columns": [{"name": "settle_id", "typ": "str"},
                        {"name": "amount", "typ": "int"}],
            "key_col": "settle_id"})
        self.client.post("/tables/details/init", json={
            "columns": [{"name": "settle_id", "typ": "str"},
                        {"name": "qty", "typ": "int"}],
            "key_col": "settle_id"})
        self.client.post("/business-batches", json={
            "business_batch_id": "S1", "entries": [
                {"table": "orders", "schema_version": 1,
                 "records": [{"offset": 1,
                              "data": {"settle_id": "S1", "amount": 100}}]},
                {"table": "details", "schema_version": 1,
                 "records": [{"offset": 1,
                              "data": {"settle_id": "S1", "qty": 2}}]}]})

    def tearDown(self):
        self._tmp.cleanup()

    def test_stale_issued_seq_returns_409_and_keeps_newer(self):
        r1 = self.client.post("/corrections", json={
            "correction_id": "new", "items": [
                {"table": "orders", "key": "S1", "op": "UPSERT",
                 "schema_version": 1, "issued_seq": 10,
                 "data": {"settle_id": "S1", "amount": 150}}]})
        self.assertEqual(r1.status_code, 200)
        r2 = self.client.post("/corrections", json={
            "correction_id": "old", "items": [
                {"table": "orders", "key": "S1", "op": "UPSERT",
                 "schema_version": 1, "issued_seq": 5,
                 "data": {"settle_id": "S1", "amount": 110}}]})
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(r2.json()["detail"]["failures"][0]["reason"],
                         "STALE_CORRECTION")
        state = self.client.get("/tables/orders/state").json()["state"]
        self.assertEqual(state["S1"]["amount"], 150)

    def test_drop_key_column_returns_distinguishable_409(self):
        r = self.client.post("/tables/orders/schema", json={
            "change_type": "drop_column", "column_name": "settle_id"})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["detail"]["reason"],
                         "KEY_COLUMN_PROTECTED")

    def test_rename_key_column_then_state_and_correction(self):
        r = self.client.post("/tables/orders/schema", json={
            "change_type": "rename_column",
            "column_name": "settle_id", "to_name": "biz_id"})
        self.assertEqual(r.status_code, 200)
        # 老结果改名后仍可按原业务键值查到，并可用新列名更正
        state = self.client.get("/tables/orders/state").json()["state"]
        self.assertIn("S1", state)
        rc = self.client.post("/corrections", json={
            "correction_id": "fix", "items": [
                {"table": "orders", "key": "S1", "op": "UPSERT",
                 "schema_version": 2,
                 "data": {"biz_id": "S1", "amount": 120}}]})
        self.assertEqual(rc.status_code, 200)
        state = self.client.get("/tables/orders/state").json()["state"]
        self.assertEqual(state["S1"]["amount"], 120)
        self.assertEqual(state["S1"]["biz_id"], "S1")

    def test_ddl_campaign_atomic_and_idempotent(self):
        body = {"campaign_id": "ddl1", "changes": [
            {"table": "orders", "change_type": "add_column",
             "column_name": "tag",
             "column": {"name": "tag", "typ": "str", "required": False}},
            {"table": "details", "change_type": "add_column",
             "column_name": "tag",
             "column": {"name": "tag", "typ": "str", "required": False}}]}
        r = self.client.post("/schema-campaigns", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(
            sorted((c["table"], c["schema_version"])
                   for c in r.json()["changes"]),
            [("details", 2), ("orders", 2)])
        # 重复提交：幂等回放，版本不再增长
        r2 = self.client.post("/schema-campaigns", json=body)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(
            sorted(c["schema_version"] for c in r2.json()["changes"]), [2, 2])

    def test_ddl_campaign_drop_key_column_aborts_all(self):
        r = self.client.post("/schema-campaigns", json={
            "campaign_id": "ddlbad", "changes": [
                {"table": "orders", "change_type": "drop_column",
                 "column_name": "settle_id"},
                {"table": "details", "change_type": "add_column",
                 "column_name": "tag",
                 "column": {"name": "tag", "typ": "str", "required": False}}]})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["detail"]["reason"],
                         "KEY_COLUMN_PROTECTED")
        # details 也未生效
        snap = self.client.get("/tables/details").json()
        self.assertEqual(snap["schema_version"], 1)
