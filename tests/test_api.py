"""HTTP 接入协议端到端测试（FastAPI TestClient）。"""
from __future__ import annotations

import shutil
import tempfile
import unittest
import warnings

from fastapi.testclient import TestClient

from ingestion.api import create_app
from tests.harness import make_engine

warnings.filterwarnings("ignore", category=DeprecationWarning)


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        # 用独立空目录，建表动作本身也由 HTTP 用例覆盖
        self.dir = tempfile.mkdtemp(prefix="ing-api-")
        make_engine(self.dir)
        self.client = TestClient(create_app(self.dir))

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_full_protocol_over_http(self) -> None:
        c = self.client
        # 建表
        resp = c.post("/tables/users/schema", json={
            "key_column": "id",
            "columns": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "name", "type": "str", "nullable": False},
            ],
        })
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["version"], 1)

        # 源端先写入两条（offset 空间以源端日志为准，推/拉共用）
        c.post("/sources/users", json={"data": {"id": 1, "name": "a"},
                                       "schema_version": 1})
        c.post("/sources/users", json={"data": {"id": 2},
                                       "schema_version": 1})
        # 提交批次（推模式）：1 条成功 1 条拒绝
        resp = c.post("/batches", json={
            "batch_id": "b1", "table": "users", "start_offset": 0,
            "schema_version": 1,
            "records": [{"id": 1, "name": "a"}, {"id": 2}],
        })
        body = resp.json()
        self.assertEqual(body["accepted_offsets"], [0])
        self.assertEqual(body["rejected"][0]["reason"], "MISSING_COLUMN")

        # 重复提交：duplicate=true
        resp = c.post("/batches", json={
            "batch_id": "b1", "table": "users", "start_offset": 0,
            "schema_version": 1,
            "records": [{"id": 1, "name": "a"}, {"id": 2}],
        })
        self.assertTrue(resp.json()["duplicate"])

        # 位点查询
        self.assertEqual(c.get("/tables/users/checkpoint").json()["next_offset"], 2)

        # 位点回退：稳定错误码
        resp = c.post("/batches", json={
            "batch_id": "bx", "table": "users", "start_offset": 0,
            "schema_version": 1, "records": [{"id": 9, "name": "z"}],
        })
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.json()["error"], "OFFSET_BACKWARD")

        # 结构变更 + 流水
        resp = c.post("/tables/users/evolve", json={
            "kind": "ADD_COLUMN", "name": "region",
            "type": "str", "nullable": False, "default": "CN",
        })
        self.assertEqual(resp.json()["version"], 2)
        hist = c.get("/tables/users/schema").json()["history"]
        self.assertTrue(any(e["kind"] == "ADD_COLUMN" for e in hist))

        # 源端写入 -> 断点续跑
        c.post("/sources/users", json={"data": {"id": 3, "name": "c"}})
        c.post("/sources/users", json={"data": {"id": 4, "name": "d"}})
        resp = c.post("/ingest?table=users")
        self.assertEqual(len(resp.json()["accepted_offsets"]), 2)
        rows = c.get("/tables/users/rows").json()
        self.assertEqual(rows["3"]["region"], "CN")
        self.assertEqual(c.get("/tables/users/checkpoint").json()["next_offset"], 4)

        # 批次结论可追溯
        got = c.get("/batches/b1").json()
        self.assertEqual(got["rejected"][0]["reason"], "MISSING_COLUMN")
