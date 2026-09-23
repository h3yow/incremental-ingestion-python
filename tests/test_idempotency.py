"""重复提交与部分失败：幂等、拒绝原因可区分、成功部分可追溯。"""
from __future__ import annotations

from ingestion.errors import BatchConflictError
from ingestion.models import Batch
from tests.harness import IngestionHarness


def _batch(bid: str, start: int, *records: dict, schema_version: int = 1) -> Batch:
    return Batch(bid, "users", start, schema_version, records)


class IdempotencyTests(IngestionHarness):
    def test_duplicate_submit_does_not_write_twice(self) -> None:
        b = _batch("b1", 0, {"id": 1, "name": "a"}, {"id": 2, "name": "b"})
        r1 = self.engine.submit_batch(b)
        self.assertEqual(r1.accepted_offsets, (0, 1))
        self.assertEqual(self.engine.target.count("users"), 2)

        # 同一 batch_id 原封不动重提：返回既有结论，目标端无新增
        r2 = self.engine.submit_batch(b)
        self.assertTrue(r2.duplicate)
        self.assertEqual(r2.accepted_offsets, (0, 1))
        self.assertEqual(r2.rejected, r1.rejected)
        self.assertEqual(self.engine.target.count("users"), 2)
        self.assert_cp(2)

    def test_same_batch_id_with_different_content_is_conflict(self) -> None:
        self.engine.submit_batch(_batch("b1", 0, {"id": 1, "name": "a"}))
        with self.assertRaises(BatchConflictError) as ctx:
            self.engine.submit_batch(_batch("b1", 0, {"id": 99, "name": "other"}))
        self.assertEqual(ctx.exception.code, "BATCH_CONFLICT")

    def test_partial_failure_is_not_silent(self) -> None:
        b = _batch(
            "b1", 0,
            {"id": 1, "name": "ok", "age": 30},          # 接受
            {"id": 2, "name": "ok-null-age"},            # 接受：nullable age -> None
            {"id": 3, "age": 10},                        # 拒绝：NOT NULL name 缺失
            {"id": 4, "name": None, "age": 1},           # 拒绝：name 显式 null
            {"id": "x", "name": "bad-type"},             # 拒绝：主键类型不符
            {"id": 5, "name": "ok2", "ghost": 1},        # 拒绝：未知列
        )
        r = self.engine.submit_batch(b)
        self.assertEqual(r.accepted_offsets, (0, 1))
        reasons = {x.offset: x.reason for x in r.rejected}
        self.assertEqual(
            reasons,
            {2: "MISSING_COLUMN", 3: "NULL_NOT_ALLOWED",
             4: "TYPE_MISMATCH", 5: "UNKNOWN_COLUMN"},
        )
        # 成功部分真实落地、可查询
        self.assertEqual(self.engine.target.count("users"), 2)
        self.assertIsNone(self.engine.target.get("users", 2)["age"])
        # 拒绝部分进入不可变台账，可追溯
        got = self.engine.ledger.get("b1")
        self.assertIsNotNone(got)
        self.assertEqual(len(got.rejected), 4)
        # 位点仍推进（拒绝记录有台账，不阻塞后续）
        self.assert_cp(6)
        # 重提同一批次：结论一致，目标端仍只有 2 行
        r2 = self.engine.submit_batch(b)
        self.assertTrue(r2.duplicate)
        self.assertEqual(self.engine.target.count("users"), 2)

    def test_missing_key_record_rejected(self) -> None:
        r = self.engine.submit_batch(
            _batch("b1", 0, {"name": "no-id"}),
        )
        self.assertEqual(r.rejected[0].reason, "MISSING_KEY")
        self.assertEqual(r.accepted_count, 0)
        self.assertEqual(self.engine.target.count("users"), 0)
