"""结构变更兼容：新增列缺省语义、删列/改名不错位、并发一致性。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from ingestion.errors import IncompatibleSchemaChangeError, SchemaVersionAheadError
from ingestion.models import Batch
from tests.harness import IngestionHarness


def _batch(bid: str, start: int, sv: int, *records: dict) -> Batch:
    return Batch(bid, "users", start, sv, records)


class SchemaEvolutionTests(IngestionHarness):
    def test_add_nullable_column_defaults_to_none(self) -> None:
        self.engine.submit_batch(_batch("b1", 0, 1, {"id": 1, "name": "a"}))
        self.engine.evolve_schema("users", "ADD_COLUMN",
                                  name="nick", type="str", nullable=True)
        # 老结构数据补入：新增列稳定缺省 None
        r = self.engine.submit_batch(_batch("b2", 1, 1, {"id": 2, "name": "b"}))
        self.assertEqual(r.applied_schema_version, 2)
        self.assertIsNone(self.engine.target.get("users", 2)["nick"])

    def test_add_not_null_column_requires_default_and_is_stable(self) -> None:
        # 不带 default 的 NOT NULL 列：变更本身被拒绝
        with self.assertRaises(IncompatibleSchemaChangeError) as ctx:
            self.engine.evolve_schema("users", "ADD_COLUMN",
                                      name="region", type="str", nullable=False)
        self.assertEqual(ctx.exception.code, "INCOMPATIBLE_SCHEMA_CHANGE")
        # 带显式 default：历史版本数据按 default 补齐
        self.engine.evolve_schema(
            "users", "ADD_COLUMN",
            name="region", type="str", nullable=False, default="CN",
        )
        self.engine.submit_batch(_batch("b1", 0, 1, {"id": 1, "name": "a"}))
        self.assertEqual(self.engine.target.get("users", 1)["region"], "CN")
        # 显式给值按值落
        self.engine.submit_batch(
            _batch("b2", 1, 2, {"id": 2, "name": "b", "region": "US"})
        )
        self.assertEqual(self.engine.target.get("users", 2)["region"], "US")

    def test_drop_column_does_not_shift_fields(self) -> None:
        self.engine.evolve_schema("users", "ADD_COLUMN",
                                  name="region", type="str", default="CN",
                                  **{"nullable": True})
        self.engine.evolve_schema("users", "DROP_COLUMN", name="region")
        # 携带已删字段的旧记录：整条拒绝（UNKNOWN_COLUMN），其余列不错位
        r = self.engine.submit_batch(
            _batch("b1", 0, 1, {"id": 1, "name": "a", "region": "CN"})
        )
        self.assertEqual(r.rejected[0].reason, "UNKNOWN_COLUMN")
        self.assertEqual(self.engine.target.count("users"), 0)
        # 干净记录正常落；缺失的 nullable age 按稳定缺省补 None；
        # 目标行绝不含被删列 region（按列名映射，无位置错位）
        r2 = self.engine.submit_batch(_batch("b2", 1, 2, {"id": 2, "name": "b"}))
        self.assertEqual(r2.accepted_offsets, (1,))
        self.assertEqual(
            self.engine.target.get("users", 2),
            {"id": 2, "name": "b", "age": None},
        )
        self.assertNotIn("region", self.engine.target.get("users", 2))

    def test_rename_column_old_name_rejected_new_name_mapped(self) -> None:
        self.engine.evolve_schema("users", "RENAME_COLUMN",
                                  old_name="name", new_name="nick")
        schema = self.engine.registry.current("users")
        self.assertEqual(schema.names(), ("id", "nick", "age"))
        # 旧字段名：UNKNOWN_COLUMN，不会把值错写到别的列
        r = self.engine.submit_batch(
            _batch("b1", 0, 1, {"id": 1, "name": "old"})
        )
        self.assertEqual(r.rejected[0].reason, "UNKNOWN_COLUMN")
        # 新字段名正确映射
        self.engine.submit_batch(_batch("b2", 1, 2, {"id": 2, "nick": "new"}))
        self.assertEqual(self.engine.target.get("users", 2)["nick"], "new")

    def test_ahead_schema_version_rejected(self) -> None:
        with self.assertRaises(SchemaVersionAheadError):
            self.engine.submit_batch(
                _batch("b1", 0, 99, {"id": 1, "name": "a"})
            )

    def test_change_history_is_a_versioned_ledger(self) -> None:
        self.engine.evolve_schema("users", "ADD_COLUMN",
                                  name="region", type="str", nullable=True)
        self.engine.evolve_schema("users", "RENAME_COLUMN",
                                  old_name="name", new_name="nick")
        events = self.engine.registry.history("users")
        kinds = [(e.seq, e.kind, e.from_version, e.to_version) for e in events]
        self.assertEqual(kinds[0][1], "CREATE")
        self.assertIn(("ADD_COLUMN", 1, 2), [(k[1], k[2], k[3]) for k in kinds])
        self.assertIn(("RENAME_COLUMN", 2, 3), [(k[1], k[2], k[3]) for k in kinds])
        # 历史版本仍可取回，供审计与重放解释
        v1 = self.engine.registry.get_version("users", 1)
        self.assertEqual(v1.names(), ("id", "name", "age"))


class ConcurrencyTests(IngestionHarness):
    def test_concurrent_submit_same_batch_only_commits_once(self) -> None:
        batch = _batch("b1", 0, 1, {"id": 1, "name": "a"})
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.engine.submit_batch(batch), range(8)))
        # 恰好 1 次首次提交，7 次重复返回；目标端只有 1 行
        self.assertEqual(sum(1 for r in results if not r.duplicate), 1)
        self.assertEqual(sum(1 for r in results if r.duplicate), 7)
        self.assertEqual(self.engine.target.count("users"), 1)
        self.assert_cp(1)

    def test_concurrent_commit_and_schema_change_is_serialized(self) -> None:
        # 批次落位点 0；同时提交结构变更。无论谁先拿锁，结论都必须自洽。
        batch = _batch("b1", 0, 1, {"id": 1, "name": "a"})
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_batch = pool.submit(self.engine.submit_batch, batch)
            fut_evolve = pool.submit(
                self.engine.evolve_schema, "users", "ADD_COLUMN",
                name="region", type="str", nullable=True,
            )
            result = fut_batch.result()
            evolved = fut_evolve.result()

        # 结构变更必然成功，批次也必然成功且可解释应用了哪个版本
        self.assertEqual(evolved.version, 2)
        self.assertIn(result.applied_schema_version, (1, 2))
        self.assertEqual(self.engine.target.count("users"), 1)
        self.assert_cp(1)
        # 串行化下批次之后再提一条，必须严格紧邻位点、不会因并发产生空洞
        nxt = self.engine.submit_batch(
            _batch("b2", 1, 2, {"id": 2, "name": "b"})
        )
        self.assertEqual(nxt.accepted_offsets, (1,))
        self.assert_cp(2)

    def test_duplicate_requests_racing_with_evolve_have_consistent_verdict(self) -> None:
        batch = _batch("b1", 0, 1, {"id": 1, "name": "a"})
        with ThreadPoolExecutor(max_workers=3) as pool:
            f1 = pool.submit(self.engine.submit_batch, batch)
            f2 = pool.submit(self.engine.submit_batch, batch)
            f3 = pool.submit(
                self.engine.evolve_schema, "users", "ADD_COLUMN",
                name="region", type="str", nullable=True,
            )
            r1, r2 = f1.result(), f2.result()
            f3.result()
        # 两次提交看到的结论必须完全一致（同一不可变台账记录）
        self.assertEqual(r1.accepted_offsets, r2.accepted_offsets)
        self.assertEqual(r1.applied_schema_version, r2.applied_schema_version)
        self.assertEqual(r1.fingerprint, r2.fingerprint)
        self.assertEqual(self.engine.target.count("users"), 1)
