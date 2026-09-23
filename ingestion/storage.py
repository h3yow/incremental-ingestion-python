"""本地存储：模拟源端（LocalSource）与目标端（LocalTarget）。

- 源端：每张表一个 JSONL，每行 {"offset": n, "schema_version": v, "data": {...}}，
  offset 从 0 开始、单调连续追加，不可修改，模拟日志型源。
- 目标端：每张表一个 JSON，{主键: {列: 值}}；upsert 按主键幂等，
  写入采用 temp + os.replace 原子替换，避免读到半截文件。
"""
from __future__ import annotations

import json
import os
import tempfile


class LocalSource:
    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(dir_path, exist_ok=True)

    def _path(self, table: str) -> str:
        return os.path.join(self.dir_path, f"{table}.jsonl")

    def append(self, table: str, data: dict[str, object], schema_version: int) -> int:
        """追加一条源记录，返回分配的 offset（从 0 开始）。"""
        offset = self.next_offset(table)
        with open(self._path(table), "a", encoding="utf-8") as f:
            f.write(json.dumps(
                {"offset": offset, "schema_version": schema_version, "data": data},
                ensure_ascii=False,
            ) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return offset

    def _rows(self, table: str) -> list[dict]:
        p = self._path(table)
        if not os.path.exists(p):
            return []
        with open(p, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def read(self, table: str, start_offset: int, limit: int) -> list[tuple[int, int, dict[str, object]]]:
        out: list[tuple[int, int, dict[str, object]]] = []
        for row in self._rows(table):
            if row["offset"] < start_offset:
                continue
            if len(out) >= limit:
                break
            out.append((row["offset"], row["schema_version"], row["data"]))
        return out

    def next_offset(self, table: str) -> int:
        rows = self._rows(table)
        return rows[-1]["offset"] + 1 if rows else 0


class LocalTarget:
    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(dir_path, exist_ok=True)

    def _path(self, table: str) -> str:
        return os.path.join(self.dir_path, f"{table}.json")

    def _load(self, table: str) -> dict[str, dict[str, object]]:
        p = self._path(table)
        if not os.path.exists(p):
            return {}
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    def _store(self, table: str, data: dict[str, dict[str, object]]) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.dir_path, prefix=f".{table}-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path(table))
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def upsert(self, table: str, key: object, row: dict[str, object]) -> None:
        """写入/覆盖一行（按主键幂等；重放同一记录不会产生第二行）。"""
        data = self._load(table)
        data[str(key)] = row
        self._store(table, data)

    def get(self, table: str, key: object) -> dict[str, object] | None:
        return self._load(table).get(str(key))

    def all_rows(self, table: str) -> dict[str, dict[str, object]]:
        return self._load(table)

    def count(self, table: str) -> int:
        return len(self._load(table))
