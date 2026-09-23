"""源端模拟：本地 JSONL 产生带连续位点与结构版本的记录。

记录形如 {"table", "offset", "schema_version", "data"}，
位点由源端按表单调递增（1 起），重启后仍可按位点范围重读，模拟可回放日志。
"""

from __future__ import annotations

from app.models import Record
from app.storage import JsonlStore


class SourceError(Exception):
    pass


class LocalSource:
    def __init__(self, store: JsonlStore):
        self.store = store

    def _rows(self, table: str) -> list[dict]:
        return [r for r in self.store.read_all() if r["table"] == table]

    def next_offset(self, table: str) -> int:
        rows = self._rows(table)
        return (max((r["offset"] for r in rows), default=0)) + 1

    def produce(self, table: str, data: dict[str, object], schema_version: int) -> Record:
        """写入一条源记录，返回其位点（位点在源端锁内分配）。"""
        with self.store.lock():
            offset = self.next_offset(table)
            self.store.append({
                "table": table, "offset": offset,
                "schema_version": schema_version, "data": data,
            })
            return Record(offset=offset, data=data, schema_version=schema_version)

    def read(self, table: str, start: int, limit: int | None = None) -> list[Record]:
        """从位点 start（含）开始顺序读取；start 必须连续可衔接，越界报错。"""
        rows = self._rows(table)
        if start < 1:
            raise SourceError(f"起始位点非法: {start}")
        records = [Record(offset=r["offset"], data=r["data"],
                          schema_version=r["schema_version"])
                   for r in rows if r["offset"] >= start]
        records.sort(key=lambda r: r.offset)
        # start 落在末尾之后 -> 暂时没有新数据，返回空（等待/正常结束）；
        # start 落在中间空洞（下一条不是 start）-> 明确报错，不允许跳读。
        if rows and start <= max(r["offset"] for r in rows) \
                and records and records[0].offset != start:
            raise SourceError(f"位点 {start} 不存在（下一条为 {records[0].offset}）")
        if limit is not None:
            records = records[:limit]
        return records
