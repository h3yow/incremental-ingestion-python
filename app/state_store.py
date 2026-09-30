"""业务键当前状态存储：在已落地事件之上折叠出「最后一次有效结论」。

事件来源（均在 rows.jsonl 内，按表区分）：
- 增量行：{"table","offset","schema_version","data",?seq}，业务键取业务键列的值；
- 更正事件：{"table","kind","key","seq","schema_version","data","correction_id"}。

确定顺序
--------
每张表维护一条单调写序号 ``write_seq``（seq.json），增量行与更正事件在应用时
于表锁内依次取号；因此所有事件存在唯一全序，折叠规则为：

1. 有 seq 的事件按 seq 升序；无 seq 的历史行（旧版本落盘）按 offset 升序、
   且一律排在有 seq 事件之前（有效序视为 0）。
2. 依次 UPSERT：``state[key] = data``；WITHDRAW：``state.pop(key)``。

当前状态只反映最后一次有效结论；事件不可变、永久保留，history() 可逐条解释。
重复重放通过 (correction_id, table, key) 去重、不会再次取号，因此结论不翻转。
"""

from __future__ import annotations

from app.models import EventKind
from app.storage import JsonlStore, KVStore


class StateStore:
    def __init__(self, rows_store: JsonlStore, seq_kv: KVStore):
        self.rows = rows_store
        self.seq_kv = seq_kv

    # ---- 写序号 ----
    def _key(self, table: str) -> str:
        return f"{table}::write_seq"

    def get_write_seq(self, table: str) -> int:
        value = self.seq_kv.get(self._key(table))
        return 0 if value is None else int(value["write_seq"])

    def allocate_write_seq(self, table: str) -> int:
        """在表锁内分配下一个写序号（调用方负责串行化）。"""
        nxt = self.get_write_seq(table) + 1
        self.seq_kv.put(self._key(table), {"write_seq": nxt})
        return nxt

    # ---- 更正事件 ----
    def append_event(self, table: str, key: object, kind: EventKind,
                     data: dict | None, schema_version: int, seq: int,
                     correction_id: str) -> None:
        """追加一条更正/撤回事件（不可变留痕）。"""
        self.rows.append({
            "table": table,
            "kind": kind.value,
            "key": key,
            "seq": seq,
            "schema_version": schema_version,
            "data": data,
            "correction_id": correction_id,
        })

    def has_event(self, correction_id: str, table: str, key: object) -> bool:
        """某条更正事件是否已应用（崩溃重放去重）。"""
        return any(
            r.get("kind") is not None
            and r.get("correction_id") == correction_id
            and r.get("table") == table and r.get("key") == key
            for r in self.rows.read_all())

    # ---- 折叠 ----
    @staticmethod
    def _is_correction(row: dict) -> bool:
        return row.get("kind") in (EventKind.UPSERT.value,
                                   EventKind.WITHDRAW.value)

    def _ordered_events(self, table: str) -> list[dict]:
        rows = [r for r in self.rows.read_all() if r["table"] == table]
        with_seq = [r for r in rows if "seq" in r]
        without_seq = [r for r in rows if "seq" not in r]
        with_seq.sort(key=lambda r: (r["seq"], r.get("offset", 0)))
        without_seq.sort(key=lambda r: r.get("offset", 0))
        # 无序号历史行视为最早发生，之后一律按全序 seq 折叠
        return without_seq + with_seq

    def current(self, table: str, key_col: str) -> dict[object, dict]:
        """折叠出该表当前所有有效业务键 -> 数据（不含已撤回）。"""
        state: dict[object, dict] = {}
        for r in self._ordered_events(table):
            if self._is_correction(r):
                key = r["key"]
                if r["kind"] == EventKind.WITHDRAW.value:
                    state.pop(key, None)
                else:
                    state[key] = dict(r["data"])
            else:
                # 增量行：按业务键列折叠（同一键的新版本覆盖旧版本）
                key = r["data"].get(key_col)
                if key is None:
                    continue
                state[key] = dict(r["data"])
        return state

    def history(self, table: str, key: object, key_col: str) -> list[dict]:
        """返回某业务键经历过的全部事件（增量写入 + 各次更正/撤回），按全序排列。"""
        out: list[dict] = []
        for r in self._ordered_events(table):
            if self._is_correction(r):
                if r["key"] != key:
                    continue
                out.append({
                    "kind": r["kind"],
                    "seq": r["seq"],
                    "schema_version": r["schema_version"],
                    "data": None if r["data"] is None else dict(r["data"]),
                    "correction_id": r["correction_id"],
                })
            elif r["data"].get(key_col) == key:
                out.append({
                    "kind": EventKind.UPSERT.value,
                    "seq": r.get("seq"),
                    "offset": r["offset"],
                    "schema_version": r["schema_version"],
                    "data": dict(r["data"]),
                    "correction_id": None,
                })
        return out
