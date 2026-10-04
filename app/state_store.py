"""业务键当前状态存储：在已落地事件之上折叠出「最后一次有效结论」。

事件来源（均在 rows.jsonl 内，按表区分）：
- 增量行：{"table","offset","schema_version","data",?seq}，业务键取业务键列的值；
- 更正事件：{"table","kind","key","seq","schema_version","data","correction_id"}。

确定顺序
--------
每张表维护一条单调写序号 ``write_seq``（seq.json），增量行与更正事件在应用时
于表锁内依次取号；因此所有事件存在唯一全序。折叠规则：

1. 每个键的**生效结论**按「更正/撤回的发起先后」确定：
   - 更正/撤回事件可携带上游发起时间 ``issued_at``（逻辑时钟），
     排序键为 ``(issued_at, seq)``——发起更晚的结论生效，
     更旧的请求即使后送达也不能把结论翻回旧值（仍留痕、可审计）；
   - 不携带 ``issued_at`` 的事件（含增量行与旧式更正）视为发起时间最旧（-1），
     同档内按写序号 ``seq``（即到达顺序）定先后——与引入该语义前的行为一致。
2. 无 seq 的历史行（旧版本落盘）按 offset 升序、且一律排在有 seq 事件之前
   （有效序视为 0）。

业务键定位
----------
业务键列可被改名（RENAME）：键的身份不变，但它在不同版本数据里的名字不同。
折叠时按**每一行自身的 schema_version** 解析该版本下业务键列的名字
（构造时注入 ``key_col_at`` 解析器），因此改名后历史行与新行都能正确定位，
不会出现「接进来了却查不到」。

当前状态只反映最后一次有效结论；事件不可变、永久保留，history() 可逐条解释。
重复重放通过 (correction_id, table, key) 去重、不会再次取号，因此结论不翻转。
"""

from __future__ import annotations

from app.models import EventKind
from app.storage import JsonlStore, KVStore


class StateStore:
    def __init__(self, rows_store: JsonlStore, seq_kv: KVStore,
                 key_col_at=None):
        self.rows = rows_store
        self.seq_kv = seq_kv
        # (table, schema_version) -> 该版本下业务键列名；缺省 None 表示不做版本解析
        self._key_col_at = key_col_at

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
                     correction_id: str,
                     issued_at: int | None = None) -> None:
        """追加一条更正/撤回事件（不可变留痕）。

        issued_at 为上游发起该请求的逻辑时间戳；乱序到达时按它判定先后，
        缺省 None 表示按到达顺序（写序号）参与排序。
        """
        self.rows.append({
            "table": table,
            "kind": kind.value,
            "key": key,
            "seq": seq,
            "schema_version": schema_version,
            "data": data,
            "correction_id": correction_id,
            "issued_at": issued_at,
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

    def _key_name(self, table: str, row: dict, default: str) -> str:
        """该行数据里业务键列的名字（按行自身的结构版本解析，兼容键列改名）。"""
        version = row.get("schema_version")
        if self._key_col_at is not None and version is not None:
            return self._key_col_at(table, version)
        return default

    @staticmethod
    def _rank(event: dict) -> tuple:
        """生效排序键：先看发起时间（缺省视为最旧），同档再看写序号（到达序）。"""
        issued = event.get("issued_at")
        return (issued if issued is not None else -1, event.get("seq", 0))

    def current(self, table: str, key_col: str) -> dict[object, dict]:
        """折叠出该表当前所有有效业务键 -> 数据（不含已撤回）。

        每个键取「排序键最大」的事件为生效结论：发起更晚的更正/撤回生效，
        更旧的请求后送达只会留痕、不会把结论翻回旧值。
        """
        best: dict[object, tuple] = {}  # key -> (rank, data|None)
        for r in self._ordered_events(table):
            if self._is_correction(r):
                key = r["key"]
                data = None if r["kind"] == EventKind.WITHDRAW.value \
                    else dict(r["data"])
            else:
                # 增量行：按该行版本下的业务键列名定位
                key = r["data"].get(self._key_name(table, r, key_col))
                if key is None:
                    continue
                data = dict(r["data"])
            rank = self._rank(r)
            # 同档（同排序键）时后落盘者生效，与历史逐条覆盖语义一致
            if key not in best or rank >= best[key][0]:
                best[key] = (rank, data)
        return {k: data for k, (rank, data) in best.items()
                if data is not None}

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
                    "issued_at": r.get("issued_at"),
                })
            elif r["data"].get(self._key_name(table, r, key_col)) == key:
                out.append({
                    "kind": EventKind.UPSERT.value,
                    "seq": r.get("seq"),
                    "offset": r["offset"],
                    "schema_version": r["schema_version"],
                    "data": dict(r["data"]),
                    "correction_id": None,
                    "issued_at": None,
                })
        return out
