"""业务键当前状态存储：在已落地事件之上折叠出「最后一次有效结论」。

事件来源（均在 rows.jsonl 内，按表区分）：
- 增量行：{"table","offset","schema_version","data",?seq}，业务键取业务键列的值；
- 更正事件：{"table","kind","key","seq","schema_version","data","correction_id",
  ?issued_seq}。

确定顺序
--------
每张表维护一条单调写序号 ``write_seq``（seq.json），增量行与更正事件在应用时
于表锁内依次取号；因此所有事件存在唯一全序，折叠规则为：

1. 有 seq 的事件按 seq 升序；无 seq 的历史行（旧版本落盘）按 offset 升序、
   且一律排在有 seq 事件之前（有效序视为 0）。
2. 依次 UPSERT：``state[key] = data``；WITHDRAW：``state.pop(key)``。

业务键列改名
------------
事件按其**生产时的结构版本**记录业务键列名：折叠/检索历史时通过
``key_resolver(table, schema_version)``（回放 RENAME 链得到）解析该版本的
键列名，因此业务键列改名后，老版本行仍按旧列名取键、新版本行按新列名取键，
当前状态不会因改名而丢结果，更正/撤回也始终能定位到同一条历史结果。

当前状态只反映最后一次有效结论；事件不可变、永久保留，history() 可逐条解释。
重复重放通过 (correction_id, table, key) 去重、不会再次取号，因此结论不翻转。
"""

from __future__ import annotations

from typing import Callable

from app.models import EventKind
from app.storage import JsonlStore, KVStore

# 键列名解析器：(table, 事件生产时的结构版本) -> 该版本下的业务键列名。
# 缺省回落到初始键列（兼容无 schema_version 的极早期落盘）。
KeyResolver = Callable[[str, int | None], str]
# 行归一化器：(data, 事件生产时的结构版本) -> 对齐到「当前结构」后的数据。
# 折叠当前状态时使用，使改名/加列/删列后，不同版本写入的行在当前状态中
# 呈现为统一的当前字段集合；历史留痕 history() 不做归一化，保留生产时原貌。
RowNormalizer = Callable[[dict, int | None], dict]


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
                     correction_id: str,
                     issued_seq: int | None = None) -> None:
        """追加一条更正/撤回事件（不可变留痕）。"""
        event = {
            "table": table,
            "kind": kind.value,
            "key": key,
            "seq": seq,
            "schema_version": schema_version,
            "data": data,
            "correction_id": correction_id,
        }
        # issued_seq 仅在调用方提供时落盘：它是「发起先后」裁决的依据
        if issued_seq is not None:
            event["issued_seq"] = issued_seq
        self.rows.append(event)

    def has_event(self, correction_id: str, table: str, key: object) -> bool:
        """某条更正事件是否已应用（崩溃重放去重）。"""
        return any(
            r.get("kind") is not None
            and r.get("correction_id") == correction_id
            and r.get("table") == table and r.get("key") == key
            for r in self.rows.read_all())

    def max_issued_seq(self, table: str, key: object) -> int | None:
        """该业务键已生效事件中最大的发起序号（含 UPSERT 与 WITHDRAW）。

        - 从未有过带 issued_seq 的事件 -> None（调用方不提供发起序号，
          退化为按到达先后裁决）；
        - 撤回后历史事件仍参与计算：撤回本身也可以携带发起序号，
          因此「更早发起、更晚送达」的请求仍会被识别为旧请求。
        """
        values = [r.get("issued_seq")
                  for r in self.rows.read_all()
                  if r["table"] == table and self._is_correction(r)
                  and r.get("key") == key and r.get("issued_seq") is not None]
        return max(values) if values else None

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

    def current(self, table: str, key_resolver: KeyResolver,
                normalizer: RowNormalizer | None = None) -> dict[object, dict]:
        """折叠出该表当前所有有效业务键 -> 数据（不含已撤回）。

        key_resolver 按每条事件生产时的结构版本解析业务键列名，
        因而业务键列改名后各版本行仍能正确归并到同一个键。
        normalizer 若提供，则把每行数据对齐到当前结构（改名映射/加列补缺省/
        删列移除），保证当前状态呈现统一的当前字段集合；不影响历史留痕。
        """
        def _norm(data: dict, version: int | None) -> dict:
            if normalizer is None:
                return dict(data)
            return normalizer(data, version)

        state: dict[object, dict] = {}
        for r in self._ordered_events(table):
            version = r.get("schema_version")
            if self._is_correction(r):
                key = r["key"]
                if r["kind"] == EventKind.WITHDRAW.value:
                    state.pop(key, None)
                else:
                    state[key] = _norm(r["data"], version)
            else:
                # 增量行：按该行版本的业务键列折叠（同一键的新版本覆盖旧版本）
                key_col = key_resolver(table, version)
                key = r["data"].get(key_col)
                if key is None:
                    continue
                state[key] = _norm(r["data"], version)
        return state

    def history(self, table: str, key: object,
                key_resolver: KeyResolver) -> list[dict]:
        """返回某业务键经历过的全部事件（增量写入 + 各次更正/撤回），按全序排列。"""
        out: list[dict] = []
        for r in self._ordered_events(table):
            if self._is_correction(r):
                if r["key"] != key:
                    continue
                out.append({
                    "kind": r["kind"],
                    "seq": r["seq"],
                    "issued_seq": r.get("issued_seq"),
                    "schema_version": r["schema_version"],
                    "data": None if r["data"] is None else dict(r["data"]),
                    "correction_id": r["correction_id"],
                })
            else:
                key_col = key_resolver(table, r.get("schema_version"))
                if r["data"].get(key_col) != key:
                    continue
                out.append({
                    "kind": EventKind.UPSERT.value,
                    "seq": r.get("seq"),
                    "issued_seq": None,
                    "offset": r["offset"],
                    "schema_version": r.get("schema_version"),
                    "data": dict(r["data"]),
                    "correction_id": None,
                })
        return out
