"""目标端：结构版本化写入。

对齐规则（把任意历史版本的记录对齐到当前结构）
----------------------------------------------
1. 先取记录生产时的列集合（columns_at(record.schema_version)），逐字段做类型校验；
   记录里出现“旧结构中不存在的列” -> UNKNOWN_COLUMN（防止字段错位写入）。
2. 计算旧列名 -> 新列名映射：重命名链通过顺序回放 RENAME 流水得出；
   已删除的列直接丢弃（对齐到新结构时不再写入，绝不与同名列错位）。
3. 当前结构中缺失的列：
   - 该列是在某 ADD_COLUMN 流水中引入的 -> 用该列确定缺省（default）补齐；
   - 旧必填列仍然缺失 -> MISSING_REQUIRED（拒绝，绝不写入静默空值）。
4. 批次结构版本 > 目标当前版本 -> SCHEMA_VERSION_AHEAD，在 Pipeline 层整批拒绝。
"""

from __future__ import annotations

from app.models import ChangeType, Column, RecordResult, RejectReason
from app.storage import JsonlStore

_TYPE_CHECKS = {
    "int": (int,),
    "float": (int, float),
    "str": (str,),
    "bool": (bool,),
}


class TargetTable:
    def __init__(self, rows_store: JsonlStore):
        self.rows = rows_store

    def load(self, table: str) -> list[dict]:
        """读取某张表已落地的增量数据行（仅业务字段，不含更正/撤回事件）。"""
        return [dict(r["data"]) for r in self.rows.read_all()
                if r["table"] == table and r.get("kind") is None]

    def write_row(self, table: str, offset: int, row: dict, schema_version: int,
                  seq: int | None = None) -> None:
        event = {
            "table": table,
            "offset": offset,
            "schema_version": schema_version,
            "data": row,
        }
        if seq is not None:
            event["seq"] = seq
        self.rows.append(event)

    @staticmethod
    def _rename_mapping(changes) -> dict[str, str]:
        """按版本顺序回放 RENAME，得到 旧名 -> 当前名（含改名链）。"""
        mapping: dict[str, str] = {}
        for ch in sorted(changes, key=lambda c: c.version):
            if ch.change_type == ChangeType.RENAME_COLUMN:
                # 反向更新所有曾用名
                for old, cur in list(mapping.items()):
                    if cur == ch.column_name:
                        mapping[old] = ch.to_name
                mapping[ch.column_name] = ch.to_name
        return mapping

    def align_record(self, record_data: dict, from_version: int,
                     current_columns: list[Column],
                     old_columns: list[Column],
                     changes) -> tuple[RecordResult | None, dict | None]:
        """对齐并校验单条记录。成功返回 (None, 对齐后行)；失败返回 (RecordResult, None)。"""
        old_by_name = {c.name: c for c in old_columns}
        rename = self._rename_mapping(changes)
        current_names = {c.name for c in current_columns}

        # 1) 旧结构内逐字段校验（只认可记录版本中真实存在的列，杜绝错位）
        for key, value in record_data.items():
            col = old_by_name.get(key)
            if col is None:
                return RecordResult(-1, False, RejectReason.UNKNOWN_COLUMN,
                                    f"字段 {key!r} 不属于版本 {from_version} 的结构"), None
            expected = _TYPE_CHECKS.get(col.typ)
            if expected is None or not isinstance(value, expected) or \
                    (col.typ == "int" and isinstance(value, bool)):
                return RecordResult(-1, False, RejectReason.TYPE_MISMATCH,
                                    f"字段 {key!r} 期望 {col.typ}，实际 {type(value).__name__}"), None

        # 2) 映射到当前列名，已删除列丢弃
        aligned: dict[str, object] = {}
        for key, value in record_data.items():
            new_name = rename.get(key, key)
            if new_name in current_names:
                aligned[new_name] = value

        # 3) 当前结构缺列：加列补 default；其余必填缺失拒绝
        old_names_now = {rename.get(c.name, c.name)
                         for c in old_columns if rename.get(c.name, c.name) in current_names}
        added_defaults = {ch.column.name: ch.column.default
                          for ch in changes
                          if ch.change_type == ChangeType.ADD_COLUMN and ch.column is not None}
        for col in current_columns:
            if col.name in aligned:
                continue
            if col.name in old_names_now:
                # 旧列在记录中缺失：按旧结构的必填性判定
                # 找到对应旧列
                old_col = next(
                    (oc for oc in old_columns
                     if rename.get(oc.name, oc.name) == col.name), None)
                if old_col is not None and old_col.required:
                    return RecordResult(-1, False, RejectReason.MISSING_REQUIRED,
                                        f"必填字段 {col.name!r} 缺失"), None
                aligned[col.name] = old_col.default if old_col is not None else None
            else:
                # 后续版本新增的列：用稳定缺省补齐
                aligned[col.name] = added_defaults.get(col.name, col.default)
        return None, aligned
