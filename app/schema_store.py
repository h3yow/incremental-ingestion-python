"""表结构与变更流水管理。

结构版本与兼容规则
------------------
- 每张表初始化后版本为 1，初始列定义作为第一条流水（INIT）持久化。
- 每次结构变更产生一条不可变流水，版本严格 +1：
  ADD_COLUMN / DROP_COLUMN / RENAME_COLUMN。
- 新增列必须给出确定缺省：非必填列缺省为 None；必填列必须显式带 default，
  否则在 DDL 阶段直接拒绝（避免缺省语义不确定）。
- 任意历史版本的列集合都可通过回放流水重建（columns_at），
  旧版本数据据此对齐到当前结构，删列/改名不会造成字段错位。
"""

from __future__ import annotations

from dataclasses import replace

from app.models import ChangeType, Column, SchemaChange
from app.storage import JsonlStore

_INIT = "INIT"


class SchemaError(Exception):
    pass


def _column_to_dict(c: Column) -> dict:
    return {"name": c.name, "typ": c.typ, "required": c.required, "default": c.default}


def _column_from_dict(d: dict) -> Column:
    return Column(name=d["name"], typ=d["typ"],
                  required=d.get("required", True),
                  default=d.get("default"))


class SchemaStore:
    def __init__(self, changelog_store: JsonlStore):
        self.changelog = changelog_store

    # ---- 读取 ----
    def is_initialized(self, table: str) -> bool:
        return any(e["table"] == table and e["change_type"] == _INIT
                   for e in self.changelog.read_all())

    def _init_key_col(self, table: str) -> str:
        """INIT 流水记录的原始业务键列名。"""
        for e in self.changelog.read_all():
            if e["table"] == table and e["change_type"] == _INIT:
                if e.get("key_col"):
                    return e["key_col"]
                return e["columns"][0]["name"]
        raise SchemaError(f"table={table} 尚未初始化")

    def key_col_at(self, table: str, version: int) -> str:
        """指定结构版本下业务键列的名字（改名链只回放到该版本）。

        业务键是更正/撤回与当前状态的定位锚点：列改名不改变键的身份，
        只是它在不同版本数据里的名字不同——v1 的行里叫旧名、改名后的行里叫新名。
        """
        name = self._init_key_col(table)
        renames = sorted(
            (e for e in self.changelog.read_all()
             if e["table"] == table
             and e["change_type"] == ChangeType.RENAME_COLUMN.value
             and e["version"] <= version),
            key=lambda e: e["version"])
        for e in renames:
            if e["column_name"] == name:
                name = e["to_name"]
        return name

    def key_col(self, table: str) -> str:
        """该表业务键列的当前名字（跟随改名链）；缺省为初始首列。"""
        return self.key_col_at(table, self.current_version(table))

    def changes(self, table: str) -> list[SchemaChange]:
        out = []
        for e in self.changelog.read_all():
            if e["table"] != table or e["change_type"] == _INIT:
                continue
            col = Column(**e["column"]) if e.get("column") else None
            out.append(SchemaChange(
                version=e["version"],
                change_type=ChangeType(e["change_type"]),
                column_name=e["column_name"],
                to_name=e.get("to_name"),
                column=col,
            ))
        return out

    def current_version(self, table: str) -> int:
        versions = [e["version"] for e in self.changelog.read_all() if e["table"] == table]
        if not versions:
            raise SchemaError(f"table={table} 尚未初始化")
        return max(versions)

    def columns(self, table: str) -> list[Column]:
        return self.columns_at(table, self.current_version(table))

    def columns_at(self, table: str, version: int) -> list[Column]:
        """回放流水，重建指定版本的列定义（保持列的原始顺序，改名不移位）。"""
        cols: list[Column] = []
        seen_init = False
        for e in self.changelog.read_all():
            if e["table"] != table or e["version"] > version:
                continue
            ct = e["change_type"]
            if ct == _INIT:
                cols = [_column_from_dict(c) for c in e["columns"]]
                seen_init = True
            elif ct == ChangeType.ADD_COLUMN.value:
                cols.append(_column_from_dict(e["column"]))
            elif ct == ChangeType.DROP_COLUMN.value:
                cols = [c for c in cols if c.name != e["column_name"]]
            elif ct == ChangeType.RENAME_COLUMN.value:
                old, new = e["column_name"], e["to_name"]
                cols = [replace(c, name=new) if c.name == old else c for c in cols]
        if not seen_init:
            raise SchemaError(f"table={table} 尚未初始化或版本 {version} 不存在")
        return cols

    # ---- 变更 ----
    def _append(self, event: dict) -> None:
        with self.changelog.lock():
            self.changelog.append(event)

    def init_table(self, table: str, columns: list[Column],
                   key_col: str | None = None) -> int:
        with self.changelog.lock():
            if self.is_initialized(table):
                raise SchemaError(f"table={table} 已初始化，禁止重复建表")
            if not columns:
                raise SchemaError("建表至少需要一列")
            names = [c.name for c in columns]
            if len(set(names)) != len(names):
                raise SchemaError("列名重复")
            key_col = key_col or names[0]
            if key_col not in names:
                raise SchemaError(f"业务键列 {key_col!r} 不在列定义中")
            self._append({
                "table": table, "version": 1, "change_type": _INIT,
                "key_col": key_col,
                "columns": [_column_to_dict(c) for c in columns],
            })
            return 1

    def _next_version(self, table: str) -> int:
        return self.current_version(table) + 1

    # ---- 校验（不落盘；多表原子变更先整体校验再整体应用） ----
    def _validate_add(self, table: str, column: Column) -> None:
        if any(c.name == column.name for c in self.columns(table)):
            raise SchemaError(f"列 {column.name} 已存在")
        if column.required and column.default is None:
            raise SchemaError("新增必填列必须显式提供 default，缺省语义才可预期")

    def _validate_drop(self, table: str, column_name: str) -> None:
        if not any(c.name == column_name for c in self.columns(table)):
            raise SchemaError(f"列 {column_name} 不存在，无法删除")
        if column_name == self.key_col(table):
            raise SchemaError(
                f"列 {column_name} 是表 {table} 的业务键列：当前状态折叠与"
                f"更正/撤回都靠它定位历史结果，删除后已接入数据将无法定位"
                f"（接进来也查不到、想改也改不了），故不允许删除；"
                f"如需调整请用 RENAME 改名（键身份会跟随改名链）或重建表")

    def _validate_rename(self, table: str, old: str, new: str) -> None:
        current = self.columns(table)
        if not any(c.name == old for c in current):
            raise SchemaError(f"列 {old} 不存在，无法改名")
        if any(c.name == new for c in current):
            raise SchemaError(f"列名 {new} 已被占用")

    def validate_change(self, table: str, change: SchemaChange) -> None:
        """只校验不落盘：供多表原子结构变更先做全量预检。"""
        if change.change_type == ChangeType.ADD_COLUMN:
            self._validate_add(table, change.column)
        elif change.change_type == ChangeType.DROP_COLUMN:
            self._validate_drop(table, change.column_name)
        elif change.change_type == ChangeType.RENAME_COLUMN:
            self._validate_rename(table, change.column_name, change.to_name)
        else:
            raise SchemaError(f"未知变更类型 {change.change_type}")

    # ---- 变更 ----
    def apply_change(self, table: str, change: SchemaChange) -> SchemaChange:
        """应用一条已校验（或即将校验）的变更，版本严格 +1。"""
        with self.changelog.lock():
            self.validate_change(table, change)
            version = self._next_version(table)
            if change.change_type == ChangeType.ADD_COLUMN:
                event = {
                    "table": table, "version": version,
                    "change_type": ChangeType.ADD_COLUMN.value,
                    "column_name": change.column.name, "to_name": None,
                    "column": _column_to_dict(change.column),
                }
                result = SchemaChange(version, ChangeType.ADD_COLUMN,
                                      change.column.name, column=change.column)
            elif change.change_type == ChangeType.DROP_COLUMN:
                event = {
                    "table": table, "version": version,
                    "change_type": ChangeType.DROP_COLUMN.value,
                    "column_name": change.column_name, "to_name": None,
                    "column": None,
                }
                result = SchemaChange(version, ChangeType.DROP_COLUMN,
                                      change.column_name)
            else:
                event = {
                    "table": table, "version": version,
                    "change_type": ChangeType.RENAME_COLUMN.value,
                    "column_name": change.column_name,
                    "to_name": change.to_name, "column": None,
                }
                result = SchemaChange(version, ChangeType.RENAME_COLUMN,
                                      change.column_name, to_name=change.to_name)
            self._append(event)
            return result

    def add_column(self, table: str, column: Column) -> SchemaChange:
        return self.apply_change(table, SchemaChange(
            0, ChangeType.ADD_COLUMN, column.name, column=column))

    def drop_column(self, table: str, column_name: str) -> SchemaChange:
        return self.apply_change(table, SchemaChange(
            0, ChangeType.DROP_COLUMN, column_name))

    def rename_column(self, table: str, old: str, new: str) -> SchemaChange:
        return self.apply_change(table, SchemaChange(
            0, ChangeType.RENAME_COLUMN, old, to_name=new))
