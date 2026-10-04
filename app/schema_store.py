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


class KeyColumnProtectedError(SchemaError):
    """结构变更试图删除业务键列：业务键是更正/撤回定位历史结果的锚点，不允许删除。

    reason 固定为 RejectReason.KEY_COLUMN_PROTECTED，调用方可据此给出
    可区分的拒绝结论（HTTP 409）。改名允许，且整条改名链会被回放追踪。
    """

    reason = "KEY_COLUMN_PROTECTED"


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

    def key_col(self, table: str) -> str:
        """该表当前结构中的业务键列名（更正/撤回按此定位历史结果）；缺省为初始首列。

        业务键列允许改名：此处返回的是沿 RENAME 链回放后的**当前**列名。
        """
        return self.key_col_at(table, self.current_version(table))

    def key_col_init(self, table: str) -> str:
        """建表时确定的业务键列名（INIT 流水里的原名，不随改名变化）。"""
        for e in self.changelog.read_all():
            if e["table"] == table and e["change_type"] == _INIT:
                return e.get("key_col") or e["columns"][0]["name"]
        raise SchemaError(f"table={table} 尚未初始化")

    def key_col_at(self, table: str, version: int) -> str:
        """指定结构版本下的业务键列名：从 INIT 原名起，回放截至 version 的改名链。

        历史行/历史更正事件携带的是其生产时版本的键列名，折叠当前状态时必须按
        事件各自的版本取键列名，否则业务键列一旦改名，老数据就会从当前状态丢失。
        """
        key_name = self.key_col_init(table)
        for e in self.changelog.read_all():
            if e["table"] != table or e["version"] > version:
                continue
            if e["change_type"] == ChangeType.RENAME_COLUMN.value \
                    and e["column_name"] == key_name:
                key_name = e["to_name"]
        return key_name

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

    def plan_change(self, table: str, change: SchemaChange) -> dict:
        """校验一条结构变更并生成不可变流水事件（不写盘；调用方持锁）。

        返回事件中的 version 已分配；调用方可把多条事件组成一个「DDL 战役」
        整体校验、整体落盘（见 Pipeline.alter_schemas），从而保证多表结构
        变更相对跨表事务不可被插入。
        """
        if not self.is_initialized(table):
            raise SchemaError(f"table={table} 尚未初始化，无法结构变更")
        version = self._next_version(table)
        current = self.columns(table)
        ct = change.change_type
        if ct == ChangeType.ADD_COLUMN:
            column = change.column
            if column is None:
                raise SchemaError("ADD_COLUMN 必须提供列定义")
            if any(c.name == column.name for c in current):
                raise SchemaError(f"列 {column.name} 已存在")
            if column.required and column.default is None:
                raise SchemaError("新增必填列必须显式提供 default，缺省语义才可预期")
            return {
                "table": table, "version": version,
                "change_type": ChangeType.ADD_COLUMN.value,
                "column_name": column.name, "to_name": None,
                "column": _column_to_dict(column),
            }
        if ct == ChangeType.DROP_COLUMN:
            name = change.column_name
            if not any(c.name == name for c in current):
                raise SchemaError(f"列 {name} 不存在，无法删除")
            # 业务键列是更正/撤回定位历史结果的锚点：删除后历史结果将无法
            # 在当前状态中找到、也无法再被更正/撤回，因此在 DDL 阶段直接拒绝。
            # 需要换名请使用 RENAME_COLUMN（改名链会被持续追踪）。
            if name == self.key_col(table):
                raise KeyColumnProtectedError(
                    f"列 {name!r} 是 table={table} 的业务键列（更正/撤回的定位锚点），"
                    f"禁止删除；如需改名请使用 RENAME_COLUMN，旧结果仍可定位与更正")
            return {
                "table": table, "version": version,
                "change_type": ChangeType.DROP_COLUMN.value,
                "column_name": name, "to_name": None, "column": None,
            }
        if ct == ChangeType.RENAME_COLUMN:
            old, new = change.column_name, change.to_name
            if not new:
                raise SchemaError("RENAME_COLUMN 必须提供新列名 to_name")
            if not any(c.name == old for c in current):
                raise SchemaError(f"列 {old} 不存在，无法改名")
            if any(c.name == new for c in current):
                raise SchemaError(f"列名 {new} 已被占用")
            return {
                "table": table, "version": version,
                "change_type": ChangeType.RENAME_COLUMN.value,
                "column_name": old, "to_name": new, "column": None,
            }
        raise SchemaError(f"未知变更类型 {ct}")

    @staticmethod
    def _change_from_event(event: dict) -> SchemaChange:
        col = Column(**event["column"]) if event.get("column") else None
        return SchemaChange(
            version=event["version"],
            change_type=ChangeType(event["change_type"]),
            column_name=event["column_name"],
            to_name=event.get("to_name"), column=col)

    def commit_event(self, event: dict) -> SchemaChange:
        """把 plan_change 产出的事件追加到不可变流水（调用方持锁）。"""
        self.changelog.append(event)
        return self._change_from_event(event)

    def add_column(self, table: str, column: Column) -> SchemaChange:
        with self.changelog.lock():
            event = self.plan_change(
                table, SchemaChange(0, ChangeType.ADD_COLUMN, column.name,
                                    column=column))
            return self.commit_event(event)

    def drop_column(self, table: str, column_name: str) -> SchemaChange:
        with self.changelog.lock():
            event = self.plan_change(
                table, SchemaChange(0, ChangeType.DROP_COLUMN, column_name))
            return self.commit_event(event)

    def rename_column(self, table: str, old: str, new: str) -> SchemaChange:
        with self.changelog.lock():
            event = self.plan_change(
                table, SchemaChange(0, ChangeType.RENAME_COLUMN, old, to_name=new))
            return self.commit_event(event)
