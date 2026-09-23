"""表结构（Schema）注册表：版本化结构 + 结构变更流水。

兼容规则在注册表这一层就被强约束：
- 新增列：允许；缺省语义稳定 —— nullable 列缺省 None；
  NOT NULL 列必须显式提供 default，否则拒绝变更。
- 删列：允许（主键列除外）；旧批次携带被删字段时按 UNKNOWN_COLUMN 拒绝该记录，
  不会发生字段错位（按列名映射，而非按位置）。
- 改名：实现为“新增一列 + 删除一列”的一次版本变更，流水里记录 old/new 名称。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from ingestion.errors import IncompatibleSchemaChangeError, SchemaNotFoundError
from ingestion.models import Column, Schema

# 允许的列类型
TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "str": str,
    "int": int,
    "float": (int, float),
    "bool": bool,
}


@dataclass(frozen=True)
class SchemaChangeEvent:
    seq: int
    table: str
    from_version: int
    to_version: int
    kind: str                    # CREATE / ADD_COLUMN / DROP_COLUMN / RENAME_COLUMN
    payload: dict = field(default_factory=dict)
    timestamp: str = ""


def _column_from_dict(d: dict) -> Column:
    return Column(
        name=d["name"],
        type=d["type"],
        nullable=d.get("nullable", True),
        has_default=d.get("has_default", False),
        default=d.get("default"),
    )


def _schema_from_dict(d: dict) -> Schema:
    return Schema(
        table=d["table"],
        version=d["version"],
        key_column=d["key_column"],
        columns=tuple(_column_from_dict(c) for c in d["columns"]),
    )


class SchemaRegistry:
    """以本地 JSON 文件持久化的版本化结构注册表，每张表一个文件。"""

    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(dir_path, exist_ok=True)

    # ---------- 持久化 ----------
    def _path(self, table: str) -> str:
        return os.path.join(self.dir_path, f"{table}.schema.json")

    def _load(self, table: str) -> dict:
        p = self._path(table)
        if not os.path.exists(p):
            raise SchemaNotFoundError(f"表 {table} 尚未注册表结构")
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    def _store(self, table: str, doc: dict) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.dir_path, prefix=f".{table}-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path(table))
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ---------- 读取 ----------
    def current(self, table: str) -> Schema:
        doc = self._load(table)
        return _schema_from_dict(doc["schemas"][str(doc["current_version"])])

    def get_version(self, table: str, version: int) -> Schema:
        doc = self._load(table)
        key = str(version)
        if key not in doc["schemas"]:
            raise SchemaNotFoundError(f"表 {table} 不存在结构版本 v{version}")
        return _schema_from_dict(doc["schemas"][key])

    def history(self, table: str) -> list[SchemaChangeEvent]:
        doc = self._load(table)
        return [SchemaChangeEvent(**e) for e in doc.get("history", [])]

    # ---------- 变更 ----------
    def _append_version(self, doc: dict, schema: Schema, kind: str, payload: dict) -> None:
        event = SchemaChangeEvent(
            seq=len(doc["history"]) + 1,
            table=schema.table,
            from_version=doc["current_version"],
            to_version=schema.version,
            kind=kind,
            payload=payload,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        doc["current_version"] = schema.version
        doc["schemas"][str(schema.version)] = asdict(schema)
        doc["history"].append(asdict(event))

    def create_table(self, table: str, key_column: str, columns: list[Column]) -> Schema:
        p = self._path(table)
        if os.path.exists(p):
            raise IncompatibleSchemaChangeError(f"表 {table} 已存在，不能重复创建")
        names = [c.name for c in columns]
        if len(names) != len(set(names)):
            raise IncompatibleSchemaChangeError("列名重复")
        if key_column not in names:
            raise IncompatibleSchemaChangeError(f"主键列 {key_column} 必须在列定义中")
        for c in columns:
            if c.type not in TYPE_MAP:
                raise IncompatibleSchemaChangeError(f"不支持的列类型：{c.type}")
        schema = Schema(table=table, version=1, key_column=key_column,
                        columns=tuple(columns))
        doc = {
            "table": table,
            "key_column": key_column,
            "current_version": 1,
            "schemas": {"1": asdict(schema)},
            "history": [],
        }
        self._append_version(doc, schema, "CREATE", {"key_column": key_column})
        self._store(table, doc)
        return schema

    def _mutate(self, table: str, kind: str, payload: dict,
                mutate_fn) -> Schema:
        doc = self._load(table)
        current = _schema_from_dict(doc["schemas"][str(doc["current_version"])])
        new_schema = mutate_fn(current)
        self._append_version(doc, new_schema, kind, payload)
        self._store(table, doc)
        return new_schema

    def add_column(self, table: str, column: Column) -> Schema:
        def fn(current: Schema) -> Schema:
            if current.get(column.name) is not None:
                raise IncompatibleSchemaChangeError(
                    f"列 {column.name} 已存在（v{current.version}）"
                )
            if column.type not in TYPE_MAP:
                raise IncompatibleSchemaChangeError(f"不支持的列类型：{column.type}")
            # 稳定的缺省语义：NOT NULL 新列必须带显式 default
            if not column.nullable and not column.has_default:
                raise IncompatibleSchemaChangeError(
                    f"新增 NOT NULL 列 {column.name} 必须提供显式 default，"
                    "否则历史数据缺省语义不确定"
                )
            return Schema(
                table=current.table, version=current.version + 1,
                key_column=current.key_column,
                columns=current.columns + (column,),
            )
        return self._mutate(table, "ADD_COLUMN",
                            {"column": asdict(column)}, fn)

    def drop_column(self, table: str, column_name: str) -> Schema:
        def fn(current: Schema) -> Schema:
            if column_name == current.key_column:
                raise IncompatibleSchemaChangeError("不允许删除主键列")
            if current.get(column_name) is None:
                raise IncompatibleSchemaChangeError(
                    f"列 {column_name} 不存在（v{current.version}）"
                )
            return Schema(
                table=current.table, version=current.version + 1,
                key_column=current.key_column,
                columns=tuple(c for c in current.columns if c.name != column_name),
            )
        return self._mutate(table, "DROP_COLUMN", {"column": column_name}, fn)

    def rename_column(self, table: str, old_name: str, new_name: str) -> Schema:
        def fn(current: Schema) -> Schema:
            old = current.get(old_name)
            if old is None:
                raise IncompatibleSchemaChangeError(
                    f"列 {old_name} 不存在（v{current.version}）"
                )
            if old_name == current.key_column:
                raise IncompatibleSchemaChangeError("不允许对主键列改名")
            if current.get(new_name) is not None:
                raise IncompatibleSchemaChangeError(
                    f"列 {new_name} 已存在（v{current.version}）"
                )
            new_cols = tuple(
                Column(new_name, old.type, old.nullable, old.has_default, old.default)
                if c.name == old_name else c
                for c in current.columns
            )
            return Schema(
                table=current.table, version=current.version + 1,
                key_column=current.key_column, columns=new_cols,
            )
        return self._mutate(table, "RENAME_COLUMN",
                            {"old": old_name, "new": new_name}, fn)
