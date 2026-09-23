"""核心数据模型。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str                 # "str" | "int" | "float" | "bool"
    nullable: bool = True
    has_default: bool = False
    default: object = None


@dataclass(frozen=True)
class Schema:
    table: str
    version: int
    key_column: str
    columns: tuple[Column, ...]

    def get(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


@dataclass(frozen=True)
class Batch:
    """源端提交的一个批次，offset 左闭右开 [start_offset, start_offset+n)。"""

    batch_id: str
    table: str
    start_offset: int
    schema_version: int
    records: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class RejectedRecord:
    offset: int
    key: object
    reason: str               # 取 RejectReason 的值
    detail: str


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    table: str
    start_offset: int
    end_offset: int
    schema_version: int
    applied_schema_version: int
    accepted_offsets: tuple[int, ...]
    rejected: tuple[RejectedRecord, ...]
    fingerprint: str = ""
    duplicate: bool = False   # True 表示本次为重复提交直接返回的既有结论

    @property
    def accepted_count(self) -> int:
        return len(self.accepted_offsets)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)
