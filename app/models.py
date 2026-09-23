"""领域模型：批次、记录结果、拒绝原因、位点、结构变更。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RejectReason(str, Enum):
    """单条记录被拒绝 / 批次被拒绝的可区分原因。"""

    UNKNOWN_COLUMN = "UNKNOWN_COLUMN"            # 数据中存在当前结构不认识的字段
    MISSING_REQUIRED = "MISSING_REQUIRED"        # 新增必填字段缺失
    TYPE_MISMATCH = "TYPE_MISMATCH"              # 字段值类型与结构不符
    DUPLICATE_KEY = "DUPLICATE_KEY"              # 主键冲突（位点连续保证不应出现）
    OFFSET_BACKWARD = "OFFSET_BACKWARD"          # 位点回退
    OFFSET_GAP = "OFFSET_GAP"                    # 位点越界 / 跳号推进
    SCHEMA_VERSION_AHEAD = "SCHEMA_VERSION_AHEAD"  # 批次基于比目标更新的结构版本


class ChangeType(str, Enum):
    """结构变更流水类型。"""

    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    RENAME_COLUMN = "RENAME_COLUMN"


@dataclass(frozen=True)
class Column:
    name: str
    typ: str                      # "int" | "str" | "float" | "bool"
    required: bool = True
    default: object = None        # 新增非必填字段的稳定缺省


@dataclass(frozen=True)
class SchemaChange:
    version: int
    change_type: ChangeType
    column_name: str
    to_name: str | None = None    # RENAME_COLUMN 时的新名
    column: Column | None = None  # ADD_COLUMN 时的列定义


@dataclass(frozen=True)
class Record:
    offset: int
    data: dict[str, object]
    schema_version: int | None = None  # 生产该记录时的结构版本


@dataclass(frozen=True)
class Batch:
    batch_id: str
    table: str
    records: list[Record]
    schema_version: int                  # 批次基于的目标结构版本


@dataclass(frozen=True)
class RecordResult:
    offset: int
    accepted: bool
    reason: RejectReason | None = None
    detail: str = ""


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    table: str
    schema_version: int
    accepted_count: int
    rejected: list[RecordResult] = field(default_factory=list)
    checkpoint: int = 0          # 处理后已连续提交的位点
    replay: bool = False         # 是否为同一批次的重复提交（幂等回放）
