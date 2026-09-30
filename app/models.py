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
    TABLE_NOT_KEYED = "TABLE_NOT_KEYED"          # 表未声明业务键列，不能走业务键更正/撤回
    KEY_MISMATCH = "KEY_MISMATCH"                # 业务键取值个数/类型与声明的键列不符
    STALE_REVISION = "STALE_REVISION"            # 更正 revision 不新于该键已生效结论（旧值翻案）
    DUPLICATE_KEY_IN_BATCH = "DUPLICATE_KEY_IN_BATCH"  # 同一业务批次内同表同键出现多次
    INVALID_OPERATION = "INVALID_OPERATION"      # 操作非法（撤回带数据 / 更正缺数据 / 表缺失等）


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


class OpType(str, Enum):
    """业务操作类型：更正（写入当前结论）/ 撤回（撤销历史结论）。"""

    UPSERT = "UPSERT"            # 正常增量写入或历史更正：以业务键覆盖当前结论
    RETRACT = "RETRACT"          # 撤回：撤销该业务键的当前结论


@dataclass(frozen=True)
class BusinessOp:
    """针对单张表某个业务键的一次操作（更正或撤回）。

    - key：业务键取值（与建表声明的 key_columns 按序对应），跨更正/撤回稳定标识一条业务结果；
    - revision：上游为该业务键分配的严格递增版本号，用于确定并发结论顺序
      （旧版本不得覆盖新版本、撤回后不会被旧更正“复活”）；
    - data：UPSERT 时的完整业务数据（按 op_schema_version 对齐到当前结构）；
    - op_schema_version：该操作数据生产时的结构版本。
    """

    table: str
    op_type: OpType
    key: tuple[object, ...]
    revision: int
    data: dict[str, object] | None = None
    op_schema_version: int | None = None


@dataclass(frozen=True)
class BusinessBatch:
    """跨表业务批次：一个结算批次可含多张关联表的操作，整体提交或整体不生效。"""

    batch_id: str                # 全局幂等键（跨表唯一）
    ops: list[BusinessOp]
    schema_version: int          # 调用方认为目标端所处的最小结构版本（版本头）


@dataclass(frozen=True)
class TableOutcome:
    """业务批次在单张表上的结果（失败时精确到表 / 业务键 / 数据类别）。"""

    table: str
    accepted: bool
    ops: int = 0
    schema_version: int = 0
    reason: RejectReason | None = None
    detail: str = ""
    key: tuple[object, ...] | None = None  # 出错的业务键（可定位到具体结果）


@dataclass(frozen=True)
class BusinessBatchResult:
    batch_id: str
    committed: bool              # 是否整体生效
    sequence: int = 0            # 全局提交顺序号（结论的确定顺序）
    tables: list[TableOutcome] = field(default_factory=list)
    replay: bool = False         # 是否为同一业务批次的幂等回放
