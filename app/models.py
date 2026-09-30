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
    KEY_NOT_FOUND = "KEY_NOT_FOUND"              # 更正/撤回的业务键当前不存在
    ENTRY_INVALID = "ENTRY_INVALID"              # 跨表业务批次中某个表的数据校验失败
    TABLE_NOT_INITIALIZED = "TABLE_NOT_INITIALIZED"  # 业务批次涉及未建表的表


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


# ---------------- 跨表业务批次 / 更正 / 撤回 ----------------

class BusinessOp(str, Enum):
    """跨表业务批次中每个表分片的提交方式。"""

    APPEND = "APPEND"            # 正常增量：按连续位点追加
    UPSERT = "UPSERT"            # 更正：按业务键整体覆盖当前结论


class EventKind(str, Enum):
    """对业务键当前状态生效的事件类型。"""

    UPSERT = "UPSERT"
    WITHDRAW = "WITHDRAW"


@dataclass(frozen=True)
class TableBatchEntry:
    """跨表业务批次中某一张表的分片。"""

    table: str
    records: list[Record]
    schema_version: int          # 该分片基于的目标结构版本
    op: BusinessOp = BusinessOp.APPEND


@dataclass(frozen=True)
class BusinessBatch:
    """跨表业务批次：包含多张关联表，整体提交、整体生效。"""

    business_batch_id: str
    entries: list[TableBatchEntry]


@dataclass(frozen=True)
class CorrectionItem:
    """针对某张表一条历史业务结果的更正或撤回。

    - UPSERT：key 必须已存在，data 为更正后的完整数据（按 key 覆盖，不做字段级合并）；
    - WITHDRAW：key 必须已存在，data 必须为 None，撤回该条历史结果。
    """

    table: str
    key: object
    op: EventKind
    schema_version: int          # 该更正基于的目标结构版本
    data: dict[str, object] | None = None


@dataclass(frozen=True)
class CorrectionBatch:
    """一次更正/撤回请求；可只含一条，也可跨多表，整体生效。"""

    correction_id: str
    items: list[CorrectionItem]


class TxState(str, Enum):
    """业务事务（跨表批次 / 更正批次）的两阶段状态。"""

    PREPARED = "PREPARED"        # 已落盘待生效内容，尚未应用
    COMMITTED = "COMMITTED"      # 已整体生效
    ABORTED = "ABORTED"          # 预检失败，整体未生效（留痕可区分）


@dataclass(frozen=True)
class EntryResult:
    """业务批次内单个表分片 / 单条更正的处理结论。"""

    table: str
    accepted_count: int = 0
    checkpoint: int = 0
    rejected: list[RecordResult] = field(default_factory=list)
    reason: RejectReason | None = None   # 分片级错误（如 KEY_NOT_FOUND / ENTRY_INVALID）
    detail: str = ""


@dataclass(frozen=True)
class BusinessBatchResult:
    business_batch_id: str
    committed: bool
    entries: list[EntryResult] = field(default_factory=list)
    state: TxState = TxState.COMMITTED
    replay: bool = False         # 是否为同一业务批次的重复提交（幂等回放）
