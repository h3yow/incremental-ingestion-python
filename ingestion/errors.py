"""错误类型与记录级拒绝原因。

所有失败都必须落到一个稳定的 code 上，便于调用方区分与断言。
"""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """单条记录被拒绝的原因（记录级，可区分、可追溯）。"""

    MISSING_KEY = "MISSING_KEY"                 # 主键缺失
    UNKNOWN_COLUMN = "UNKNOWN_COLUMN"           # 字段在当前表结构中不存在（删列/改名后旧数据）
    MISSING_COLUMN = "MISSING_COLUMN"           # NOT NULL 且无缺省值的字段缺失
    NULL_NOT_ALLOWED = "NULL_NOT_ALLOWED"       # NOT NULL 字段显式给了 null
    TYPE_MISMATCH = "TYPE_MISMATCH"             # 字段类型与表结构不符


class IngestionError(Exception):
    """所有流程级错误的基类，携带稳定 code。"""

    code: str = "INGESTION_ERROR"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class OffsetBackwardError(IngestionError):
    """位点回退/重叠：batch.start_offset 小于已提交位点。"""

    code = "OFFSET_BACKWARD"


class OffsetGapError(IngestionError):
    """位点越界推进：batch.start_offset 大于已提交位点，中间存在空洞。"""

    code = "OFFSET_GAP"


class BatchConflictError(IngestionError):
    """同一 batch_id 重复提交但内容（位点或记录指纹）不一致。"""

    code = "BATCH_CONFLICT"


class SchemaNotFoundError(IngestionError):
    """表尚未注册表结构。"""

    code = "SCHEMA_NOT_FOUND"


class SchemaVersionAheadError(IngestionError):
    """批次携带的 schema_version 比目标端当前版本更新。"""

    code = "SCHEMA_VERSION_AHEAD"


class IncompatibleSchemaChangeError(IngestionError):
    """结构变更本身不兼容（如新增 NOT NULL 列且无缺省值）。"""

    code = "INCOMPATIBLE_SCHEMA_CHANGE"


class TableLockedError(IngestionError):
    """获取表级提交锁超时（并发冲突可解释）。"""

    code = "TABLE_LOCKED"
