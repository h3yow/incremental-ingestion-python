"""位点存储与推进规则。

位点语义
--------
- 每张表维护一个整数位点 ``checkpoint``，表示“已连续提交的最大源端位点”；
  初始为 0，即尚未提交任何记录。
- 推进只能基于调用方读到的基线 ``expected`` 进行 CAS 推进：
  * ``new < 当前位点``        -> OFFSET_BACKWARD（位点回退，明确拒绝）；
  * ``expected != 当前位点``  -> OFFSET_BACKWARD（基线已被别的提交推进，
                                拒绝从过期中间位置继续）；
  * ``new < expected``        -> OFFSET_BACKWARD；
  * 记录层面的“跳号”由 Pipeline 在校验记录连续位点时以 OFFSET_GAP 拒绝。
- 位点与目标数据在同一把表锁内更新，保证“中断重启后不重复也不丢失”。
"""

from __future__ import annotations

from app.models import RejectReason
from app.storage import KVStore


class OffsetError(Exception):
    """位点非法：回退或越界。reason 取值见 RejectReason。"""

    def __init__(self, reason: RejectReason, detail: str):
        self.reason = reason
        super().__init__(f"{reason.value}: {detail}")


class CheckpointStore:
    def __init__(self, kv: KVStore):
        self.kv = kv

    def get(self, table: str) -> int:
        value = self.kv.get(table)
        return 0 if value is None else int(value["checkpoint"])

    def advance(self, table: str, expected: int, new: int) -> int:
        """把表位点从 ``expected`` CAS 推进到 ``new``，返回推进后的位点。"""
        current = self.get(table)
        if expected != current:
            raise OffsetError(
                RejectReason.OFFSET_BACKWARD,
                f"table={table} 位点基线过期：调用方基于 {expected}，"
                f"当前已推进到 {current}，拒绝从中间位置继续",
            )
        if new < expected:
            raise OffsetError(
                RejectReason.OFFSET_BACKWARD,
                f"table={table} 位点回退：{expected} -> {new}",
            )
        if new == current:
            return current
        self.kv.put(table, {"checkpoint": new})
        return new

    def ensure(self, table: str, target: int) -> int:
        """幂等推进到 ``target``：已达/超过则保持，绝不回退。

        用于两阶段提交的崩溃重放：位点只会被抬到 PREPARED 文档记录的边界。
        """
        current = self.get(table)
        if target <= current:
            return current
        self.kv.put(table, {"checkpoint": target})
        return target
