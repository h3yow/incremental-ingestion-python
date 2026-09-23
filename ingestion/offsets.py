"""位点与断点管理。

位点语义：checkpoint 保存 next_offset，即“下一条待处理记录的 offset”。
- 合法起点：0
- 批次必须恰好紧邻 checkpoint：start_offset == checkpoint，否则
  * start_offset < checkpoint -> OFFSET_BACKWARD（回退/重叠，拒绝）
  * start_offset > checkpoint -> OFFSET_GAP（越界推进留下空洞，拒绝）
- 只有整批落地流程内部才允许把 checkpoint 推进到 end_offset（必须 > 旧值）
"""
from __future__ import annotations

import json
import os
import tempfile

from ingestion.errors import OffsetBackwardError, OffsetGapError


class CheckpointManager:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if not os.path.exists(path):
            self._write_all({})

    # ---------- 持久化 ----------
    def _read_all(self) -> dict[str, int]:
        with open(self.path, "r", encoding="utf-8") as f:
            return {t: int(v) for t, v in json.load(f).items()}

    def _write_all(self, data: dict[str, int]) -> None:
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".cp-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ---------- 读取/校验 ----------
    def get(self, table: str) -> int:
        """返回当前 next_offset（无记录时为 0）。"""
        return self._read_all().get(table, 0)

    def validate_adjacent(self, table: str, start_offset: int) -> None:
        """拒绝回退（< cp）与越界推进（> cp）。"""
        cp = self.get(table)
        if start_offset < cp:
            raise OffsetBackwardError(
                f"位点回退被拒绝：表 {table} 当前 next_offset={cp}，"
                f"批次起始={start_offset}，区间重叠/回退"
            )
        if start_offset > cp:
            raise OffsetGapError(
                f"位点越界推进被拒绝：表 {table} 当前 next_offset={cp}，"
                f"批次起始={start_offset}，中间存在 [{cp},{start_offset}) 的空洞"
            )

    def advance(self, table: str, end_offset: int) -> int:
        """仅允许在 commit 流程内部推进：新值必须严格大于旧值。"""
        data = self._read_all()
        old = data.get(table, 0)
        if end_offset < old:
            raise OffsetBackwardError(
                f"位点推进失败：表 {table} 当前 next_offset={old}，"
                f"目标值={end_offset} 小于当前值"
            )
        data[table] = end_offset
        self._write_all(data)
        return end_offset
