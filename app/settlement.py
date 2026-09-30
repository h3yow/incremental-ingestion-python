"""跨表业务批次的提交日志（journal）与结论折叠。

设计要点
--------
1. 一个业务批次（可含多张关联表）在**通过全部校验后，只产生一次 journal 追加**：
   追加成功 = 整个业务批次生效；追加不存在 / 仅写入半行 = 整个批次未生效。
   因此崩溃边界外永远观察不到“某张表生效一半”。
2. journal 行内携带全局单调 ``sequence``（提交顺序号），它就是“上次确认的边界”：
   重启后从日志完整重放即可重建同一最终状态。
3. 末行写入不完整（进程在追加中途崩溃）时，加载阶段把坏尾行截断丢弃，
   视为该批次未提交；调用方用同一 ``batch_id`` 重放会重新提交 —— 两种中断形态
   都收敛到同一最终状态。
4. 当前状态 = 按 sequence 顺序折叠日志：每个 (table, key) 只保留 revision 最大的
   有效结论（UPSERT 存活 / RETRACT 撤回）；旧 revision 一律跳过，
   因而旧值不会覆盖新值、撤回后旧更正也不会“复活”。全部历史结论保留在日志中，
   可逐键回放其更正/撤回轨迹。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from app.models import OpType


@dataclass
class Conclusion:
    """某个业务键折叠到当前的最后一条有效结论。"""

    table: str
    key: tuple[object, ...]
    revision: int
    status: OpType                       # UPSERT=存活；RETRACT=已撤回
    data: dict[str, object] | None       # RETRACT 时为 None
    schema_version: int                  # 写入该结论时对齐到的结构版本
    sequence: int                        # 该结论所属业务批次的提交顺序号
    batch_id: str


@dataclass
class LedgerView:
    """日志折叠结果：当前结论 + 逐键历史轨迹。"""

    conclusions: dict[tuple[str, tuple[object, ...]], Conclusion] = field(
        default_factory=dict)
    history: dict[tuple[str, tuple[object, ...]], list[Conclusion]] = field(
        default_factory=dict)

    def get(self, table: str, key: tuple[object, ...]) -> Conclusion | None:
        return self.conclusions.get((table, key))

    def live_rows(self, table: str) -> list[Conclusion]:
        out = [c for (t, _k), c in self.conclusions.items()
               if t == table and c.status == OpType.UPSERT]
        return sorted(out, key=lambda c: c.sequence)


class SettlementJournal:
    """business_batches.jsonl：跨表业务批次的唯一事实来源。"""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    # ---- 原始读写 ----
    def append_commit(self, entry: dict) -> None:
        """一次落盘的单行追加：跨表批次的原子提交点（fsync 后才算提交）。"""
        line = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def read_commits(self) -> list[dict]:
        """读取全部已提交条目；末行损坏（追加中途崩溃）则截断丢弃。"""
        if not self.path.exists():
            return []
        good: list[dict] = []
        valid_bytes = 0
        with self.path.open("rb") as f:
            raw = f.read()
        lines = raw.splitlines(keepends=True)
        for idx, raw_line in enumerate(lines):
            text = raw_line.decode("utf-8").strip()
            if not text:
                # 行尾之后的空片段：计入有效字节
                valid_bytes += len(raw_line)
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                if idx != len(lines) - 1:
                    raise
                # 仅允许末行不完整：截断到上一条完整记录边界
                with self.path.open("r+b") as f:
                    f.truncate(valid_bytes)
                break
            good.append(obj)
            valid_bytes += len(raw_line)
        return good

    def find(self, batch_id: str) -> dict | None:
        return next((e for e in self.read_commits()
                     if e["batch_id"] == batch_id), None)

    def max_sequence(self) -> int:
        return max((e["sequence"] for e in self.read_commits()), default=0)

    # ---- 折叠 ----
    @staticmethod
    def fold(entries: list[dict]) -> LedgerView:
        """按提交顺序折叠：旧 revision 不覆盖新 revision；撤回覆盖存活值。"""
        view = LedgerView()
        for e in sorted(entries, key=lambda x: x["sequence"]):
            for t in e["tables"]:
                table = t["table"]
                for op in t["ops"]:
                    key = tuple(op["key"])
                    ident = (table, key)
                    cur = view.conclusions.get(ident)
                    rev = int(op["revision"])
                    # 确定性顺序：只接受更新 revision 的结论
                    if cur is not None and rev <= cur.revision:
                        continue
                    conclusion = Conclusion(
                        table=table, key=key, revision=rev,
                        status=OpType(op["op_type"]),
                        data=op.get("data"),
                        schema_version=int(t["schema_version"]),
                        sequence=int(e["sequence"]),
                        batch_id=e["batch_id"])
                    view.conclusions[ident] = conclusion
                    view.history.setdefault(ident, []).append(conclusion)
        return view
