"""业务事务日志：跨表业务批次 / 更正批次的两阶段提交留痕。

事务文档（transactions.jsonl，每行一条状态翻转记录）：
- PREPARED：已通过全部校验、待生效内容（对齐后的行、位点推进区间、更正事件）
  完整落盘；文档自身确定，不依赖任何运行期状态；
- COMMITTED：待生效内容已全部幂等应用；
- ABORTED：预检失败，未应用任何效果，仅留痕（可按业务批次定位是哪张表哪类数据）。

同一 txn_id 可能有多条记录（PREPARED -> COMMITTED/ABORTED），
按落盘顺序取最后一条即为当前状态。全部记录均不可变、永久可追溯。
"""

from __future__ import annotations

from app.models import TxState
from app.storage import JsonlStore


class TxLog:
    """事务日志存储（追加式；状态翻转通过 append 新行表达）。"""

    def __init__(self, store: JsonlStore):
        self.store = store

    def read_all(self) -> list[dict]:
        return self.store.read_all()

    def find(self, txn_id: str) -> dict | None:
        """返回某业务事务的最新状态文档（按落盘顺序取最后一条）。"""
        latest: dict | None = None
        for doc in self.store.read_all():
            if doc["txn_id"] == txn_id:
                latest = doc
        return latest

    def append_state(self, doc: dict) -> None:
        """追加一条事务状态文档（PREPARED/COMMITTED/ABORTED）。"""
        self.store.append(doc)

    def prepared_open(self) -> list[dict]:
        """返回最新状态仍为 PREPARED 的事务文档（崩溃后待重放）。

        多个未决事务按其 PREPARED 文档落盘顺序返回，
        保证恢复时的应用顺序与崩溃前一致。
        """
        latest: dict[str, dict] = {}
        order: list[str] = []
        for doc in self.store.read_all():
            txn_id = doc["txn_id"]
            if txn_id not in latest:
                order.append(txn_id)
            latest[txn_id] = doc
        return [latest[txn_id] for txn_id in order
                if latest[txn_id]["state"] == TxState.PREPARED.value]
