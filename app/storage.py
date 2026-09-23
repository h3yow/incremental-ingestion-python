"""JSONL 本地存储与进程内锁（模拟本地持久化介质）。

- JsonlStore：每行一个 JSON 对象的追加式存储，用于目标数据、批次处理留痕、结构变更流水。
- KVStore：单 JSON 文件的键值存储，用于位点。
- 每个存储对象持有一把可重入锁；同一表的所有存储共享一把表锁，
  由 Pipeline 层注入，保证“批次提交”和“结构变更”对同一张表严格串行。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path


class JsonlStore:
    """按行追加的 JSONL 存储。"""

    def __init__(self, path: str | Path, lock: threading.RLock | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = lock or threading.RLock()

    def lock(self) -> threading.RLock:
        return self._lock

    def append(self, obj: dict) -> None:
        line = json.dumps(obj, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()

    def read_all(self) -> list[dict]:
        with self._lock:
            if not self.path.exists():
                return []
            with self.path.open("r", encoding="utf-8") as f:
                return [json.loads(line) for line in f if line.strip()]


class KVStore:
    """单键 JSON 文件存储（dict 持久化）。"""

    def __init__(self, path: str | Path, lock: threading.RLock | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = lock or threading.RLock()

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as f:
            content = f.read().strip()
            return json.loads(content) if content else {}

    def _dump(self, data: dict) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, sort_keys=True, indent=2)
        tmp.replace(self.path)

    def get(self, key: str) -> dict | None:
        with self._lock:
            return self._load().get(key)

    def put(self, key: str, value: dict) -> None:
        with self._lock:
            data = self._load()
            data[key] = value
            self._dump(data)
