"""表级锁：进程内互斥 + 跨进程 fcntl 文件锁。

所有“提交批次”和“结构变更”都必须先拿到同一把表锁，
从而保证并发提交与结构变更同时发生时结论仍然一致且可解释。
"""
from __future__ import annotations

import fcntl
import os
import threading
import time
from contextlib import contextmanager
from typing import Iterator

from ingestion.errors import TableLockedError


class TableLock:
    def __init__(self, lock_dir: str, timeout: float = 10.0) -> None:
        self.lock_dir = lock_dir
        self.timeout = timeout
        os.makedirs(lock_dir, exist_ok=True)
        self._guard = threading.Lock()
        self._thread_locks: dict[str, threading.Lock] = {}

    def _thread_lock(self, table: str) -> threading.Lock:
        with self._guard:
            lock = self._thread_locks.get(table)
            if lock is None:
                lock = threading.Lock()
                self._thread_locks[table] = lock
            return lock

    @contextmanager
    def table(self, table: str) -> Iterator[None]:
        # 1) 进程内串行化（同一进程多线程 / FastAPI 请求）
        tlock = self._thread_lock(table)
        if not tlock.acquire(timeout=self.timeout):
            raise TableLockedError(f"表 {table} 的进程内锁等待超时")
        fh = None
        try:
            # 2) 跨进程串行化（多进程 / 多 uvicorn worker）
            fh = open(os.path.join(self.lock_dir, f"{table}.lock"), "a+")
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TableLockedError(f"表 {table} 的文件锁等待超时")
                    time.sleep(0.02)
            yield
        finally:
            if fh is not None:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                finally:
                    fh.close()
            tlock.release()
