"""批次处理台账。

每个批次一个不可变 JSON 结果文件（ledger/<batch_id>.json）：
- 重复提交同一 batch_id：直接读取并返回既有结论（duplicate=True），
  不重复写入目标端；
- 若同一 batch_id 携带的位点/记录指纹与首次不一致，抛 BATCH_CONFLICT；
- 文件先写临时名再 os.replace，崩溃时不会留下半写结论。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile

from ingestion.models import Batch, BatchResult, RejectedRecord


def fingerprint(batch: Batch) -> str:
    """批次内容指纹：表、位点、schema 版本、记录全部参与。"""
    payload = json.dumps(
        {
            "table": batch.table,
            "start_offset": batch.start_offset,
            "end_offset": batch.start_offset + len(batch.records),
            "schema_version": batch.schema_version,
            "records": list(batch.records),
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def result_to_dict(r: BatchResult) -> dict:
    return {
        "batch_id": r.batch_id,
        "table": r.table,
        "start_offset": r.start_offset,
        "end_offset": r.end_offset,
        "schema_version": r.schema_version,
        "applied_schema_version": r.applied_schema_version,
        "accepted_offsets": list(r.accepted_offsets),
        "rejected": [dataclasses_asdict(x) for x in r.rejected],
        "duplicate": False,   # 台账保存的永远是首次结论
        "fingerprint": r.fingerprint,
    }


def dataclasses_asdict(r: RejectedRecord) -> dict:
    return {"offset": r.offset, "key": r.key, "reason": r.reason, "detail": r.detail}


def _dict_to_result(d: dict) -> BatchResult:
    return BatchResult(
        batch_id=d["batch_id"],
        table=d["table"],
        start_offset=d["start_offset"],
        end_offset=d["end_offset"],
        schema_version=d["schema_version"],
        applied_schema_version=d["applied_schema_version"],
        accepted_offsets=tuple(d["accepted_offsets"]),
        rejected=tuple(RejectedRecord(**x) for x in d["rejected"]),
        fingerprint=d.get("fingerprint", ""),
        duplicate=False,
    )


class BatchLedger:
    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(dir_path, exist_ok=True)

    def _path(self, batch_id: str) -> str:
        return os.path.join(self.dir_path, f"{batch_id}.json")

    def get(self, batch_id: str) -> BatchResult | None:
        p = self._path(batch_id)
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            return _dict_to_result(json.load(f))

    def get_raw(self, batch_id: str) -> dict | None:
        p = self._path(batch_id)
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, result: BatchResult, fp: str) -> None:
        """原子写入；结果文件不可变，已存在时直接忽略（重放安全）。"""
        doc = result_to_dict(result)
        doc["fingerprint"] = fp
        doc["duplicate"] = False
        fd, tmp = tempfile.mkstemp(dir=self.dir_path, prefix=".ledger-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path(result.batch_id))
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
