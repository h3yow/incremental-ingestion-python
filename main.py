"""FastAPI 应用入口：uvicorn main:app --reload。

数据目录默认 ./data，可用环境变量 INGESTION_DATA_DIR 覆盖。
"""
from ingestion.api import app

__all__ = ["app"]
