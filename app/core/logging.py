import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.core.config import get_settings

# Context variables for tracing across async tasks
ctx_request_id: ContextVar[Optional[str]] = ContextVar("ctx_request_id", default=None)
ctx_job_id: ContextVar[Optional[str]] = ContextVar("ctx_job_id", default=None)
ctx_worker_id: ContextVar[Optional[str]] = ContextVar("ctx_worker_id", default=None)


def set_request_id(request_id: Optional[str]) -> None:
    ctx_request_id.set(request_id)


def set_job_id(job_id: Optional[str]) -> None:
    ctx_job_id.set(job_id)


def set_worker_id(worker_id: Optional[str]) -> None:
    ctx_worker_id.set(worker_id)


def clear_context() -> None:
    ctx_request_id.set(None)
    ctx_job_id.set(None)
    ctx_worker_id.set(None)


class StructuredJSONFormatter(logging.Formatter):
    """Formats log records as structured single-line JSON objects with context tracking."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }

        # Contextual metadata
        req_id = ctx_request_id.get()
        if req_id:
            log_entry["request_id"] = req_id

        job_id = ctx_job_id.get()
        if job_id:
            log_entry["job_id"] = job_id

        worker_id = ctx_worker_id.get()
        if worker_id:
            log_entry["worker_id"] = worker_id

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging() -> None:
    settings = get_settings()
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredJSONFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = [handler]

    # Silence verbose sqlalchemy / uvicorn noise if desired
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
