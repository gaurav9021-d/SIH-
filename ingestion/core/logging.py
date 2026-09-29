"""Structured JSON logging with defense-in-depth secret redaction."""

from __future__ import annotations

import json
import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, date, datetime
from typing import Any, Iterator

_LOG_CONTEXT: ContextVar[dict[str, Any]] = ContextVar("ingestion_log_context", default={})
_REGISTERED_SECRETS: set[str] = set()
_FIRMS_KEY_PATTERN = re.compile(
    r"(/(?:area|data_availability)/csv/)[^/\s?]+", re.IGNORECASE
)
_URI_USERINFO_PATTERN = re.compile(
    r"([a-z][a-z0-9+.-]*://)([^\s/@:]+):([^\s/@]+)@", re.IGNORECASE
)


def register_secret(secret: str | None) -> None:
    if secret:
        _REGISTERED_SECRETS.add(secret)


def redact(value: str) -> str:
    result = value
    for secret in sorted(_REGISTERED_SECRETS, key=len, reverse=True):
        result = result.replace(secret, "***")
    result = _FIRMS_KEY_PATTERN.sub(r"\1***", result)
    return _URI_USERINFO_PATTERN.sub(r"\1***:***@", result)


def _safe(value: Any) -> Any:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe(item) for item in value]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return redact(str(value))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
        }
        payload.update(_LOG_CONTEXT.get())
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        message = record.getMessage()
        if message and message != payload["event"]:
            payload["message"] = message
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(_safe(payload), sort_keys=True, separators=(",", ":"))


def configure_logging(*secrets: str | None, level: int = logging.INFO) -> logging.Logger:
    for secret in secrets:
        register_secret(secret)

    logger = logging.getLogger("ingestion")
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"ingestion.{name}")


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    logger.log(level, event, extra={"event": event, "fields": fields})


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    merged = dict(_LOG_CONTEXT.get())
    merged.update(fields)
    token = _LOG_CONTEXT.set(merged)
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)
