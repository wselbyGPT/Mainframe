from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            'ts': datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if hasattr(record, 'event'):
            payload['event'] = record.event
        if hasattr(record, 'job_id'):
            payload['job_id'] = record.job_id
        if hasattr(record, 'stage'):
            payload['stage'] = record.stage
        if hasattr(record, 'attempt'):
            payload['attempt'] = record.attempt
        if hasattr(record, 'context'):
            payload['context'] = record.context
        return json.dumps(payload, separators=(',', ':'))


def setup_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if getattr(root, '_structured_logging_enabled', False):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root._structured_logging_enabled = True  # type: ignore[attr-defined]


class StructuredLogger:
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def info(self, event: str, **kwargs: Any) -> None:
        self._logger.info(event, extra={'event': event, **kwargs})

    def warning(self, event: str, **kwargs: Any) -> None:
        self._logger.warning(event, extra={'event': event, **kwargs})

    def error(self, event: str, **kwargs: Any) -> None:
        self._logger.error(event, extra={'event': event, **kwargs})


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(logging.getLogger(name))


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def stage_duration_ms(start_at: str | None, end_at: str | None) -> int | None:
    start = parse_iso8601(start_at)
    end = parse_iso8601(end_at)
    if not start or not end:
        return None
    ms = int((end - start).total_seconds() * 1000)
    return ms if ms >= 0 else None
