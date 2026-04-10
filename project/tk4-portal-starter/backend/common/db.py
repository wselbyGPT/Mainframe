from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import settings

_LOCK = threading.Lock()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    Path(settings.database_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.database_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL,
                    submitted_by TEXT NOT NULL,
                    input_params_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    result TEXT,
                    job_name TEXT,
                    mainframe_job_id TEXT,
                    return_code TEXT,
                    abend_code TEXT,
                    error_text TEXT,
                    stage TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS spool_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    section_type TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    content_text TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                );

                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()


def create_job(template_id: str, submitted_by: str, params: dict[str, Any]) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = _utcnow()
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, template_id, submitted_by, input_params_json, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, template_id, submitted_by, json.dumps(params), 'queued', now, now),
            )
            conn.commit()
        finally:
            conn.close()
    add_event(job_id, 'job.created', {'state': 'queued'})
    return get_job(job_id)


def update_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    fields = dict(fields)
    fields['updated_at'] = _utcnow()
    assignments = ', '.join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    with _LOCK:
        conn = connect()
        try:
            conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", values)
            conn.commit()
        finally:
            conn.close()


def add_event(job_id: str, event_type: str, payload: dict[str, Any]) -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                'INSERT INTO job_events (job_id, ts, event_type, payload_json) VALUES (?, ?, ?, ?)',
                (job_id, _utcnow(), event_type, json.dumps(payload)),
            )
            conn.commit()
        finally:
            conn.close()


def replace_spool_sections(job_id: str, sections: list[dict[str, Any]]) -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.execute('DELETE FROM spool_sections WHERE job_id = ?', (job_id,))
            conn.executemany(
                'INSERT INTO spool_sections (job_id, section_type, ordinal, content_text) VALUES (?, ?, ?, ?)',
                [(job_id, s['section_type'], s['ordinal'], s['content_text']) for s in sections],
            )
            conn.commit()
        finally:
            conn.close()


def get_job(job_id: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_jobs() -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = conn.execute('SELECT * FROM jobs ORDER BY created_at DESC').fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def next_queued_job() -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute(
                "SELECT * FROM jobs WHERE state = 'queued' ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            now = _utcnow()
            conn.execute(
                "UPDATE jobs SET state = 'starting', started_at = ?, updated_at = ? WHERE id = ?",
                (now, now, row['id']),
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (row['id'],)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def get_spool_sections(job_id: str) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = conn.execute(
            'SELECT section_type, ordinal, content_text FROM spool_sections WHERE job_id = ? ORDER BY ordinal ASC',
            (job_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_job_events(job_id: str) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = conn.execute(
            'SELECT ts, event_type, payload_json FROM job_events WHERE job_id = ? ORDER BY id ASC',
            (job_id,),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item['payload'] = json.loads(item.pop('payload_json'))
            out.append(item)
        return out
    finally:
        conn.close()
