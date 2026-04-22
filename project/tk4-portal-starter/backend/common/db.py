from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from common.config import settings

_LOCK = threading.Lock()
_CANCELABLE_STATES = {
    'queued',
    'reserved',
    'retryable',
    'starting',
    'submitted',
    'running',
    'logging_in',
    'writing_jcl',
    'waiting_for_completion',
    'reading_spool',
}
_REQUEUEABLE_STATES = _CANCELABLE_STATES | {'failed', 'canceled'}
_FAILURE_RESULTS = {'error', 'failed', 'jcl_error', 'abend'}


class JobTransitionError(RuntimeError):
    def __init__(self, code: str, message: str, state: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.state = state

    def to_dict(self) -> dict[str, Any]:
        detail: dict[str, Any] = {'code': self.code, 'message': self.message}
        if self.state is not None:
            detail['state'] = self.state
        return detail


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    Path(settings.database_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.database_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
    return any(row['name'] == column for row in rows)


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
                    attempt INTEGER NOT NULL DEFAULT 1,
                    parent_job_id TEXT,
                    retry_of_job_id TEXT,
                    state TEXT NOT NULL,
                    result TEXT,
                    job_name TEXT,
                    mainframe_job_id TEXT,
                    return_code TEXT,
                    abend_code TEXT,
                    error_text TEXT,
                    stage TEXT,
                    available_at TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    dead_letter_reason TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS spool_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    section_type TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    content_text TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                );

                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                );

                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS identities (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(provider, subject),
                    FOREIGN KEY(user_id) REFERENCES users(id)
                );

                CREATE TABLE IF NOT EXISTS refresh_sessions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    refresh_jti TEXT NOT NULL UNIQUE,
                    issued_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    last_seen TEXT,
                    revoked_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                );

                CREATE TABLE IF NOT EXISTS token_revocations (
                    jti TEXT PRIMARY KEY,
                    token_type TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT,
                    revoked_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    reason TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(session_id) REFERENCES refresh_sessions(id)
                );
                """
            )
            if not _table_has_column(conn, 'jobs', 'attempt'):
                conn.execute('ALTER TABLE jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1')
            if not _table_has_column(conn, 'jobs', 'parent_job_id'):
                conn.execute('ALTER TABLE jobs ADD COLUMN parent_job_id TEXT')
            if not _table_has_column(conn, 'jobs', 'retry_of_job_id'):
                conn.execute('ALTER TABLE jobs ADD COLUMN retry_of_job_id TEXT')
            if not _table_has_column(conn, 'jobs', 'available_at'):
                conn.execute('ALTER TABLE jobs ADD COLUMN available_at TEXT')
            if not _table_has_column(conn, 'jobs', 'lease_owner'):
                conn.execute('ALTER TABLE jobs ADD COLUMN lease_owner TEXT')
            if not _table_has_column(conn, 'jobs', 'lease_expires_at'):
                conn.execute('ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT')
            if not _table_has_column(conn, 'jobs', 'dead_letter_reason'):
                conn.execute('ALTER TABLE jobs ADD COLUMN dead_letter_reason TEXT')
            if not _table_has_column(conn, 'spool_sections', 'attempt'):
                conn.execute('ALTER TABLE spool_sections ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1')
            if not _table_has_column(conn, 'job_events', 'attempt'):
                conn.execute('ALTER TABLE job_events ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1')
            conn.commit()
        finally:
            conn.close()


def upsert_user(username: str, password_hash: str, role: str) -> None:
    now = _utcnow()
    user_id = str(uuid.uuid4())
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                """
                INSERT INTO users (id, username, password_hash, role, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    password_hash = excluded.password_hash,
                    role = excluded.role,
                    updated_at = excluded.updated_at
                """,
                (user_id, username, password_hash, role, now, now),
            )
            conn.commit()
        finally:
            conn.close()


def get_user_by_username(username: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def ensure_identity(user_id: str, provider: str, subject: str) -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO identities (id, user_id, provider, subject, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), user_id, provider, subject, _utcnow()),
            )
            conn.commit()
        finally:
            conn.close()


def create_refresh_session(user_id: str, refresh_jti: str, issued_at: str, expires_at: str) -> dict[str, Any]:
    session_id = str(uuid.uuid4())
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                """
                INSERT INTO refresh_sessions (id, user_id, refresh_jti, issued_at, expires_at, last_seen, revoked_at)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                (session_id, user_id, refresh_jti, issued_at, expires_at, issued_at),
            )
            conn.commit()
            row = conn.execute('SELECT * FROM refresh_sessions WHERE id = ?', (session_id,)).fetchone()
            return dict(row)
        finally:
            conn.close()


def get_refresh_session_by_jti(refresh_jti: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute('SELECT * FROM refresh_sessions WHERE refresh_jti = ?', (refresh_jti,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def rotate_refresh_session(session_id: str, next_refresh_jti: str, last_seen: str) -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                'UPDATE refresh_sessions SET refresh_jti = ?, last_seen = ? WHERE id = ?',
                (next_refresh_jti, last_seen, session_id),
            )
            conn.commit()
        finally:
            conn.close()


def revoke_refresh_session(session_id: str, revoked_at: str) -> None:
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                'UPDATE refresh_sessions SET revoked_at = ?, last_seen = ? WHERE id = ?',
                (revoked_at, revoked_at, session_id),
            )
            conn.commit()
        finally:
            conn.close()


def revoke_token(
    jti: str,
    token_type: str,
    user_id: str,
    session_id: str | None,
    expires_at: str,
    reason: str | None = None,
) -> None:
    revoked_at = _utcnow()
    with _LOCK:
        conn = connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO token_revocations
                (jti, token_type, user_id, session_id, revoked_at, expires_at, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (jti, token_type, user_id, session_id, revoked_at, expires_at, reason),
            )
            conn.commit()
        finally:
            conn.close()


def is_token_revoked(jti: str) -> bool:
    conn = connect()
    try:
        row = conn.execute('SELECT 1 FROM token_revocations WHERE jti = ?', (jti,)).fetchone()
        return row is not None
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
                    id, template_id, submitted_by, input_params_json, attempt, parent_job_id, state,
                    available_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, template_id, submitted_by, json.dumps(params), 1, job_id, 'queued', now, now, now),
            )
            conn.commit()
        finally:
            conn.close()
    add_event(job_id, 'job.created', {'state': 'queued', 'attempt': 1}, attempt=1)
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


def add_event(job_id: str, event_type: str, payload: dict[str, Any], attempt: int | None = None) -> None:
    with _LOCK:
        conn = connect()
        try:
            stored_payload = dict(payload)
            if event_type.startswith('job.') and 'profile' not in stored_payload:
                stored_payload['profile'] = settings.worker_adapter_profile
            event_attempt = attempt
            if event_attempt is None:
                row = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
                event_attempt = int(row['attempt']) if row else 1
            conn.execute(
                'INSERT INTO job_events (job_id, attempt, ts, event_type, payload_json) VALUES (?, ?, ?, ?, ?)',
                (job_id, int(event_attempt), _utcnow(), event_type, json.dumps(stored_payload)),
            )
            conn.commit()
        finally:
            conn.close()


def replace_spool_sections(job_id: str, sections: list[dict[str, Any]], attempt: int | None = None) -> None:
    with _LOCK:
        conn = connect()
        try:
            spool_attempt = attempt
            if spool_attempt is None:
                row = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
                spool_attempt = int(row['attempt']) if row else 1
            conn.execute('DELETE FROM spool_sections WHERE job_id = ? AND attempt = ?', (job_id, int(spool_attempt)))
            conn.executemany(
                'INSERT INTO spool_sections (job_id, attempt, section_type, ordinal, content_text) VALUES (?, ?, ?, ?, ?)',
                [(job_id, int(spool_attempt), s['section_type'], s['ordinal'], s['content_text']) for s in sections],
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
    from common.queue_backend import QueueBackend

    lease = QueueBackend().reserve('legacy-worker', lease_seconds=30)
    if not lease:
        return None
    return lease.job


def get_spool_sections(job_id: str) -> list[dict[str, Any]]:
    conn = connect()
    try:
        job = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
        if not job:
            return []
        rows = conn.execute(
            """
            SELECT attempt, section_type, ordinal, content_text
            FROM spool_sections
            WHERE job_id = ? AND attempt = ?
            ORDER BY ordinal ASC
            """,
            (job_id, int(job['attempt'])),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_spool_sections(
    job_id: str,
    query: str | None = None,
    section_type: str | None = None,
) -> list[dict[str, Any]]:
    conn = connect()
    try:
        job = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
        if not job:
            return []
        clauses = ['job_id = ?', 'attempt = ?']
        params: list[Any] = [job_id, int(job['attempt'])]
        if section_type:
            clauses.append('section_type = ?')
            params.append(section_type.strip().lower())
        if query and query.strip():
            clauses.append('LOWER(content_text) LIKE ?')
            params.append(f"%{query.strip().lower()}%")
        where_sql = ' AND '.join(clauses)
        rows = conn.execute(
            f"""
            SELECT attempt, section_type, ordinal, content_text
            FROM spool_sections
            WHERE {where_sql}
            ORDER BY ordinal ASC
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_job_events(job_id: str) -> list[dict[str, Any]]:
    conn = connect()
    try:
        job = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
        if not job:
            return []
        rows = conn.execute(
            """
            SELECT id, attempt, ts, event_type, payload_json
            FROM job_events
            WHERE job_id = ? AND attempt = ?
            ORDER BY id ASC
            """,
            (job_id, int(job['attempt'])),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item['payload'] = json.loads(item.pop('payload_json'))
            out.append(item)
        return out
    finally:
        conn.close()


def get_job_events_since(job_id: str, after_id: int = 0, limit: int = 100) -> list[dict[str, Any]]:
    safe_after_id = max(0, int(after_id))
    safe_limit = max(1, min(int(limit), 500))
    conn = connect()
    try:
        job = conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()
        if not job:
            return []
        rows = conn.execute(
            """
            SELECT id, attempt, ts, event_type, payload_json
            FROM job_events
            WHERE job_id = ? AND attempt = ? AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (job_id, int(job['attempt']), safe_after_id, safe_limit),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item['payload'] = json.loads(item.pop('payload_json'))
            out.append(item)
        return out
    finally:
        conn.close()


def _insert_event_locked(
    conn: sqlite3.Connection, job_id: str, attempt: int, event_type: str, payload: dict[str, Any]
) -> None:
    conn.execute(
        'INSERT INTO job_events (job_id, attempt, ts, event_type, payload_json) VALUES (?, ?, ?, ?, ?)',
        (job_id, int(attempt), _utcnow(), event_type, json.dumps(payload)),
    )


def transition_job_state(
    job_id: str,
    *,
    to_state: str,
    from_states: set[str],
    extra_fields: dict[str, Any] | None = None,
    expected_worker: str | None = None,
    idempotent: bool = False,
) -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            state = str(job['state'])
            if state == to_state and idempotent:
                return job
            if state not in from_states:
                raise JobTransitionError(
                    code='invalid_transition',
                    message=f"Cannot transition job from state '{state}' to '{to_state}'",
                    state=state,
                )
            if expected_worker is not None and job.get('lease_owner') != expected_worker:
                raise JobTransitionError(
                    code='lease_mismatch',
                    message='Lease ownership mismatch',
                    state=state,
                )
            now = _utcnow()
            updates = {'state': to_state, 'updated_at': now}
            if extra_fields:
                updates.update(extra_fields)
            assignments = ', '.join(f"{name} = ?" for name in updates)
            conn.execute(f'UPDATE jobs SET {assignments} WHERE id = ?', (*updates.values(), job_id))
            _insert_event_locked(
                conn,
                job_id,
                int(job.get('attempt') or 1),
                'job.state',
                {'from_state': state, 'state': to_state, 'attempt': int(job.get('attempt') or 1)},
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def extend_lease(job_id: str, worker_id: str, *, lease_seconds: int) -> bool:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute(
                'SELECT id FROM jobs WHERE id = ? AND state IN (\'reserved\', \'running\') AND lease_owner = ?',
                (job_id, worker_id),
            ).fetchone()
            if not row:
                return False
            expires_at = (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(lease_seconds)))).isoformat()
            now = _utcnow()
            conn.execute(
                'UPDATE jobs SET lease_expires_at = ?, updated_at = ? WHERE id = ?',
                (expires_at, now, job_id),
            )
            _insert_event_locked(
                conn,
                job_id,
                int(conn.execute('SELECT attempt FROM jobs WHERE id = ?', (job_id,)).fetchone()['attempt']),
                'job.heartbeat',
                {'lease_expires_at': expires_at, 'worker_id': worker_id},
            )
            conn.commit()
            return True
        finally:
            conn.close()


def clear_lease(job_id: str, worker_id: str) -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            if job.get('lease_owner') and job.get('lease_owner') != worker_id:
                raise JobTransitionError(
                    code='lease_mismatch',
                    message='Cannot clear a lease owned by another worker',
                    state=str(job.get('state')),
                )
            now = _utcnow()
            conn.execute(
                'UPDATE jobs SET lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE id = ?',
                (now, job_id),
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def cancel_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            state = job['state']
            if state not in _CANCELABLE_STATES:
                raise JobTransitionError(
                    code='invalid_transition',
                    message=f"Cannot cancel job from state '{state}'",
                    state=state,
                )
            now = _utcnow()
            conn.execute(
                """
                UPDATE jobs
                SET state = ?, result = ?, stage = ?, finished_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                ('canceled', 'canceled', 'canceled', now, now, job_id),
            )
            _insert_event_locked(
                conn,
                job_id,
                int(job['attempt']),
                'job.canceled',
                {'from_state': state, 'state': 'canceled', 'attempt': int(job['attempt'])},
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def retry_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            state = job['state']
            result = job['result']
            if state != 'failed' and result not in _FAILURE_RESULTS:
                raise JobTransitionError(
                    code='invalid_transition',
                    message=f"Cannot retry job from state '{state}' with result '{result}'",
                    state=state,
                )
            next_attempt = int(job.get('attempt') or 1) + 1
            now = _utcnow()
            conn.execute(
                """
                UPDATE jobs
                SET attempt = ?, state = ?, result = NULL, mainframe_job_id = NULL, return_code = NULL,
                    abend_code = NULL, error_text = NULL, stage = NULL, started_at = NULL, finished_at = NULL,
                    available_at = ?, lease_owner = NULL, lease_expires_at = NULL, dead_letter_reason = NULL,
                    updated_at = ?, retry_of_job_id = ?
                WHERE id = ?
                """,
                (next_attempt, 'queued', now, now, job_id, job_id),
            )
            _insert_event_locked(
                conn,
                job_id,
                next_attempt,
                'job.retried',
                {
                    'state': 'queued',
                    'attempt': next_attempt,
                    'retry_of_job_id': job_id,
                    'previous_attempt': int(job.get('attempt') or 1),
                },
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def requeue_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        conn = connect()
        try:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            state = job['state']
            if state == 'completed':
                raise JobTransitionError(
                    code='invalid_transition',
                    message="Cannot requeue a completed job without clone semantics",
                    state=state,
                )
            if state not in _REQUEUEABLE_STATES:
                raise JobTransitionError(
                    code='invalid_transition',
                    message=f"Cannot requeue job from state '{state}'",
                    state=state,
                )
            next_attempt = int(job.get('attempt') or 1) + 1
            now = _utcnow()
            conn.execute(
                """
                UPDATE jobs
                SET attempt = ?, state = ?, result = NULL, mainframe_job_id = NULL, return_code = NULL,
                    abend_code = NULL, error_text = NULL, stage = NULL, started_at = NULL, finished_at = NULL,
                    available_at = ?, lease_owner = NULL, lease_expires_at = NULL, dead_letter_reason = NULL,
                    updated_at = ?, retry_of_job_id = NULL
                WHERE id = ?
                """,
                (next_attempt, 'queued', now, now, job_id),
            )
            _insert_event_locked(
                conn,
                job_id,
                next_attempt,
                'job.requeued',
                {
                    'state': 'queued',
                    'attempt': next_attempt,
                    'previous_attempt': int(job.get('attempt') or 1),
                    'from_state': state,
                },
            )
            conn.commit()
            fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(fresh) if fresh else None
        finally:
            conn.close()


def cleanup_old_jobs(retention_days: int, limit: int = 100) -> dict[str, int]:
    safe_retention_days = max(1, int(retention_days))
    safe_limit = max(1, min(int(limit), 5000))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=safe_retention_days)).isoformat()
    with _LOCK:
        conn = connect()
        try:
            rows = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE COALESCE(finished_at, created_at) < ?
                ORDER BY COALESCE(finished_at, created_at) ASC
                LIMIT ?
                """,
                (cutoff, safe_limit),
            ).fetchall()
            job_ids = [row['id'] for row in rows]
            if not job_ids:
                return {'jobs_deleted': 0, 'events_deleted': 0, 'spool_sections_deleted': 0}

            placeholders = ','.join('?' for _ in job_ids)
            spool_deleted = conn.execute(
                f'DELETE FROM spool_sections WHERE job_id IN ({placeholders})',
                job_ids,
            ).rowcount
            events_deleted = conn.execute(
                f'DELETE FROM job_events WHERE job_id IN ({placeholders})',
                job_ids,
            ).rowcount
            jobs_deleted = conn.execute(
                f'DELETE FROM jobs WHERE id IN ({placeholders})',
                job_ids,
            ).rowcount
            conn.commit()
            return {
                'jobs_deleted': int(jobs_deleted or 0),
                'events_deleted': int(events_deleted or 0),
                'spool_sections_deleted': int(spool_deleted or 0),
            }
        finally:
            conn.close()
