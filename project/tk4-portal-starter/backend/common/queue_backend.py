from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from common import db


@dataclass(frozen=True)
class Lease:
    job: dict[str, Any]
    worker_id: str
    lease_expires_at: str


class QueueBackend:
    """SQLite-backed queue semantics using the jobs table."""

    def enqueue(self, job_id: str, *, available_at: str | None = None) -> dict[str, Any] | None:
        now = db._utcnow()
        scheduled_at = available_at or now
        return db.transition_job_state(
            job_id,
            to_state='queued',
            from_states={'queued', 'retryable', 'failed'},
            extra_fields={
                'available_at': scheduled_at,
                'lease_owner': None,
                'lease_expires_at': None,
                'result': None,
                'error_text': None,
                'started_at': None,
                'finished_at': None,
            },
            idempotent=True,
        )

    def reserve(self, worker_id: str, *, lease_seconds: int) -> Lease | None:
        with db._LOCK:
            conn = db.connect()
            try:
                conn.execute('BEGIN IMMEDIATE')
                now = datetime.now(timezone.utc)
                now_iso = now.isoformat()
                row = conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE (
                        (state = 'queued' AND COALESCE(available_at, created_at) <= ?)
                        OR (state = 'retryable' AND COALESCE(available_at, created_at) <= ?)
                        OR (state = 'reserved' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                    )
                    ORDER BY COALESCE(available_at, created_at) ASC
                    LIMIT 1
                    """,
                    (now_iso, now_iso, now_iso),
                ).fetchone()
                if not row:
                    conn.commit()
                    return None

                job = dict(row)
                next_attempt = int(job.get('attempt') or 1)
                if job['state'] == 'retryable':
                    next_attempt += 1
                lease_expires_at = (now + timedelta(seconds=max(1, int(lease_seconds)))).isoformat()
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'reserved',
                        attempt = ?,
                        lease_owner = ?,
                        lease_expires_at = ?,
                        started_at = COALESCE(started_at, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (next_attempt, worker_id, lease_expires_at, now_iso, now_iso, job['id']),
                )
                db._insert_event_locked(
                    conn,
                    job['id'],
                    next_attempt,
                    'job.reserved',
                    {'state': 'reserved', 'attempt': next_attempt, 'worker_id': worker_id},
                )
                conn.commit()
                fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job['id'],)).fetchone()
                if not fresh:
                    return None
                fresh_dict = dict(fresh)
                return Lease(job=fresh_dict, worker_id=worker_id, lease_expires_at=lease_expires_at)
            finally:
                conn.close()

    def mark_running(self, job_id: str, worker_id: str) -> dict[str, Any] | None:
        return db.transition_job_state(
            job_id,
            to_state='running',
            from_states={'reserved', 'running'},
            extra_fields={'lease_owner': worker_id},
            expected_worker=worker_id,
            idempotent=True,
        )

    def ack(self, job_id: str, worker_id: str) -> dict[str, Any] | None:
        return db.clear_lease(job_id, worker_id)

    def heartbeat(self, job_id: str, worker_id: str, *, lease_seconds: int) -> bool:
        return db.extend_lease(job_id, worker_id, lease_seconds=lease_seconds)

    def nack_retry(
        self,
        job_id: str,
        worker_id: str,
        *,
        delay_seconds: int,
        max_attempts: int,
        reason: str,
    ) -> dict[str, Any] | None:
        with db._LOCK:
            conn = db.connect()
            try:
                row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
                if not row:
                    return None
                job = dict(row)
                if job.get('lease_owner') != worker_id:
                    return None
                if job.get('state') in {'canceled', 'completed', 'dead-lettered'}:
                    return job
                attempt = int(job.get('attempt') or 1)
                now = datetime.now(timezone.utc)
                now_iso = now.isoformat()
                if attempt >= max(1, int(max_attempts)):
                    conn.execute(
                        """
                        UPDATE jobs
                        SET state = 'dead-lettered', result = 'failed', stage = 'dead-lettered',
                            lease_owner = NULL, lease_expires_at = NULL,
                            dead_letter_reason = ?, finished_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (reason, now_iso, now_iso, job_id),
                    )
                    db._insert_event_locked(
                        conn,
                        job_id,
                        attempt,
                        'job.dead_lettered',
                        {'state': 'dead-lettered', 'attempt': attempt, 'reason': reason},
                    )
                else:
                    available_at = (now + timedelta(seconds=max(0, int(delay_seconds)))).isoformat()
                    conn.execute(
                        """
                        UPDATE jobs
                        SET state = 'retryable', stage = 'retryable',
                            lease_owner = NULL, lease_expires_at = NULL,
                            available_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (available_at, now_iso, job_id),
                    )
                    db._insert_event_locked(
                        conn,
                        job_id,
                        attempt,
                        'job.retry_scheduled',
                        {
                            'state': 'retryable',
                            'attempt': attempt,
                            'retry_at': available_at,
                            'reason': reason,
                        },
                    )
                conn.commit()
                fresh = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
                return dict(fresh) if fresh else None
            finally:
                conn.close()
