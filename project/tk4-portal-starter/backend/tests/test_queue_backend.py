from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

from common import db
from common.config import settings
from common.queue_backend import QueueBackend


def _setup_temp_db() -> tempfile.TemporaryDirectory[str]:
    tmp = tempfile.TemporaryDirectory()
    object.__setattr__(settings, 'database_path', str(Path(tmp.name) / 'jobs.sqlite3'))
    db.init_db()
    return tmp


def test_concurrent_consumers_do_not_double_reserve() -> None:
    tmp = _setup_temp_db()
    try:
        job = db.create_job('hello-world', 'tester', {'message': 'hello', 'job_name': 'HELLO1'})
        queue = QueueBackend()
        winners: list[str] = []
        lock = threading.Lock()

        def _reserve(worker_id: str) -> None:
            lease = queue.reserve(worker_id, lease_seconds=10)
            if lease:
                with lock:
                    winners.append(lease.job['id'])

        threads = [threading.Thread(target=_reserve, args=(f'w{i}',)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert winners.count(job['id']) == 1
    finally:
        tmp.cleanup()


def test_lease_expiry_allows_recovery() -> None:
    tmp = _setup_temp_db()
    try:
        job = db.create_job('hello-world', 'tester', {'message': 'hello', 'job_name': 'HELLO1'})
        queue = QueueBackend()
        first = queue.reserve('worker-a', lease_seconds=1)
        assert first is not None
        assert first.job['id'] == job['id']

        time.sleep(1.2)

        recovered = queue.reserve('worker-b', lease_seconds=10)
        assert recovered is not None
        assert recovered.job['id'] == job['id']
        assert recovered.job['lease_owner'] == 'worker-b'
    finally:
        tmp.cleanup()


def test_retry_backoff_schedules_and_increments_attempt() -> None:
    tmp = _setup_temp_db()
    try:
        db.create_job('hello-world', 'tester', {'message': 'hello', 'job_name': 'HELLO1'})
        queue = QueueBackend()
        lease = queue.reserve('worker-a', lease_seconds=10)
        assert lease is not None

        queue.nack_retry(
            lease.job['id'],
            'worker-a',
            delay_seconds=1,
            max_attempts=3,
            reason='boom',
        )

        immediate = queue.reserve('worker-b', lease_seconds=10)
        assert immediate is None

        time.sleep(1.1)
        retried = queue.reserve('worker-b', lease_seconds=10)
        assert retried is not None
        assert retried.job['attempt'] == 2
    finally:
        tmp.cleanup()


def test_cancellation_race_wins_over_retry_schedule() -> None:
    tmp = _setup_temp_db()
    try:
        job = db.create_job('hello-world', 'tester', {'message': 'hello', 'job_name': 'HELLO1'})
        queue = QueueBackend()
        lease = queue.reserve('worker-a', lease_seconds=10)
        assert lease is not None

        db.cancel_job(job['id'])
        queue.nack_retry(job['id'], 'worker-a', delay_seconds=0, max_attempts=3, reason='late-failure')

        final = db.get_job(job['id'])
        assert final is not None
        assert final['state'] == 'canceled'
    finally:
        tmp.cleanup()
