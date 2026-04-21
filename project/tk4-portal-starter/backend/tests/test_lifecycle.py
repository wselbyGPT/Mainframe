from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - environment dependency
    TestClient = None

from common import db
from common.config import settings


class LifecycleDbTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        object.__setattr__(settings, 'database_path', str(Path(self._tmp.name) / 'jobs.sqlite3'))
        db.init_db()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cancel_atomic_allows_single_winner(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        outcomes: list[str] = []
        lock = threading.Lock()

        def _cancel() -> None:
            try:
                result = db.cancel_job(job['id'])
                with lock:
                    outcomes.append('ok' if result else 'missing')
            except db.JobTransitionError:
                with lock:
                    outcomes.append('conflict')

        threads = [threading.Thread(target=_cancel) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(outcomes.count('ok'), 1)
        self.assertEqual(outcomes.count('conflict'), 7)

    def test_retry_resets_execution_fields_and_increments_attempt(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        db.update_job(
            job['id'],
            state='failed',
            result='error',
            started_at='2026-01-01T00:00:00+00:00',
            finished_at='2026-01-01T00:01:00+00:00',
            mainframe_job_id='JOB11111',
            return_code='0008',
            abend_code='S0C7',
            error_text='boom',
            stage='waiting_for_completion',
        )

        retried = db.retry_job(job['id'])
        assert retried is not None
        self.assertEqual(retried['attempt'], 2)
        self.assertEqual(retried['state'], 'queued')
        self.assertIsNone(retried['started_at'])
        self.assertIsNone(retried['finished_at'])
        self.assertIsNone(retried['mainframe_job_id'])
        self.assertIsNone(retried['return_code'])
        self.assertIsNone(retried['abend_code'])
        self.assertIsNone(retried['error_text'])
        self.assertIsNone(retried['stage'])

    def test_requeue_rejects_completed(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        db.update_job(job['id'], state='completed', result='success')
        with self.assertRaises(db.JobTransitionError):
            db.requeue_job(job['id'])

    def test_cleanup_old_jobs_removes_expired_artifacts(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        db.replace_spool_sections(job['id'], [{'section_type': 'jes', 'ordinal': 1, 'content_text': 'DONE'}])
        old_finished_at = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        db.update_job(job['id'], state='completed', result='success', finished_at=old_finished_at)

        cleanup = db.cleanup_old_jobs(retention_days=30, limit=50)
        self.assertEqual(cleanup['jobs_deleted'], 1)
        self.assertEqual(db.get_job(job['id']), None)


@unittest.skipIf(TestClient is None, 'fastapi is not installed in this execution environment')
class LifecycleApiTests(unittest.TestCase):
    def setUp(self) -> None:
        from app.main import app

        self._tmp = tempfile.TemporaryDirectory()
        object.__setattr__(settings, 'database_path', str(Path(self._tmp.name) / 'jobs.sqlite3'))
        db.init_db()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        self._tmp.cleanup()

    def _auth_headers(self, username: str = 'alice', password: str = 'alice-pass') -> dict[str, str]:
        response = self.client.post('/api/login', json={'username': username, 'password': password})
        self.assertEqual(response.status_code, 200)
        token = response.json()['access_token']
        return {'Authorization': f'Bearer {token}'}

    def test_cancel_endpoint_conflict_from_completed(self) -> None:
        job = db.create_job('hello-world', 'alice', {'message': 'hello'})
        db.update_job(job['id'], state='completed', result='success')

        response = self.client.post(f"/api/jobs/{job['id']}/cancel", headers=self._auth_headers())
        self.assertEqual(response.status_code, 409)
        detail = response.json()['detail']
        self.assertEqual(detail['code'], 'invalid_transition')

    def test_retry_endpoint_returns_attempt_info_and_event_summary(self) -> None:
        job = db.create_job('hello-world', 'alice', {'message': 'hello'})
        db.update_job(job['id'], state='failed', result='error', stage='unexpected')

        response = self.client.post(f"/api/jobs/{job['id']}/retry", headers=self._auth_headers())
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['state'], 'queued')
        self.assertEqual(payload['attempt_info']['attempt'], 2)
        self.assertEqual(payload['events'][-1]['event_type'], 'job.retried')
        self.assertEqual(payload['event_summary']['last_event'], 'job.retried')

    def test_requeue_endpoint_unknown_job_404(self) -> None:
        response = self.client.post('/api/jobs/does-not-exist/requeue', headers=self._auth_headers())
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['detail']['code'], 'job_not_found')

    def test_lifecycle_endpoints_require_authentication(self) -> None:
        job = db.create_job('hello-world', 'alice', {'message': 'hello'})
        response = self.client.post(f"/api/jobs/{job['id']}/cancel")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()['detail']['code'], 'auth_required')

    def test_lifecycle_endpoints_enforce_job_ownership(self) -> None:
        job = db.create_job('hello-world', 'alice', {'message': 'hello'})
        response = self.client.post(f"/api/jobs/{job['id']}/cancel", headers=self._auth_headers(username='bob', password='bob-pass'))
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()['detail']['code'], 'forbidden')

    def test_lifecycle_endpoints_allow_admin_override(self) -> None:
        job = db.create_job('hello-world', 'alice', {'message': 'hello'})
        response = self.client.post(f"/api/jobs/{job['id']}/cancel", headers=self._auth_headers(username='admin', password='admin-pass'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['state'], 'canceled')


class WorkerCancellationRaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        object.__setattr__(settings, 'database_path', str(Path(self._tmp.name) / 'jobs.sqlite3'))
        db.init_db()
        object.__setattr__(settings, 'dry_run', True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cancel_during_execution_marks_job_canceled(self) -> None:
        from worker.runner import run_job

        queued = db.create_job('hello-world', 'tester', {'message': 'hello', 'job_name': 'HELLO1'})
        claimed = db.next_queued_job()
        self.assertIsNotNone(claimed)

        errors: list[Exception] = []

        def _run() -> None:
            try:
                run_job(claimed)
            except Exception as exc:  # pragma: no cover - diagnostic safety
                errors.append(exc)

        t = threading.Thread(target=_run)
        t.start()
        db.cancel_job(queued['id'])
        t.join(timeout=5)

        self.assertFalse(errors)
        final_job = db.get_job(queued['id'])
        assert final_job is not None
        self.assertEqual(final_job['state'], 'canceled')
        events = db.get_job_events(queued['id'])
        self.assertTrue(any(item['event_type'] == 'job.canceled' for item in events))


if __name__ == '__main__':
    unittest.main()
