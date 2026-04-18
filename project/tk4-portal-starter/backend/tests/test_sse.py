from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - environment dependency
    TestClient = None

from common import db
from common.config import settings


def _read_sse_events(response: object, target_count: int) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    current: dict[str, object] = {}
    for raw_line in response.iter_lines():
        line = raw_line.decode() if isinstance(raw_line, bytes) else raw_line
        if line == '':
            if current:
                events.append(current)
                current = {}
                if len(events) >= target_count:
                    break
            continue
        if line.startswith(':'):
            continue
        key, _, value = line.partition(':')
        payload = value.lstrip()
        if key == 'id':
            current['id'] = int(payload)
        elif key == 'event':
            current['event'] = payload
        elif key == 'data':
            current['data'] = json.loads(payload)
    return events


class JobEventCursorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        object.__setattr__(settings, 'database_path', str(Path(self._tmp.name) / 'jobs.sqlite3'))
        db.init_db()
        self.job = db.create_job('hello-world', 'tester', {'message': 'hello'})

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_get_job_events_since_cursor_ordering(self) -> None:
        db.add_event(self.job['id'], 'job.running', {'state': 'running', 'step': 1})
        db.add_event(self.job['id'], 'job.running', {'state': 'running', 'step': 2})
        db.add_event(self.job['id'], 'job.completed', {'state': 'completed'})

        first_batch = db.get_job_events_since(self.job['id'], after_id=0, limit=2)
        self.assertEqual(len(first_batch), 2)
        self.assertLess(first_batch[0]['id'], first_batch[1]['id'])

        second_batch = db.get_job_events_since(self.job['id'], after_id=first_batch[-1]['id'], limit=10)
        self.assertTrue(second_batch)
        for item in second_batch:
            self.assertGreater(item['id'], first_batch[-1]['id'])
        combined_ids = [item['id'] for item in first_batch + second_batch]
        self.assertEqual(combined_ids, sorted(combined_ids))


@unittest.skipIf(TestClient is None, 'fastapi is not installed in this execution environment')
class JobEventSseApiTests(unittest.TestCase):
    def setUp(self) -> None:
        from app.main import app

        self._tmp = tempfile.TemporaryDirectory()
        object.__setattr__(settings, 'database_path', str(Path(self._tmp.name) / 'jobs.sqlite3'))
        db.init_db()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        self._tmp.cleanup()

    def test_stream_returns_backlog_then_new_events_in_order(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        db.add_event(job['id'], 'job.running', {'state': 'running', 'progress': 10})

        delayed = threading.Timer(0.1, lambda: db.add_event(job['id'], 'job.running', {'state': 'running', 'progress': 20}))
        delayed.start()
        try:
            with self.client.stream('GET', f"/api/jobs/{job['id']}/events/stream") as response:
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers['content-type'].split(';')[0], 'text/event-stream')
                events = _read_sse_events(response, target_count=3)
        finally:
            delayed.cancel()

        event_ids = [item['id'] for item in events]
        self.assertEqual(event_ids, sorted(event_ids))
        payloads = [item['data'] for item in events]
        self.assertEqual(payloads[-1]['progress'], 20)

    def test_stream_resume_with_last_event_id_has_no_duplicates(self) -> None:
        job = db.create_job('hello-world', 'tester', {'message': 'hello'})
        db.add_event(job['id'], 'job.running', {'state': 'running', 'progress': 10})
        db.add_event(job['id'], 'job.running', {'state': 'running', 'progress': 20})
        db.add_event(job['id'], 'job.completed', {'state': 'completed'})

        with self.client.stream('GET', f"/api/jobs/{job['id']}/events/stream") as response:
            first_events = _read_sse_events(response, target_count=2)
        first_ids = [item['id'] for item in first_events]

        last_seen = str(first_ids[-1])
        with self.client.stream(
            'GET',
            f"/api/jobs/{job['id']}/events/stream",
            headers={'Last-Event-ID': last_seen},
        ) as response:
            second_events = _read_sse_events(response, target_count=2)
        second_ids = [item['id'] for item in second_events]

        self.assertTrue(first_ids)
        self.assertTrue(second_ids)
        self.assertTrue(all(item_id > first_ids[-1] for item_id in second_ids))
        self.assertEqual(len(set(first_ids) & set(second_ids)), 0)


if __name__ == '__main__':
    unittest.main()
