from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - environment dependency
    TestClient = None


@unittest.skipIf(TestClient is None, 'fastapi is not installed in this execution environment')
class TemplateApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from app.main import app
        from common import db
        from common.config import settings

        cls._tmp = tempfile.TemporaryDirectory()
        cls._db_path = str(Path(cls._tmp.name) / 'jobs.sqlite3')
        object.__setattr__(settings, 'database_path', cls._db_path)
        db.init_db()
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls._tmp.cleanup()

    def test_get_templates_catalog(self) -> None:
        response = self.client.get('/api/templates')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        template_ids = {item['template_id'] for item in payload['templates']}
        self.assertEqual(template_ids, {'hello-world', 'idcams-listcat', 'iebgener-copy', 'sort-basic'})

    def test_post_jobs_rejects_unknown_template(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={'template_id': 'does-not-exist', 'submitted_by': 'tester', 'params': {}},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn('Unknown template_id', response.json()['detail'])

    def test_post_jobs_keeps_defaults(self) -> None:
        response = self.client.post('/api/jobs', json={'params': {'message': 'DEFAULT FLOW'}})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['template_id'], 'hello-world')
        self.assertEqual(payload['submitted_by'], 'anonymous')


if __name__ == '__main__':
    unittest.main()
