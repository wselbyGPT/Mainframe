from __future__ import annotations

import json
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
        hello_world = next(item for item in payload['templates'] if item['template_id'] == 'hello-world')
        self.assertEqual(hello_world['params']['job_name']['default'], 'HELLO1')

    def test_get_template_details(self) -> None:
        response = self.client.get('/api/templates/hello-world')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['template_id'], 'hello-world')
        self.assertIn('message', payload['params'])

    def test_post_jobs_rejects_unknown_template(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={'template_id': 'does-not-exist', 'submitted_by': 'tester', 'params': {}},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json()['detail']
        self.assertEqual(detail['code'], 'unknown_template_id')
        self.assertEqual(detail['errors'][0]['path'], 'template_id')

    def test_post_jobs_validation_422_structure(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={'template_id': 'idcams-listcat', 'submitted_by': 'tester', 'params': {'job_name': 'listcat'}},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json()['detail']
        self.assertEqual(detail['code'], 'template_params_invalid')
        self.assertEqual(detail['errors'][0]['path'], 'params.level')
        self.assertEqual(detail['errors'][0]['reason'], 'missing_required_field')

    def test_post_jobs_persists_normalized_params(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={
                'template_id': 'hello-world',
                'submitted_by': 'tester',
                'params': {'job_name': 'hello9999', 'message': '  hello client  '},
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        stored = json.loads(payload['input_params_json'])
        self.assertEqual(stored['job_name'], 'HELLO999')
        self.assertEqual(stored['message'], 'hello client')

    def test_post_jobs_hello_world_defaults_regression(self) -> None:
        response = self.client.post('/api/jobs', json={'params': {'message': 'DEFAULT FLOW'}})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['template_id'], 'hello-world')
        self.assertEqual(payload['submitted_by'], 'anonymous')
        stored = json.loads(payload['input_params_json'])
        self.assertEqual(stored['job_name'], 'HELLO1')
        self.assertEqual(stored['message'], 'DEFAULT FLOW')


if __name__ == '__main__':
    unittest.main()
