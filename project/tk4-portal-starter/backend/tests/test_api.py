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

    def _auth_headers(self, username: str = 'alice', password: str = 'alice-pass') -> dict[str, str]:
        response = self.client.post('/api/login', json={'username': username, 'password': password})
        self.assertEqual(response.status_code, 200)
        token = response.json()['access_token']
        return {'Authorization': f'Bearer {token}'}

    def test_get_templates_catalog(self) -> None:
        response = self.client.get('/api/templates')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        template_ids = {item['template_id'] for item in payload['templates']}
        self.assertEqual(
            template_ids,
            {'hello-world', 'idcams-listcat', 'iebgener-copy', 'sort-basic', 'lattice-crypto-demo'},
        )
        hello_world = next(item for item in payload['templates'] if item['template_id'] == 'hello-world')
        self.assertEqual(hello_world['params']['job_name']['default'], 'HELLO1')

    def test_index_serves_web_ui(self) -> None:
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/html', response.headers['content-type'])
        self.assertIn('TK4 Portal', response.text)

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
            headers=self._auth_headers(),
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json()['detail']
        self.assertEqual(detail['code'], 'unknown_template_id')
        self.assertEqual(detail['errors'][0]['path'], 'template_id')

    def test_post_jobs_validation_422_structure(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={'template_id': 'idcams-listcat', 'submitted_by': 'tester', 'params': {'job_name': 'listcat'}},
            headers=self._auth_headers(),
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
            headers=self._auth_headers(),
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        stored = json.loads(payload['input_params_json'])
        self.assertEqual(stored['job_name'], 'HELLO999')
        self.assertEqual(stored['message'], 'hello client')

    def test_post_jobs_hello_world_defaults_regression(self) -> None:
        response = self.client.post(
            '/api/jobs',
            json={'params': {'message': 'DEFAULT FLOW'}},
            headers=self._auth_headers(),
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['template_id'], 'hello-world')
        self.assertEqual(payload['submitted_by'], 'alice')
        stored = json.loads(payload['input_params_json'])
        self.assertEqual(stored['job_name'], 'HELLO1')
        self.assertEqual(stored['message'], 'DEFAULT FLOW')

    def test_post_jobs_requires_authentication(self) -> None:
        response = self.client.post('/api/jobs', json={'params': {'message': 'no auth'}})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()['detail']['code'], 'auth_required')

    def test_get_job_details_includes_stage_timeline_and_artifact_links(self) -> None:
        created = self.client.post(
            '/api/jobs',
            json={
                'template_id': 'hello-world',
                'submitted_by': 'tester',
                'params': {'job_name': 'hello9999', 'message': 'hello'},
            },
            headers=self._auth_headers(),
        )
        self.assertEqual(created.status_code, 200)
        job_id = created.json()['id']

        response = self.client.get(f'/api/jobs/{job_id}')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['normalized_params']['job_name'], 'HELLO999')
        self.assertEqual(payload['stage_model']['current'], 'queued')
        self.assertEqual(payload['stage_model']['timeline'][0]['stage'], 'queued')
        self.assertIn('durations_ms', payload['stage_model'])
        self.assertEqual(payload['artifact_links']['spool'], f'/api/jobs/{job_id}/spool')
        self.assertEqual(payload['artifact_links']['spool_text'], f'/api/jobs/{job_id}/spool/text')
        self.assertIn('jes', payload['artifact_links']['spool_sections'])

    def test_spool_endpoints_support_search_and_text_download(self) -> None:
        from common import db

        created = self.client.post('/api/jobs', json={'params': {'message': 'spool test'}}, headers=self._auth_headers())
        self.assertEqual(created.status_code, 200)
        job_id = created.json()['id']
        db.replace_spool_sections(
            job_id,
            [
                {'section_type': 'jes', 'ordinal': 1, 'content_text': 'JOB COMPLETED - RC=0000'},
                {'section_type': 'sysout', 'ordinal': 2, 'content_text': 'HELLO WORLD'},
            ],
        )

        filtered = self.client.get(f'/api/jobs/{job_id}/spool', params={'query': 'rc=0000', 'section_type': 'jes'})
        self.assertEqual(filtered.status_code, 200)
        payload = filtered.json()
        self.assertEqual(len(payload['sections']), 1)
        self.assertEqual(payload['sections'][0]['section_type'], 'jes')

        download = self.client.get(f'/api/jobs/{job_id}/spool/text', params={'query': 'hello'})
        self.assertEqual(download.status_code, 200)
        self.assertIn('text/plain', download.headers['content-type'])
        self.assertIn('attachment; filename=', download.headers['content-disposition'])
        self.assertIn('HELLO WORLD', download.text)

    def test_ops_dashboard_exposes_health_and_stage_metrics(self) -> None:
        created = self.client.post('/api/jobs', json={'params': {'message': 'ops dashboard'}}, headers=self._auth_headers())
        self.assertEqual(created.status_code, 200)
        job_id = created.json()['id']

        from common import db

        db.update_job(job_id, state='completed', result='success', stage='done', finished_at='2026-01-01T00:00:04+00:00')
        db.add_event(job_id, 'job.state', {'state': 'logging_in', 'attempt': 1, 'stage': 'logging_in'})
        db.add_event(job_id, 'job.state', {'state': 'reading_spool', 'attempt': 1, 'stage': 'reading_spool'})

        response = self.client.get('/api/ops/dashboard')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn('health', payload)
        self.assertIn('stage_metrics', payload)
        self.assertIn('status', payload['health'])


if __name__ == '__main__':
    unittest.main()
