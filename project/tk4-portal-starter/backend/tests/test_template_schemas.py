from __future__ import annotations

import unittest

from common.template_schemas import (
    TemplateSchemaError,
    get_template_catalog,
    get_template_schema,
    normalize_and_validate_template_params,
)


class TemplateSchemaValidationTests(unittest.TestCase):
    def test_hello_world_valid_payload_with_normalization(self) -> None:
        payload = normalize_and_validate_template_params(
            'hello-world',
            {
                'job_name': '  hello1234  ',
                'message': '  hi tk4  ',
            },
        )
        self.assertEqual(payload['job_name'], 'HELLO123')
        self.assertEqual(payload['message'], 'hi tk4')

    def test_hello_world_default_injection(self) -> None:
        payload = normalize_and_validate_template_params('hello-world', {})
        self.assertEqual(payload['job_name'], 'HELLO1')
        self.assertEqual(payload['message'], 'HELLO FROM WEB PORTAL')

    def test_missing_required_field(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            normalize_and_validate_template_params('idcams-listcat', {'job_name': 'LISTCAT'})
        self.assertEqual(exc.exception.code, 'template_params_invalid')
        self.assertEqual(exc.exception.errors[0]['path'], 'params.level')
        self.assertEqual(exc.exception.errors[0]['reason'], 'missing_required_field')

    def test_invalid_job_name_format(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            normalize_and_validate_template_params('hello-world', {'job_name': '1bad', 'message': 'ok'})
        self.assertEqual(exc.exception.errors[0]['path'], 'params.job_name')
        self.assertEqual(exc.exception.errors[0]['reason'], 'invalid_format')

    def test_invalid_type(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            normalize_and_validate_template_params('hello-world', {'job_name': 'HELLO1', 'message': 100})
        self.assertEqual(exc.exception.errors[0]['path'], 'params.message')
        self.assertEqual(exc.exception.errors[0]['reason'], 'invalid_type')


class TemplateSchemaDiscoveryTests(unittest.TestCase):
    def test_catalog_has_all_templates(self) -> None:
        template_ids = {item['template_id'] for item in get_template_catalog()}
        self.assertEqual(
            template_ids,
            {'hello-world', 'idcams-listcat', 'iebgener-copy', 'sort-basic', 'lattice-crypto-demo'},
        )

    def test_get_single_template_schema(self) -> None:
        schema = get_template_schema('sort-basic')
        self.assertEqual(schema['template_id'], 'sort-basic')
        self.assertIn('sort_fields', schema['params'])
        self.assertEqual(schema['params']['sort_fields']['default'], '1,10,CH,A')


if __name__ == '__main__':
    unittest.main()
