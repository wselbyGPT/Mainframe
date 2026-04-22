from __future__ import annotations

import unittest

from common.template_schemas import (
    TemplateSchemaError,
    get_template_catalog,
    get_template_schema,
    normalize_and_validate_template_params,
    validate_template_pack_structure,
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

    def test_catalog_grouped_pack_discovery(self) -> None:
        packs = get_template_catalog(grouped=True, include_pack_metadata=True)
        self.assertEqual(len(packs), 1)
        pack = packs[0]
        self.assertEqual(pack['operations_pack_id'], 'ops-core')
        template_ids = {item['template_id'] for item in pack['templates']}
        self.assertIn('hello-world', template_ids)
        self.assertIn('compatibility', pack)

    def test_get_single_template_schema(self) -> None:
        schema = get_template_schema('sort-basic')
        self.assertEqual(schema['template_id'], 'sort-basic')
        self.assertIn('sort_fields', schema['params'])
        self.assertEqual(schema['params']['sort_fields']['default'], '1,10,CH,A')

    def test_template_schema_can_include_pack_metadata(self) -> None:
        schema = get_template_schema('sort-basic', include_pack_metadata=True)
        self.assertEqual(schema['operations_pack']['operations_pack_id'], 'ops-core')
        self.assertIn('target_profiles', schema['compatibility'])

    def test_inherited_defaults_and_template_override(self) -> None:
        hello = get_template_schema('hello-world')
        self.assertEqual(hello['params']['job_name']['format'], 'jcl_job_name')
        self.assertEqual(hello['params']['job_name']['default'], 'HELLO1')

    def test_nested_pack_template_validation_errors(self) -> None:
        invalid_catalog = [
            {
                'operations_pack_id': 'ops-invalid',
                'version': '0.1.0',
                'description': 'broken',
                'params': {
                    'job_name': {'type': 'string', 'required': False},
                },
                'templates': [
                    {
                        'template_id': 'bad-template',
                        'description': 'broken template',
                        'params': {
                            'job_name': 'not-a-dict',
                        },
                    }
                ],
            }
        ]
        with self.assertRaises(TemplateSchemaError) as exc:
            validate_template_pack_structure(invalid_catalog)
        self.assertEqual(exc.exception.code, 'template_catalog_invalid')
        self.assertEqual(exc.exception.errors[0]['path'], 'packs[0].templates[0].params.job_name')

    def test_backward_compatible_flat_catalog_shape(self) -> None:
        schema = get_template_catalog()[0]
        self.assertIn('template_id', schema)
        self.assertIn('description', schema)
        self.assertIn('params', schema)
        self.assertNotIn('operations_pack', schema)


if __name__ == '__main__':
    unittest.main()
