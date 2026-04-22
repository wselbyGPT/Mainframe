from __future__ import annotations

import unittest

from common.template_schemas import TemplateSchemaError
from common.templates import TemplateRenderError, get_template_catalog, render_template, validate_template_params


class TemplateRenderingTests(unittest.TestCase):
    def test_render_hello_world_success(self) -> None:
        jcl = render_template('hello-world', {'job_name': 'hello1', 'message': 'HI TK4'})
        self.assertIn('//HELLO1   JOB', jcl)
        self.assertIn('HI TK4', jcl)

    def test_render_idcams_listcat_success(self) -> None:
        jcl = render_template('idcams-listcat', {'job_name': 'listcat', 'level': 'SYS1'})
        self.assertIn('//LISTCAT  JOB', jcl)
        self.assertIn("LISTCAT LEVEL('SYS1') ALL", jcl)

    def test_render_lattice_crypto_demo_success(self) -> None:
        jcl = render_template(
            'lattice-crypto-demo',
            {
                'job_name': 'pqcjob1',
                'algorithm': 'CRYSTALS-KYBER',
                'security_level': 'LEVEL3',
                'key_dataset': 'IBMUSER.PQC.KEYS',
                'notes': 'PILOT',
            },
        )
        self.assertIn('//PQCJOB1  JOB', jcl)
        self.assertIn('ALGORITHM=CRYSTALS-KYBER', jcl)
        self.assertIn('KEY_DATASET=IBMUSER.PQC.KEYS', jcl)

    def test_render_unknown_template_deterministic_error(self) -> None:
        with self.assertRaises(TemplateRenderError) as exc:
            render_template('unknown-template', {})
        self.assertEqual(exc.exception.code, 'unknown_template_id')
        self.assertEqual(str(exc.exception), "Unknown template_id 'unknown-template'")


class TemplateValidationTests(unittest.TestCase):
    def test_missing_required_parameter(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            validate_template_params('idcams-listcat', {'job_name': 'LISTCAT'})
        self.assertEqual(exc.exception.errors[0]['path'], 'params.level')

    def test_bad_job_name_format(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            validate_template_params('hello-world', {'job_name': '1BAD', 'message': 'X'})
        self.assertEqual(exc.exception.errors[0]['path'], 'params.job_name')

    def test_lattice_crypto_requires_key_dataset(self) -> None:
        with self.assertRaises(TemplateSchemaError) as exc:
            validate_template_params('lattice-crypto-demo', {'job_name': 'PQCDEMO'})
        self.assertEqual(exc.exception.errors[0]['path'], 'params.key_dataset')

    def test_flat_catalog_response_is_backward_compatible(self) -> None:
        catalog = get_template_catalog()
        hello = next(item for item in catalog if item['template_id'] == 'hello-world')
        self.assertIn('params', hello)
        self.assertNotIn('operations_pack', hello)

    def test_catalog_can_include_pack_metadata(self) -> None:
        catalog = get_template_catalog(include_pack_metadata=True)
        hello = next(item for item in catalog if item['template_id'] == 'hello-world')
        self.assertIn('operations_pack', hello)
        self.assertIn('template_param_overrides', hello)
        self.assertIn('template', hello['compatibility'])


if __name__ == '__main__':
    unittest.main()
