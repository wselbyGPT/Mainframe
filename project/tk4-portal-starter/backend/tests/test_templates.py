from __future__ import annotations

import unittest

from common.template_schemas import TemplateSchemaError
from common.templates import TemplateRenderError, render_template, validate_template_params


class TemplateRenderingTests(unittest.TestCase):
    def test_render_hello_world_success(self) -> None:
        jcl = render_template('hello-world', {'job_name': 'hello1', 'message': 'HI TK4'})
        self.assertIn('//HELLO1   JOB', jcl)
        self.assertIn('HI TK4', jcl)

    def test_render_idcams_listcat_success(self) -> None:
        jcl = render_template('idcams-listcat', {'job_name': 'listcat', 'level': 'SYS1'})
        self.assertIn('//LISTCAT  JOB', jcl)
        self.assertIn("LISTCAT LEVEL('SYS1') ALL", jcl)

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


if __name__ == '__main__':
    unittest.main()
