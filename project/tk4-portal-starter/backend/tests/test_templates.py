from __future__ import annotations

import unittest

from common.templates import TemplateValidationError, render_template, validate_template_params


class TemplateRenderingTests(unittest.TestCase):
    def test_render_hello_world_success(self) -> None:
        jcl = render_template('hello-world', {'job_name': 'hello1', 'message': 'HI TK4'})
        self.assertIn('//HELLO1   JOB', jcl)
        self.assertIn('HI TK4', jcl)

    def test_render_idcams_listcat_success(self) -> None:
        jcl = render_template('idcams-listcat', {'job_name': 'listcat', 'level': 'SYS1'})
        self.assertIn('//LISTCAT  JOB', jcl)
        self.assertIn("LISTCAT LEVEL('SYS1') ALL", jcl)

    def test_render_iebgener_copy_success(self) -> None:
        jcl = render_template(
            'iebgener-copy',
            {
                'job_name': 'copyjob',
                'input_dataset': 'SYS1.PROCLIB',
                'output_dataset': 'IBMUSER.PROCLIB',
            },
        )
        self.assertIn('//COPYJOB  JOB', jcl)
        self.assertIn('//COPY     EXEC PGM=IEBGENER', jcl)
        self.assertIn('//SYSUT1   DD DSN=SYS1.PROCLIB,DISP=SHR', jcl)
        self.assertIn('//SYSUT2   DD DSN=IBMUSER.PROCLIB,DISP=SHR', jcl)

    def test_render_sort_basic_success(self) -> None:
        jcl = render_template(
            'sort-basic',
            {
                'job_name': 'sortjob',
                'input_dataset': 'IBMUSER.INPUT',
                'output_dataset': 'IBMUSER.OUTPUT',
                'sort_fields': '1,8,CH,A',
            },
        )
        self.assertIn('//SORTJOB  JOB', jcl)
        self.assertIn('//SORTSTEP EXEC PGM=SORT', jcl)
        self.assertIn('SORT FIELDS=(1,8,CH,A)', jcl)


class TemplateValidationTests(unittest.TestCase):
    def test_missing_required_parameter(self) -> None:
        with self.assertRaises(TemplateValidationError) as exc:
            validate_template_params('idcams-listcat', {'job_name': 'LISTCAT'})
        self.assertIn("Missing required parameter 'level'", str(exc.exception))

    def test_bad_job_name_format(self) -> None:
        with self.assertRaises(TemplateValidationError) as exc:
            validate_template_params('hello-world', {'job_name': '1BAD', 'message': 'X'})
        self.assertIn("Invalid parameter 'job_name'", str(exc.exception))

    def test_bad_job_name_length(self) -> None:
        with self.assertRaises(TemplateValidationError) as exc:
            validate_template_params('hello-world', {'job_name': 'ABCDEFGHI', 'message': 'X'})
        self.assertIn('at most 8 characters', str(exc.exception))

    def test_unknown_template(self) -> None:
        with self.assertRaises(TemplateValidationError) as exc:
            validate_template_params('unknown-template', {})
        self.assertIn("Unknown template_id 'unknown-template'", str(exc.exception))


if __name__ == '__main__':
    unittest.main()
