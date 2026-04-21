from __future__ import annotations

import unittest

from common.jcl_lint import lint_jcl
from common.template_schemas import get_template_catalog
from common.templates import render_template


class JclLintRuleTests(unittest.TestCase):
    def test_rejects_missing_job_card(self) -> None:
        issues = lint_jcl('//STEP1 EXEC PGM=IEFBR14')
        self.assertTrue(any(issue.code == 'missing_job_card' for issue in issues))

    def test_rejects_line_over_80_columns(self) -> None:
        long_line = '//HELLO1   JOB ' + ('X' * 70)
        issues = lint_jcl(long_line)
        self.assertTrue(any(issue.code == 'line_too_long' for issue in issues))


class JclTemplateGateTests(unittest.TestCase):
    def test_all_starter_templates_pass_lint(self) -> None:
        for schema in get_template_catalog():
            template_id = schema['template_id']
            params: dict[str, str] = {}
            for param_name, meta in schema['params'].items():
                if 'default' in meta:
                    continue
                if meta.get('required'):
                    params[param_name] = meta.get('examples', ['VALUE'])[0]

            jcl = render_template(template_id, params)
            issues = lint_jcl(jcl)
            self.assertEqual([], issues, f'{template_id} has lint issues: {issues}')


if __name__ == '__main__':
    unittest.main()
