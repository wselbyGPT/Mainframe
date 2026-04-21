from __future__ import annotations

import re
from dataclasses import dataclass

_CARD_NAME_RE = re.compile(r'^[A-Z$#@][A-Z0-9$#@]{0,7}$')


@dataclass(frozen=True)
class JclLintIssue:
    line: int
    code: str
    message: str


def lint_jcl(text: str) -> list[JclLintIssue]:
    """Return starter JCL lint issues.

    Rules are intentionally small and conservative so the gate is easy to adopt:
    - max line length is 80 columns
    - tabs are not allowed
    - first non-empty line must be a JOB card
    - JCL control statements must have a valid 1-8 char card name
    """

    issues: list[JclLintIssue] = []
    lines = text.splitlines()

    first_non_empty_idx = next((idx for idx, raw in enumerate(lines) if raw.strip()), None)
    if first_non_empty_idx is None:
        return [JclLintIssue(line=1, code='empty_jcl', message='JCL content is empty')]

    first_line = lines[first_non_empty_idx].rstrip()
    if not first_line.startswith('//') or ' JOB ' not in first_line.upper():
        issues.append(
            JclLintIssue(
                line=first_non_empty_idx + 1,
                code='missing_job_card',
                message='First non-empty line must be a JOB card (//NAME JOB ...)',
            )
        )

    in_stream_data = False

    for idx, raw in enumerate(lines, start=1):
        line = raw.rstrip('\n')

        if '\t' in line:
            issues.append(JclLintIssue(line=idx, code='tab_character', message='Tab characters are not allowed in JCL'))

        if len(line) > 80:
            issues.append(
                JclLintIssue(
                    line=idx,
                    code='line_too_long',
                    message=f'JCL line exceeds 80 columns ({len(line)} chars)',
                )
            )

        if not line.strip():
            continue

        if in_stream_data:
            if line.strip().startswith('/*'):
                in_stream_data = False
            continue

        stripped = line.lstrip()
        if not stripped.startswith('//'):
            issues.append(
                JclLintIssue(
                    line=idx,
                    code='invalid_prefix',
                    message='JCL control statements must begin with //',
                )
            )
            continue

        card = stripped[2:].split(maxsplit=1)[0] if stripped[2:].strip() else ''
        if card and not _CARD_NAME_RE.fullmatch(card):
            issues.append(
                JclLintIssue(
                    line=idx,
                    code='invalid_card_name',
                    message=f"Invalid card name '{card}' (expected 1-8 chars A-Z0-9#$@)",
                )
            )

        if ' DD *' in stripped.upper() or stripped.upper().endswith(' DD *'):
            in_stream_data = True

    return issues


def assert_jcl_lint_clean(text: str) -> None:
    issues = lint_jcl(text)
    if not issues:
        return

    joined = '; '.join(f"line {issue.line}: {issue.code} ({issue.message})" for issue in issues)
    raise ValueError(f'JCL lint failed: {joined}')


__all__ = ['JclLintIssue', 'assert_jcl_lint_clean', 'lint_jcl']
