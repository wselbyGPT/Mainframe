from __future__ import annotations

import re


IEF_RE = re.compile(r'\b(IEF|IEB|IEC|ICH|IKJ|HASP|JES)\w*', re.IGNORECASE)


def split_spool(raw: str) -> list[dict[str, object]]:
    lines = raw.splitlines()
    jes_lines: list[str] = []
    jcl_lines: list[str] = []
    sysout_lines: list[str] = []

    mode = 'jes'
    saw_jcl = False
    for line in lines:
        stripped = line.rstrip()
        if stripped.startswith('//'):
            mode = 'jcl'
            saw_jcl = True
            jcl_lines.append(stripped)
            continue

        if mode == 'jcl' and stripped and not stripped.startswith('//'):
            mode = 'sysout'

        if mode == 'jes':
            jes_lines.append(stripped)
        elif mode == 'jcl':
            jcl_lines.append(stripped)
        else:
            sysout_lines.append(stripped)

    if not saw_jcl:
        maybe_jes = []
        maybe_sysout = []
        for line in lines:
            if IEF_RE.search(line):
                maybe_jes.append(line)
            else:
                maybe_sysout.append(line)
        jes_lines = maybe_jes
        sysout_lines = maybe_sysout

    sections = []
    ordinal = 0
    for section_type, content in (
        ('jes', '\n'.join(jes_lines).strip()),
        ('jcl', '\n'.join(jcl_lines).strip()),
        ('sysout', '\n'.join(sysout_lines).strip()),
        ('raw', raw.strip()),
    ):
        if content:
            sections.append({'section_type': section_type, 'ordinal': ordinal, 'content_text': content})
            ordinal += 1
    return sections
