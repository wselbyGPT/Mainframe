from __future__ import annotations

import re


IEF_RE = re.compile(r'\b(IEF|IEB|IEC|ICH|IKJ|HASP|JES)\w*', re.IGNORECASE)
RC_RE = re.compile(r'\bRC\s*=\s*([0-9]{4})\b', re.IGNORECASE)
ABEND_RE = re.compile(r'\bABEND(?:=|\s+S?)([SU]?[0-9A-F]{3,4})\b', re.IGNORECASE)


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


def summarize_sections(sections: list[dict[str, object]]) -> dict[str, object]:
    by_type: dict[str, dict[str, int]] = {}
    rc_nonzero = 0
    abend = 0
    for section in sections:
        section_type = str(section.get('section_type') or 'unknown').strip().lower() or 'unknown'
        content = str(section.get('content_text') or '')
        line_count = len(content.splitlines()) if content else 0
        stats = by_type.setdefault(section_type, {'sections': 0, 'lines': 0})
        stats['sections'] += 1
        stats['lines'] += line_count

        if any(match != '0000' for match in RC_RE.findall(content)):
            rc_nonzero += 1
        if ABEND_RE.search(content):
            abend += 1

    return {
        'sections_total': sum(item['sections'] for item in by_type.values()),
        'lines_total': sum(item['lines'] for item in by_type.values()),
        'section_types': by_type,
        'nonzero_rc_sections': rc_nonzero,
        'abend_sections': abend,
    }
