from __future__ import annotations

import re


READY_RE = re.compile(r'(^|\n)READY\b', re.IGNORECASE)
PASSWORD_RE = re.compile(r'PASSWORD', re.IGNORECASE)
USERID_RE = re.compile(r'USER\s*ID|USERID|ENTER\s+USERID|LOGON', re.IGNORECASE)
APPLID_RE = re.compile(r'APPLID', re.IGNORECASE)
INPUT_RE = re.compile(r'INPUT\b', re.IGNORECASE)
JOBID_RE = re.compile(r'\b(JOB\d{3,7})\b', re.IGNORECASE)
RC_RE = re.compile(r'COND\s+CODE\s+([0-9]{4})|RC=([0-9]{4})', re.IGNORECASE)
ABEND_RE = re.compile(r'\b(S[0-9A-F]{3}|U[0-9A-F]{4})\b', re.IGNORECASE)
DONE_RE = re.compile(r'OUTPUT\s+QUEUE|ON\s+OUTPUT\s+QUEUE|HELD|COND\s+CODE|RC=', re.IGNORECASE)
JCL_ERROR_RE = re.compile(r'IEFC\d+I|JCL\s+ERROR', re.IGNORECASE)


def normalize(text: str) -> str:
    return text.replace('\x00', '').replace('\r', '')


def is_ready(text: str) -> bool:
    return bool(READY_RE.search(normalize(text)))


def wants_password(text: str) -> bool:
    return bool(PASSWORD_RE.search(normalize(text)))


def wants_userid(text: str) -> bool:
    return bool(USERID_RE.search(normalize(text)))


def wants_applid(text: str) -> bool:
    return bool(APPLID_RE.search(normalize(text)))


def in_input_mode(text: str) -> bool:
    return bool(INPUT_RE.search(normalize(text)))


def extract_job_id(text: str) -> str | None:
    m = JOBID_RE.search(normalize(text))
    return m.group(1).upper() if m else None


def extract_return_code(text: str) -> str | None:
    m = RC_RE.search(normalize(text))
    if not m:
        return None
    return next(group for group in m.groups() if group)


def extract_abend(text: str) -> str | None:
    m = ABEND_RE.search(normalize(text))
    return m.group(1).upper() if m else None


def looks_done(text: str) -> bool:
    return bool(DONE_RE.search(normalize(text)))


def looks_jcl_error(text: str) -> bool:
    return bool(JCL_ERROR_RE.search(normalize(text)))
