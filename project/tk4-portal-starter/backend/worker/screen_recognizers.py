from __future__ import annotations

import re
from typing import Pattern

from worker.profiles.base import WorkerProfile


BASELINE_PATTERNS: dict[str, Pattern[str]] = {
    'ready': re.compile(r'(^|\n)\s*(READY|READY>)\b', re.IGNORECASE),
    'password': re.compile(r'PASSWORD', re.IGNORECASE),
    'userid': re.compile(r'USER\s*ID|USERID|ENTER\s+USERID|LOGON', re.IGNORECASE),
    'applid': re.compile(r'APPLID', re.IGNORECASE),
    'input': re.compile(r'INPUT\b|ENTER\s+INPUT|===>\s*INPUT', re.IGNORECASE),
    'jobid': re.compile(r'\b(JOB\d{3,7})\b', re.IGNORECASE),
    'rc': re.compile(r'COND\s+CODE\s+([0-9]{4})|RC=([0-9]{4})', re.IGNORECASE),
    'abend': re.compile(r'\b(S[0-9A-F]{3}|U[0-9A-F]{4})\b', re.IGNORECASE),
    'done': re.compile(r'OUTPUT\s+QUEUE|ON\s+OUTPUT\s+QUEUE|HELD|COND\s+CODE|RC=', re.IGNORECASE),
    'jcl_error': re.compile(r'IEFC\d+I|JCL\s+ERROR', re.IGNORECASE),
    'tso_context': re.compile(r'\b(TSO|ISPF|READY|IKJ\d{5}[A-Z])\b', re.IGNORECASE),
    'login_error': re.compile(
        r'INVALID\s+(USERID|PASSWORD)|IKJ56425I|IKJ56701I|NOT\s+AUTHORIZED|LOGON\s+REJECTED',
        re.IGNORECASE,
    ),
    'dataset_error': re.compile(r'DATA\s*SET\s+NOT\s+FOUND|NOT\s+CATALOGED|IKJ56228I', re.IGNORECASE),
}


def normalize(text: str) -> str:
    return text.replace('\x00', '').replace('\r', '')


def _pattern(name: str, profile: WorkerProfile | None = None) -> Pattern[str]:
    # Deterministic precedence: profile override first, then baseline recognizer.
    if profile and name in profile.recognizer_overrides:
        return profile.recognizer_overrides[name]
    return BASELINE_PATTERNS[name]


def is_ready(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('ready', profile).search(normalize(text)))


def wants_password(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('password', profile).search(normalize(text)))


def wants_userid(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('userid', profile).search(normalize(text)))


def wants_applid(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('applid', profile).search(normalize(text)))


def in_input_mode(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('input', profile).search(normalize(text)))


def extract_job_id(text: str, profile: WorkerProfile | None = None) -> str | None:
    m = _pattern('jobid', profile).search(normalize(text))
    return m.group(1).upper() if m else None


def extract_return_code(text: str, profile: WorkerProfile | None = None) -> str | None:
    m = _pattern('rc', profile).search(normalize(text))
    if not m:
        return None
    return next(group for group in m.groups() if group)


def extract_abend(text: str, profile: WorkerProfile | None = None) -> str | None:
    m = _pattern('abend', profile).search(normalize(text))
    return m.group(1).upper() if m else None


def looks_done(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('done', profile).search(normalize(text)))


def looks_jcl_error(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('jcl_error', profile).search(normalize(text)))


def looks_like_tso_screen(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('tso_context', profile).search(normalize(text)))


def has_login_error(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('login_error', profile).search(normalize(text)))


def has_dataset_error(text: str, profile: WorkerProfile | None = None) -> bool:
    return bool(_pattern('dataset_error', profile).search(normalize(text)))
