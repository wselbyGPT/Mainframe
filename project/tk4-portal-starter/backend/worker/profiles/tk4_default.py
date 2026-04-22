from __future__ import annotations

from worker.profiles.base import WorkerProfile, compile_patterns


PROFILE = WorkerProfile(
    name='tk4_default',
    recognizer_overrides=compile_patterns(
        {
            'ready': r'(^|\n)\s*(READY|READY>)\b',
            'password': r'PASSWORD',
            'userid': r'USER\s*ID|USERID|ENTER\s+USERID|LOGON',
            'applid': r'APPLID',
            'input': r'INPUT\b|ENTER\s+INPUT|===>\s*INPUT',
            'jobid': r'\b(JOB\d{3,7})\b',
            'rc': r'COND\s+CODE\s+([0-9]{4})|RC=([0-9]{4})',
            'abend': r'\b(S[0-9A-F]{3}|U[0-9A-F]{4})\b',
            'done': r'OUTPUT\s+QUEUE|ON\s+OUTPUT\s+QUEUE|HELD|COND\s+CODE|RC=',
            'jcl_error': r'IEFC\d+I|JCL\s+ERROR',
            'tso_context': r'\b(TSO|ISPF|READY|IKJ\d{5}[A-Z])\b',
            'login_error': r'INVALID\s+(USERID|PASSWORD)|IKJ56425I|IKJ56701I|NOT\s+AUTHORIZED|LOGON\s+REJECTED',
            'dataset_error': r'DATA\s*SET\s+NOT\s+FOUND|NOT\s+CATALOGED|IKJ56228I',
        }
    ),
)
