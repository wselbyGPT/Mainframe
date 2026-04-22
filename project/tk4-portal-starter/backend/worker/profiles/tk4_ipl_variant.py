from __future__ import annotations

from worker.profiles.base import WorkerProfile, compile_patterns


PROFILE = WorkerProfile(
    name='tk4_ipl_variant',
    recognizer_overrides=compile_patterns(
        {
            'ready': r'(^|\n)\s*(READY|READY>|TSO READY)\b',
            'userid': r'ENTER\s+(USERID|USER\s*ID)|LOGON\s+ID',
            'password': r'(CURRENT\s+)?PASSWORD|PASSCODE',
            'applid': r'APPLID|APPLICATION\s+REQUESTED\s+APPLID',
            'input': r'INPUT\b|LINE\s+INPUT|===>\s*INPUT',
            'done': r'OUTPUT\s+QUEUE|ON\s+OUTPUT\s+QUEUE|HELD|JOB\s+COMPLETE|COND\s+CODE|RC=',
        }
    ),
    remediation_hints={
        'unrecognized_logon_screen': (
            'This IPL variant may require APPLID acknowledgement and split USERID/PASSWORD prompts.',
            'Switch WORKER_ADAPTER_PROFILE to tk4_ipl_variant if READY cannot be reached.',
        )
    },
)
