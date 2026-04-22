from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Pattern


@dataclass(frozen=True)
class WorkerProfile:
    name: str
    recognizer_overrides: dict[str, Pattern[str]] = field(default_factory=dict)
    remediation_hints: dict[str, tuple[str, ...]] = field(default_factory=dict)
    ready_attempts: int = 20
    ready_delay_seconds: float = 0.5
    logon_attempts: int = 8

    def should_acknowledge_applid(self, screen: str, wants_applid: bool) -> bool:
        return wants_applid

    def should_force_input_command(self, in_input_mode: bool) -> bool:
        return not in_input_mode

    def spool_continue_command(self, is_ready: bool) -> str | None:
        if is_ready:
            return None
        return 'CONTINUE'


def compile_patterns(raw_patterns: dict[str, str]) -> dict[str, Pattern[str]]:
    return {name: re.compile(pattern, re.IGNORECASE) for name, pattern in raw_patterns.items()}
