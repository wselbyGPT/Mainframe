from __future__ import annotations

from worker.profiles.base import WorkerProfile
from worker.profiles.tk4_default import PROFILE as TK4_DEFAULT_PROFILE
from worker.profiles.tk4_ipl_variant import PROFILE as TK4_IPL_VARIANT_PROFILE

_PROFILES: dict[str, WorkerProfile] = {
    TK4_DEFAULT_PROFILE.name: TK4_DEFAULT_PROFILE,
    TK4_IPL_VARIANT_PROFILE.name: TK4_IPL_VARIANT_PROFILE,
}


def get_profile(name: str | None) -> WorkerProfile:
    if not name:
        return TK4_DEFAULT_PROFILE
    return _PROFILES.get(name.strip().lower(), TK4_DEFAULT_PROFILE)


def supported_profiles() -> tuple[str, ...]:
    return tuple(sorted(_PROFILES))
