from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class AuthPrincipal:
    subject: str
    role: str
    provider: str = 'local'


class IdentityProviderAdapter(Protocol):
    def authenticate(self, username: str, password: str) -> AuthPrincipal | None:
        """Authenticate credentials and return a principal when valid."""


class LocalIdentityProvider:
    def __init__(self, users: dict[str, dict[str, str]]):
        self._users = users

    def authenticate(self, username: str, password: str) -> AuthPrincipal | None:
        user = self._users.get(username)
        if not user or user.get('password') != password:
            return None
        return AuthPrincipal(subject=username, role=str(user['role']), provider='local')
