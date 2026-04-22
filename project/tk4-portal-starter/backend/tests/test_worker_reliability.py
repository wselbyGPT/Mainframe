from unittest.mock import patch

from common.config import settings
from worker.profiles import get_profile
from worker.runner import AutomationFailure, _build_failure_payload, _logon
from worker.screen_recognizers import has_login_error, is_ready, looks_like_tso_screen, wants_userid


def test_recognizer_catches_common_logon_error() -> None:
    screen = "IKJ56701I INVALID USERID OR PASSWORD"
    assert has_login_error(screen)


def test_recognizer_detects_tso_context() -> None:
    screen = "IKJ56455I USERID IBMUSER READY"
    assert looks_like_tso_screen(screen)
    assert wants_userid("ENTER USERID")


def test_profile_override_precedence_for_ready_detection() -> None:
    profile = get_profile("tk4_ipl_variant")
    assert is_ready("TSO READY", profile)
    assert not is_ready("TSO READY")


def test_negative_prompt_detection_for_misleading_screen() -> None:
    screen = "PLEASE READ USERID RULES BEFORE YOU CONTINUE"
    assert not wants_userid(screen)


def test_failure_payload_includes_taxonomy_and_remediation() -> None:
    err = AutomationFailure(
        stage="logging_in",
        detail="generic",
        screen="IKJ56701I INVALID USERID OR PASSWORD",
        code="automation_error",
    )
    payload = _build_failure_payload(err, get_profile("tk4_default"))
    assert payload["category"] == "auth"
    assert payload["code"] == "credentials_rejected"
    assert payload["retryable"] is False
    assert payload["remediation"]
    assert payload["profile"] == "tk4_default"


class _FakeS3270Client:
    def __init__(self, screens: list[str]) -> None:
        self._screens = screens
        self._cursor = 0
        self.actions: list[tuple[str, str | None]] = []

    def connect(self, host: str, port: int) -> None:
        self.actions.append(("connect", f"{host}:{port}"))

    def wait_output(self, timeout: float | None = None) -> None:
        self.actions.append(("wait_output", str(timeout)))

    def ascii(self) -> str:
        current = self._screens[min(self._cursor, len(self._screens) - 1)]
        self._cursor += 1
        return current

    def enter(self) -> None:
        self.actions.append(("enter", None))

    def erase_input(self) -> None:
        self.actions.append(("erase_input", None))

    def tab(self) -> None:
        self.actions.append(("tab", None))

    def string(self, value: str) -> None:
        self.actions.append(("string", value))


def test_ipl_smoke_logon_flow_reaches_ready_prompt() -> None:
    object.__setattr__(settings, "tso_user", "IBMUSER")
    object.__setattr__(settings, "tso_pass", "SYS1")
    fake = _FakeS3270Client(
        [
            "TK4- MVS 3.8j\nAPPLICATION REQUESTED APPLID",
            "ENTER USERID",
            "ENTER CURRENT PASSWORD",
            "IKJ56455I USERID IBMUSER\nREADY",
        ]
    )

    with patch("worker.runner.time.sleep", return_value=None):
        screen = _logon(fake, get_profile("tk4_ipl_variant"))  # type: ignore[arg-type]

    assert "READY" in screen
    assert ("string", "IBMUSER") in fake.actions
    assert ("string", "SYS1") in fake.actions


def test_ipl_smoke_logon_reports_credential_rejection() -> None:
    object.__setattr__(settings, "tso_user", "BADUSER")
    object.__setattr__(settings, "tso_pass", "BADPASS")
    fake = _FakeS3270Client(
        [
            "TK4- MVS 3.8j\nAPPLICATION REQUESTED APPLID",
            "ENTER USERID",
            "IKJ56701I INVALID USERID OR PASSWORD",
        ]
    )

    with patch("worker.runner.time.sleep", return_value=None):
        try:
            _logon(fake, get_profile("tk4_ipl_variant"))  # type: ignore[arg-type]
            assert False, "expected AutomationFailure"
        except AutomationFailure as exc:
            assert exc.code == "credentials_rejected"
            assert exc.stage == "logging_in"
