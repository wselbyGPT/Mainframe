from worker.runner import AutomationFailure, _build_failure_payload
from worker.screen_recognizers import has_login_error, looks_like_tso_screen, wants_userid


def test_recognizer_catches_common_logon_error() -> None:
    screen = "IKJ56701I INVALID USERID OR PASSWORD"
    assert has_login_error(screen)


def test_recognizer_detects_tso_context() -> None:
    screen = "IKJ56455I USERID IBMUSER READY"
    assert looks_like_tso_screen(screen)
    assert wants_userid("ENTER USERID")


def test_failure_payload_includes_taxonomy_and_remediation() -> None:
    err = AutomationFailure(
        stage="logging_in",
        detail="generic",
        screen="IKJ56701I INVALID USERID OR PASSWORD",
        code="automation_error",
    )
    payload = _build_failure_payload(err)
    assert payload["category"] == "auth"
    assert payload["code"] == "credentials_rejected"
    assert payload["retryable"] is False
    assert payload["remediation"]
