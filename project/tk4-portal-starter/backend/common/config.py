from __future__ import annotations

from dataclasses import dataclass
import os


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = _get_int("API_PORT", 8000)
    database_path: str = os.getenv("DATABASE_PATH", "/data/jobs.sqlite3")
    dry_run: bool = _get_bool("DRY_RUN", True)
    poll_interval_seconds: int = _get_int("POLL_INTERVAL_SECONDS", 2)
    tk4_host: str = os.getenv("TK4_HOST", "127.0.0.1")
    tk4_port: int = _get_int("TK4_PORT", 3270)
    tso_user: str = os.getenv("TSO_USER", "IBMUSER")
    tso_pass: str = os.getenv("TSO_PASS", "IBMPASS")
    tso_prefix: str = os.getenv("TSO_PREFIX", os.getenv("TSO_USER", "IBMUSER"))
    tso_timeout_seconds: int = _get_int("TSO_TIMEOUT_SECONDS", 15)
    job_poll_seconds: int = _get_int("JOB_POLL_SECONDS", 2)
    job_poll_attempts: int = _get_int("JOB_POLL_ATTEMPTS", 15)
    s3270_bin: str = os.getenv("S3270_BIN", "s3270")
    s3270_model: str = os.getenv("S3270_MODEL", "2")
    worker_version: str = os.getenv("WORKER_VERSION", "dev")
    worker_build: str = os.getenv("WORKER_BUILD", "local")


settings = Settings()
