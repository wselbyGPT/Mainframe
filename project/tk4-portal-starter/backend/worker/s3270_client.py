from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass

from common.config import settings


class S3270Error(RuntimeError):
    pass


@dataclass
class S3270Result:
    ok: bool
    lines: list[str]
    status: str | None

    @property
    def text(self) -> str:
        return '\n'.join(self.lines)


class S3270Client:
    def __init__(self, binary: str | None = None, model: str | None = None):
        self.binary = binary or settings.s3270_bin
        self.model = model or settings.s3270_model
        self.proc: subprocess.Popen[str] | None = None

    def __enter__(self) -> 'S3270Client':
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def start(self) -> None:
        if self.proc is not None:
            return
        self.proc = subprocess.Popen(
            [self.binary, '-utf8', '-model', str(self.model)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def close(self) -> None:
        if self.proc is None:
            return
        try:
            try:
                self.action('Disconnect', allow_error=True)
            except Exception:
                pass
            if self.proc.stdin:
                self.proc.stdin.close()
            self.proc.terminate()
            self.proc.wait(timeout=2)
        except Exception:
            self.proc.kill()
        finally:
            self.proc = None

    def action(self, command: str, timeout: float | None = None, allow_error: bool = False) -> S3270Result:
        if self.proc is None or self.proc.stdin is None or self.proc.stdout is None:
            raise S3270Error('s3270 is not running')
        timeout = settings.tso_timeout_seconds if timeout is None else timeout
        self.proc.stdin.write(command + '\n')
        self.proc.stdin.flush()

        lines: list[str] = []
        deadline = time.monotonic() + timeout
        while True:
            if time.monotonic() > deadline:
                raise S3270Error(f'Timeout waiting for s3270 response to {command!r}. Partial output:\n' + '\n'.join(lines))
            line = self.proc.stdout.readline()
            if line == '':
                raise S3270Error(f's3270 exited while executing {command!r}. Partial output:\n' + '\n'.join(lines))
            line = line.rstrip('\n')
            if line in {'ok', 'error'}:
                status = None
                payload = lines[:]
                if payload and self._looks_like_status_line(payload[-1]):
                    status = payload.pop()
                result = S3270Result(ok=(line == 'ok'), lines=payload, status=status)
                if not result.ok and not allow_error:
                    raise S3270Error(f's3270 action failed: {command!r}\n' + result.text)
                return result
            lines.append(line)

    @staticmethod
    def _looks_like_status_line(line: str) -> bool:
        parts = line.split()
        return len(parts) >= 6 and any(p in {'U', 'L', 'N', 'E'} or p.startswith('C(') for p in parts)

    def connect(self, host: str, port: int) -> None:
        self.action(f'Connect({host}:{port})', timeout=settings.tso_timeout_seconds)

    def wait_output(self, timeout: float | None = None) -> None:
        self.action('Wait(Output)', timeout=timeout)

    def enter(self) -> None:
        self.action('Enter')

    def tab(self) -> None:
        self.action('Tab')

    def clear(self) -> None:
        self.action('Clear')

    def erase_input(self) -> None:
        self.action('EraseInput', allow_error=True)

    def string(self, value: str) -> None:
        escaped = value.replace('\\', r'\\').replace('"', r'\"')
        self.action(f'String("{escaped}")')

    def ascii(self) -> str:
        result = self.action('Ascii()')
        return result.text
