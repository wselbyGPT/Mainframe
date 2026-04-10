from __future__ import annotations

import time

from common.config import settings
from common.db import add_event, init_db, next_queued_job
from worker.runner import run_job


def main() -> None:
    init_db()
    while True:
        job = next_queued_job()
        if not job:
            time.sleep(settings.poll_interval_seconds)
            continue
        try:
            run_job(job)
        except Exception as exc:
            add_event(job['id'], 'worker.crash', {'detail': str(exc)})
        time.sleep(0.2)


if __name__ == '__main__':
    main()
