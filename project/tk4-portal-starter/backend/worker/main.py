from __future__ import annotations

import asyncio
import os
import signal
from contextlib import suppress

from common.config import settings
from common.db import add_event, get_job, init_db
from common.queue_backend import QueueBackend
from worker.runner import run_job


async def _worker_loop(worker_id: str, stop_event: asyncio.Event) -> None:
    queue = QueueBackend()
    while not stop_event.is_set():
        lease = queue.reserve(worker_id, lease_seconds=settings.queue_lease_seconds)
        if not lease:
            await asyncio.sleep(settings.poll_interval_seconds)
            continue

        job = lease.job
        job_id = str(job['id'])
        try:
            queue.mark_running(job_id, worker_id)
            heartbeat_task = asyncio.create_task(_heartbeat_loop(queue, job_id, worker_id, stop_event))
            await asyncio.to_thread(run_job, job)

            fresh = get_job(job_id)
            final_state = (fresh or {}).get('state')
            if final_state in {'completed', 'canceled'}:
                queue.ack(job_id, worker_id)
            else:
                queue.nack_retry(
                    job_id,
                    worker_id,
                    delay_seconds=settings.queue_retry_delay_seconds,
                    max_attempts=settings.queue_max_attempts,
                    reason=f'worker_exit_state:{final_state}',
                )
        except Exception as exc:  # pragma: no cover - defensive guard
            queue.nack_retry(
                job_id,
                worker_id,
                delay_seconds=settings.queue_retry_delay_seconds,
                max_attempts=settings.queue_max_attempts,
                reason=f'worker_exception:{type(exc).__name__}',
            )
            add_event(job_id, 'worker.crash', {'detail': str(exc)})
        finally:
            if 'heartbeat_task' in locals():
                heartbeat_task.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat_task


async def _heartbeat_loop(queue: QueueBackend, job_id: str, worker_id: str, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(settings.queue_heartbeat_seconds)
        ok = queue.heartbeat(job_id, worker_id, lease_seconds=settings.queue_lease_seconds)
        if not ok:
            return


async def _run() -> None:
    init_db()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for signame in ('SIGINT', 'SIGTERM'):
        signum = getattr(signal, signame, None)
        if signum is not None:
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, _request_stop)

    worker_count = max(1, int(settings.worker_concurrency))
    tasks = [asyncio.create_task(_worker_loop(f"{os.getpid()}-w{i+1}", stop_event)) for i in range(worker_count)]

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(_run())


if __name__ == '__main__':
    main()
