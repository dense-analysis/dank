"""CollectionWorker that gives each collection its own spawned process."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess

from dank.api.refresh.manager import Report
from dank.api.refresh.model import RefreshPlan, full_refresh
from dank.web.refresh_worker import collect_refresh

WORKER_POLL_SECONDS = 0.25
# Four browser sessions can each need their bounded 20-second cleanup.
WORKER_STOP_SECONDS = 90


class ProcessWorker:
    async def collect(
        self, job_id: str, plan: RefreshPlan, report: Report,
    ) -> None:
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
        process = context.Process(
            target=collect_refresh,
            args=(plan.settings, plan.target_post, send),
            kwargs={"exclusive": full_refresh(plan)},
            name=f"dank-refresh-{job_id}",
        )

        try:
            process.start()
            send.close()

            while process.is_alive():
                read_updates(receive, report)
                await asyncio.sleep(WORKER_POLL_SECONDS)

            read_updates(receive, report)
        finally:
            await stop_worker(process)
            receive.close()
            send.close()


def read_updates(receive: Connection, report: Report) -> None:
    while receive.poll():
        try:
            update = receive.recv()
        except EOFError:
            break

        if update:
            report(update)


async def stop_worker(
    process: BaseProcess, *, stop_seconds: float = WORKER_STOP_SECONDS,
) -> None:
    if process.pid is None:
        return

    if process.is_alive():
        process.terminate()
        await asyncio.to_thread(process.join, stop_seconds)

        if process.is_alive():
            # Never signal the web server's group if spawn has not set it yet.
            try:
                if os.getpgid(process.pid) == process.pid:
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass

            await asyncio.to_thread(process.join, 5)

    process.join(timeout=0)
    process.close()
