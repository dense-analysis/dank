"""The refresh queue: schedules collections and keeps their history.

One manager lives as long as the server, opened by running_refresh. Jobs run
through a CollectionWorker, so nothing here knows how a collection is run.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import pathlib
import uuid
from collections.abc import AsyncGenerator, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from functools import partial
from typing import Any, Protocol

from dank.api.refresh.model import (
    ACTIVE_STATUSES,
    MAX_CONCURRENT_JOBS,
    MAX_PENDING_JOBS,
    RefreshJob,
    RefreshPlan,
    plans_overlap,
    restored_jobs,
    retained_jobs,
)
from dank.config import Settings
from dank.errors import TooManyRequests

logger = logging.getLogger(__name__)
BUSY_RETRY_SECONDS = 2
# An abandoned external request must not keep the queue blocked indefinitely.
MAX_REFRESH_SECONDS = 2 * 60 * 60

type Report = Callable[[Mapping[str, Any]], None]


class CollectionWorker(Protocol):
    async def collect(
        self, job_id: str, plan: RefreshPlan, report: Report,
    ) -> None:
        """Run one collection, reporting its job's changes as they happen.

        Cancelling it must stop the collection before it returns.
        """
        ...


def timestamp() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class RefreshManager:
    def __init__(
        self,
        path: pathlib.Path,
        worker: CollectionWorker,
        *,
        busy_retry_seconds: float = BUSY_RETRY_SECONDS,
    ) -> None:
        self.path = path
        self.scheduler: asyncio.Task[None] | None = None
        self.pending: asyncio.Queue[tuple[str, RefreshPlan]] = (
            asyncio.Queue(maxsize=MAX_PENDING_JOBS)
        )
        self.waiting: list[tuple[str, RefreshPlan]] = []
        self.running: dict[str, tuple[RefreshPlan, asyncio.Task[None]]] = {}
        self.retry_after: dict[str, float] = {}
        self.changed = asyncio.Event()
        self._jobs: dict[str, RefreshJob] = {}
        self._worker = worker
        self._busy_retry_seconds = busy_retry_seconds

    def jobs(self) -> tuple[RefreshJob, ...]:
        """Every job history keeps, oldest first."""
        return tuple(self._jobs.values())

    def restore(self) -> None:
        if not self.path.exists():
            return

        try:
            saved = json.loads(self.path.read_text())
            jobs = restored_jobs(saved, timestamp())
        except (OSError, ValueError):
            logger.exception("Could not restore reader refresh history")

            return

        self._jobs = {job.id: job for job in jobs}
        self.persist()

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps([
            job._asdict() for job in self._jobs.values()
        ]))
        temporary.replace(self.path)

    def submit(self, plan: RefreshPlan) -> RefreshJob:
        active = [job for job in self._jobs.values()
                  if job.status in ACTIVE_STATUSES]

        for job in active:
            if job.scope == plan.scope:
                return job

        if len(active) >= MAX_PENDING_JOBS:
            raise TooManyRequests(
                "The refresh queue is full. Try again when a job finishes.",
            )

        job = RefreshJob(
            id=str(uuid.uuid4()),
            scope=plan.scope,
            label=plan.label,
            status="queued",
            message="Waiting for collection…",
            created_at=timestamp(),
            finished_at=None,
            posts_saved=0,
            run_id=None,
        )
        self._jobs = {
            item.id: item for item in retained_jobs((*self.jobs(), job))
        }
        self.persist()
        self.pending.put_nowait((job.id, plan))
        self.changed.set()

        return job

    def update(self, job_id: str, update: Mapping[str, Any]) -> None:
        # History can have let go of a finished job a worker still reports on.
        if (job := self._jobs.get(job_id)) is None:
            return

        job = job._replace(**update)

        if job.status not in ACTIVE_STATUSES:
            job = job._replace(finished_at=timestamp())

        self._jobs[job_id] = job
        self.persist()

    async def run(self) -> None:
        try:
            while True:
                self.changed.clear()

                while not self.pending.empty():
                    self.waiting.append(self.pending.get_nowait())

                self.start_ready()
                delay = min((
                    max(0.1, value - asyncio.get_running_loop().time())
                    for value in self.retry_after.values()
                ), default=None)

                try:
                    await asyncio.wait_for(self.changed.wait(), delay)
                except TimeoutError:
                    pass
        finally:
            tasks = [task for _, task in self.running.values()]

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

    def start_ready(self) -> None:
        waiting: list[tuple[str, RefreshPlan]] = []

        for job_id, plan in self.waiting:
            # Earlier overlapping requests reserve their turn, so All cannot
            # be starved by a stream of later narrow collections.
            blockers = [
                *(selected for _, selected in waiting),
                *(running for running, _ in self.running.values()),
            ]
            conflict = next(
                (other for other in blockers if plans_overlap(plan, other)),
                None,
            )
            retry = self.retry_after.get(job_id, 0)

            if retry > asyncio.get_running_loop().time():
                waiting.append((job_id, plan))
                continue

            self.retry_after.pop(job_id, None)

            if conflict:
                self.wait_message(job_id, f"Waiting for {conflict.label} "
                                  "to finish; these refreshes share a source.")
            elif len(self.running) >= MAX_CONCURRENT_JOBS:
                self.wait_message(job_id, "Waiting for a refresh slot…")
            else:
                task = asyncio.create_task(self.run_job(job_id, plan))
                self.running[job_id] = (plan, task)
                continue

            waiting.append((job_id, plan))

        self.waiting = waiting

    def wait_message(self, job_id: str, message: str) -> None:
        job = self._jobs.get(job_id)

        if job is not None and job.message != message:
            self.update(job_id, {"status": "queued", "message": message})

    async def run_job(self, job_id: str, plan: RefreshPlan) -> None:
        retry = False

        try:
            self.update(job_id, {
                "status": "running",
                "message": "Collecting in the background…",
            })

            async with asyncio.timeout(MAX_REFRESH_SECONDS):
                await self._worker.collect(
                    job_id, plan, partial(self.update, job_id),
                )

            job = self._jobs.get(job_id)

            if job is not None and job.status in {"running", "processing"}:
                raise RuntimeError("Collection worker exited without a result")

            retry = job is not None and job.status == "queued"
        except asyncio.CancelledError:
            self.update(job_id, {
                "status": "interrupted", "message": "Refresh was interrupted.",
            })
            raise
        except Exception:
            logger.exception("Reader refresh failed: %s", job_id)
            self.update(job_id, {
                "status": "failed", "message": "Refresh could not finish. "
                "Saved content is still available.",
            })
        finally:
            self.running.pop(job_id, None)

            if retry:
                self.waiting.insert(0, (job_id, plan))
                self.retry_after[job_id] = (
                    asyncio.get_running_loop().time()
                    + self._busy_retry_seconds
                )
            else:
                self.pending.task_done()

            self.changed.set()

    async def close(self) -> None:
        if self.scheduler is not None:
            self.scheduler.cancel()

            with suppress(asyncio.CancelledError):
                await self.scheduler

        for _ in self.waiting:
            self.pending.task_done()

        self.waiting.clear()
        self.retry_after.clear()

        while not self.pending.empty():
            self.pending.get_nowait()
            self.pending.task_done()

        for job in self.jobs():
            if job.status in ACTIVE_STATUSES:
                self.update(job.id, {
                    "status": "interrupted",
                    "message": "Refresh was interrupted.",
                })


@asynccontextmanager
async def running_refresh(
    settings: Settings,
    worker: CollectionWorker,
    *,
    busy_retry_seconds: float = BUSY_RETRY_SECONDS,
) -> AsyncGenerator[RefreshManager]:
    """Restore saved jobs and run the scheduler; on exit, stop the workers."""
    manager = RefreshManager(
        pathlib.Path(settings.data_dir) / "reader-refresh.json",
        worker,
        busy_retry_seconds=busy_retry_seconds,
    )
    manager.restore()
    manager.scheduler = asyncio.create_task(manager.run())

    try:
        yield manager
    finally:
        await manager.close()
