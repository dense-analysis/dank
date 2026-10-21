"""Bounded, serialized reader refresh jobs that survive reader navigation."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import multiprocessing
import os
import pathlib
import signal
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Any, cast

from aiohttp import web

from dank.config import Settings
from dank.web.reader_refresh_scope import RefreshPlan, resolve_scope
from dank.web.refresh_worker import collect_refresh

logger = logging.getLogger(__name__)
ACTIVE_STATUSES = frozenset({"queued", "running", "processing"})
MAX_PENDING_JOBS = 5
MAX_HISTORY_JOBS = 20
WORKER_POLL_SECONDS = 0.25
# Four browser sessions can each need their bounded 20-second cleanup.
WORKER_STOP_SECONDS = 90
# An abandoned external request must not keep the queue blocked indefinitely.
MAX_REFRESH_SECONDS = 2 * 60 * 60


def timestamp() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class RefreshManager:
    def __init__(self, settings: Settings) -> None:
        self.path = pathlib.Path(settings.data_dir) / "reader-refresh.json"
        self.jobs: list[dict[str, Any]] = []
        self.pending: asyncio.Queue[tuple[dict[str, Any], RefreshPlan]] = (
            asyncio.Queue(maxsize=MAX_PENDING_JOBS)
        )
        self.worker: asyncio.Task[None] | None = None

    def restore(self) -> None:
        if not self.path.exists():
            return

        try:
            saved = json.loads(self.path.read_text())

            if not isinstance(saved, list):
                raise ValueError("Invalid refresh history")

            self.jobs = []

            for item in cast(list[object], saved)[-MAX_HISTORY_JOBS:]:
                if not isinstance(item, dict):
                    continue

                job = cast(dict[str, Any], item)

                if all(isinstance(job.get(key), str)
                       for key in ("id", "label", "status")) and isinstance(
                    job.get("scope"), dict,
                ):
                    self.jobs.append(job)
        except (OSError, ValueError):
            logger.exception("Could not restore reader refresh history")

            return

        for job in self.jobs:
            if job.get("status") in ACTIVE_STATUSES:
                job.update(
                    status="interrupted", finished_at=timestamp(),
                    message="Refresh was interrupted when DANK restarted. "
                    "You can start it again.",
                )

        self.persist()

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.jobs))
        temporary.replace(self.path)

    def submit(self, plan: RefreshPlan) -> dict[str, Any]:
        for job in self.jobs:
            if job["status"] in ACTIVE_STATUSES and job["scope"] == plan.scope:
                return job

        if sum(job["status"] in ACTIVE_STATUSES for job in self.jobs) >= (
            MAX_PENDING_JOBS
        ):
            raise web.HTTPTooManyRequests(
                reason="The refresh queue is full. "
                "Try again when a job finishes.",
            )

        job: dict[str, Any] = {
            "id": str(uuid.uuid4()), "scope": plan.scope, "label": plan.label,
            "status": "queued", "message": "Waiting for collection…",
            "created_at": timestamp(), "finished_at": None,
            "posts_saved": 0, "run_id": None,
        }
        self.jobs.append(job)
        self.jobs = self.jobs[-MAX_HISTORY_JOBS:]
        self.persist()
        self.pending.put_nowait((job, plan))

        return job

    def update(self, job: dict[str, Any], update: dict[str, Any]) -> None:
        job.update(update)

        if job["status"] not in ACTIVE_STATUSES:
            job["finished_at"] = timestamp()

        self.persist()

    async def run(self) -> None:
        while True:
            job, plan = await self.pending.get()

            try:
                self.update(job, {
                    "status": "running",
                    "message": "Collecting in the background…",
                })

                async with asyncio.timeout(MAX_REFRESH_SECONDS):
                    await self.collect(job, plan)
            except asyncio.CancelledError:
                self.update(job, {
                    "status": "interrupted",
                    "message": "Refresh was interrupted.",
                })
                raise
            except Exception:
                logger.exception("Reader refresh failed: %s", job["id"])
                self.update(job, {
                    "status": "failed",
                    "message": "Refresh could not finish. "
                    "Saved content is still available.",
                })
            finally:
                self.pending.task_done()

    async def collect(self, job: dict[str, Any], plan: RefreshPlan) -> None:
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
        process = context.Process(
            target=collect_refresh,
            args=(plan.settings, plan.target_post, send),
            name=f"dank-refresh-{job['id']}",
        )

        try:
            process.start()
            send.close()

            while process.is_alive():
                self.read_updates(job, receive)
                await asyncio.sleep(WORKER_POLL_SECONDS)

            self.read_updates(job, receive)

            if job["status"] in ACTIVE_STATUSES:
                raise RuntimeError("Collection worker exited without a result")
        finally:
            await stop_worker(process)
            receive.close()
            send.close()

    def read_updates(self, job: dict[str, Any], receive: Connection) -> None:
        while receive.poll():
            try:
                update = receive.recv()
            except EOFError:
                break

            if update:
                self.update(job, update)

    async def close(self) -> None:
        if self.worker is not None:
            self.worker.cancel()

            with suppress(asyncio.CancelledError):
                await self.worker

        for job in self.jobs:
            if job["status"] in ACTIVE_STATUSES:
                self.update(job, {
                    "status": "interrupted",
                    "message": "Refresh was interrupted.",
                })


async def stop_worker(process: BaseProcess) -> None:
    if process.pid is None:
        return

    if process.is_alive():
        process.terminate()
        await asyncio.to_thread(process.join, WORKER_STOP_SECONDS)

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


REFRESH_MANAGER = web.AppKey("reader_refresh_manager", RefreshManager)


def add_refresh_routes(app: web.Application) -> None:
    app.cleanup_ctx.append(refresh_context)
    app.router.add_get("/api/reader/refresh", handle_refresh)
    app.router.add_post("/api/reader/refresh", handle_refresh)


async def refresh_context(app: web.Application) -> AsyncIterator[None]:
    manager = RefreshManager(app["state"].settings)
    manager.restore()
    app[REFRESH_MANAGER] = manager
    manager.worker = asyncio.create_task(manager.run())

    try:
        yield
    finally:
        await manager.close()


async def handle_refresh(request: web.Request) -> web.Response:
    manager = request.app[REFRESH_MANAGER]

    if request.method == "GET":
        return web.json_response({"jobs": list(reversed(manager.jobs))})

    origin = request.headers.get("Origin")
    # The local HTTPS proxy preserves Host and supplies the external scheme.
    scheme = request.headers.get("X-Forwarded-Proto", request.scheme)

    if request.headers.get("Sec-Fetch-Site") == "cross-site" or (
        origin is not None and origin != f"{scheme}://{request.host}"
    ):
        raise web.HTTPForbidden(reason="Same-origin request required")

    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType(reason="JSON request required")

    try:
        raw_body = await request.json()

        if not isinstance(raw_body, dict):
            raise ValueError("A refresh scope is required")

        body = cast(dict[str, Any], raw_body)

        if set(body) != {"scope"}:
            raise ValueError("A refresh scope is required")

        plan = await resolve_scope(
            request.app["state"].settings, request.app["clickhouse"],
            body["scope"],
        )
    except (ValueError, TypeError) as error:
        raise web.HTTPBadRequest(reason=str(error)) from error

    return web.json_response({"job": manager.submit(plan)}, status=202)
