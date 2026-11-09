"""The aiohttp glue for refresh jobs, until DEN-51 removes it.

Real requests over a Unix socket. tests/api/refresh covers the queue and the
rules, which both servers share.
"""

from __future__ import annotations

import asyncio
import pathlib
import tempfile
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from dank.api.refresh.manager import Report, running_refresh
from dank.api.refresh.model import MAX_PENDING_JOBS, RefreshPlan
from dank.api.sources.saved_catalog import SavedSourceCatalog
from dank.api.sources.service import SourceCatalog
from dank.config import PostgresSettings, Settings, SourceConfig, load_settings
from dank.sources.model import Source
from dank.web.reader_api import reader_errors
from dank.web.reader_refresh import REFRESH_MANAGER, handle_refresh
from dank.web.sources import SOURCE_CATALOG
from tests.api.client import BASE_URL
from tests.api.refresh.fakes import FakeWorker
from tests.sources.fakes import FakeSourceRepository

EXAMPLE = pathlib.Path(__file__).parents[2] / "config.native.example.toml"
URL = BASE_URL + "/api/reader/refresh"
JSON = {"Content-Type": "application/json"}
QUEUE_FULL = "The refresh queue is full. Try again when a job finishes."


def source(domain: str) -> dict[str, Any]:
    return {"scope": {"kind": "rss", "domains": [domain]}}


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Settings:
    return load_settings(EXAMPLE)._replace(
        data_dir=tmp_path,
        postgres=PostgresSettings(enabled=False),
        sources=tuple(
            SourceConfig(f"source{index}.example", ())
            for index in range(MAX_PENDING_JOBS + 1)
        ),
    )


@pytest.fixture
def worker() -> FakeWorker:
    return FakeWorker()


@asynccontextmanager
async def serving(
    settings: Settings,
    worker: FakeWorker,
    saved: SourceCatalog | None = None,
) -> AsyncGenerator[aiohttp.ClientSession]:
    app = web.Application(middlewares=[reader_errors])
    app["state"] = SimpleNamespace(settings=settings)

    if saved is not None:
        app[SOURCE_CATALOG] = saved

    # Source scopes read nothing saved, so this is never reached.
    app["clickhouse"] = None
    app.router.add_get("/api/reader/refresh", handle_refresh)
    app.router.add_post("/api/reader/refresh", handle_refresh)

    async with running_refresh(settings, worker) as manager:
        app[REFRESH_MANAGER] = manager
        runner = web.AppRunner(app)
        await runner.setup()

        try:
            # A Unix socket, since tests may not dial TCP.
            with tempfile.TemporaryDirectory() as directory:
                path = str(pathlib.Path(directory) / "web.sock")
                await web.UnixSite(runner, path).start()
                connector = aiohttp.UnixConnector(path=path)

                async with aiohttp.ClientSession(
                    connector=connector,
                ) as session:
                    yield session
        finally:
            await runner.cleanup()


@pytest.fixture
async def session(
    settings: Settings, worker: FakeWorker,
) -> AsyncIterator[aiohttp.ClientSession]:
    async with serving(settings, worker) as session:
        yield session


async def hold(plan: RefreshPlan, report: Report) -> None:
    await asyncio.Event().wait()


async def test_post_queues_a_job_and_get_lists_the_newest_first(
    session: aiohttp.ClientSession, worker: FakeWorker,
) -> None:
    worker.script = hold

    async with session.get(URL) as response:
        assert await response.json() == {"jobs": []}

    async with session.post(URL, json=source("source0.example")) as response:
        assert response.status == 202

        first = (await response.json())["job"]

    assert first["scope"] == source("source0.example")["scope"]
    assert first["label"] == "source0.example"
    assert first["status"] == "queued"
    assert first["finished_at"] is None
    assert first["posts_saved"] == 0
    assert first["run_id"] is None

    async with session.post(URL, json=source("source1.example")) as response:
        second = (await response.json())["job"]

    async with session.get(URL) as response:
        assert response.headers["Cache-Control"] == "no-store"
        assert [job["id"] for job in (await response.json())["jobs"]] == [
            second["id"], first["id"],
        ]


async def test_a_full_queue_answers_429_with_its_message(
    session: aiohttp.ClientSession, worker: FakeWorker,
) -> None:
    worker.script = hold

    for index in range(MAX_PENDING_JOBS):
        async with session.post(
            URL, json=source(f"source{index}.example"),
        ) as response:
            assert response.status == 202

    async with session.post(
        URL, json=source(f"source{MAX_PENDING_JOBS}.example"),
    ) as response:
        assert response.status == 429
        assert await response.json() == {"error": QUEUE_FULL}


@pytest.mark.parametrize(("headers", "body", "status", "error"), [
    ({"Origin": "https://foreign.example"}, b'{"scope": {"kind": "all"}}', 403,
     "Same-origin request required"),
    ({"Sec-Fetch-Site": "cross-site"}, b'{"scope": {"kind": "all"}}', 403,
     "Same-origin request required"),
    ({"Content-Type": "text/plain"}, b'{"scope": {"kind": "all"}}', 415,
     "JSON request required"),
    ({}, b"{}", 400, "A refresh scope is required"),
    ({}, b"[]", 400, "A refresh scope is required"),
    ({}, b'{"scope": {}, "url": "https://foreign.example"}', 400,
     "A refresh scope is required"),
    ({}, b'{"scope": null}', 400, "Refresh scope must be an object"),
    ({}, b'{"scope": {"kind": "unsupported"}}', 400,
     "Unknown refresh scope kind"),
])
async def test_untrusted_or_malformed_requests_queue_nothing(
    session: aiohttp.ClientSession,
    headers: dict[str, str],
    body: bytes,
    status: int,
    error: str,
) -> None:
    async with session.post(
        URL, data=body, headers=JSON | headers,
    ) as response:
        assert response.status == status
        assert await response.json() == {"error": error}

    async with session.get(URL) as response:
        assert await response.json() == {"jobs": []}


async def test_https_proxy_requests_still_require_matching_origin(
    session: aiohttp.ClientSession, worker: FakeWorker,
) -> None:
    worker.script = hold
    # What the proxy sends once it has checked the https:// origin.
    proxied = {"Host": "dank.local", "X-Forwarded-Proto": "https"}

    async with session.post(
        URL,
        json=source("source0.example"),
        headers={**proxied, "Origin": "http://dank.local"},
    ) as response:
        assert response.status == 202

    async with session.post(
        URL,
        json=source("source1.example"),
        headers={**proxied, "Origin": "https://evil.local"},
    ) as response:
        assert response.status == 403


async def test_the_switch_collects_enabled_saved_sources_and_no_others(
    settings: Settings, worker: FakeWorker,
) -> None:
    collected: list[list[str]] = []

    async def record(plan: RefreshPlan, report: Report) -> None:
        collected.append([source.domain for source in plan.settings.sources])
        await hold(plan, report)

    worker.script = record
    saved = SavedSourceCatalog(FakeSourceRepository(
        Source(uuid.UUID(int=1), enabled=True, domain="saved.example"),
        Source(uuid.UUID(int=2), enabled=False, domain="off.example"),
    ))

    async with serving(
        settings._replace(sources_in_database=True), worker, saved,
    ) as session:
        async with session.post(URL, json=source("saved.example")) as response:
            assert response.status == 202

        for domain in ("off.example", "source0.example"):
            async with session.post(URL, json=source(domain)) as response:
                assert response.status == 400

        # The job starts on the event loop after the request is answered.
        async with session.get(URL) as response:
            assert len((await response.json())["jobs"]) == 1

    assert collected == [["saved.example"]]
