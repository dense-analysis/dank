"""Refresh jobs run through the app's lifespan with a scripted worker."""

import asyncio
import json
import pathlib
from collections.abc import AsyncIterator
from typing import Any

import clickhouse_connect
import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api import lifespan
from dank.api.app import create_app
from dank.api.refresh.manager import Report
from dank.api.refresh.model import RefreshPlan
from dank.config import Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from tests.api.client import serving
from tests.api.refresh.fakes import FakeWorker

PATH = "/api/reader/refresh"


class Driver:
    async def close(self) -> None:
        pass


@pytest.fixture
def selected(settings: Settings) -> Settings:
    return settings._replace(sources=(SourceConfig(
        "feed.test", (), feed_urls=("https://feed.test/news.xml",),
    ),))


@pytest.fixture
def running_app(
    selected: Settings, worker: FakeWorker, monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    async def connect(**_options: object) -> Driver:
        return Driver()

    async def prepare(_client: ClickHouseClient) -> None:
        pass

    monkeypatch.setattr(clickhouse_connect, "get_async_client", connect)
    monkeypatch.setattr(lifespan, "prepare_clickhouse", prepare)
    monkeypatch.setattr(lifespan, "ProcessWorker", lambda: worker)

    return create_app(selected)


@pytest.fixture
async def running_client(running_app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with serving(running_app) as client:
        yield client


async def start(client: AsyncClient) -> dict[str, Any]:
    response = await client.post(PATH, json={
        "scope": {"kind": "rss", "domains": ["feed.test"]},
    })

    assert response.status_code == 202

    return response.json()["job"]


async def test_a_job_runs_inside_the_app_lifespan(
    running_client: AsyncClient, worker: FakeWorker,
) -> None:
    reported = asyncio.Event()

    async def complete(plan: RefreshPlan, report: Report) -> None:
        assert plan.settings.sources[0].domain == "feed.test"
        report({
            "status": "completed", "posts_saved": 0, "run_id": "capture-1",
            "message": "Refresh complete. Updates are ready to view.",
        })
        reported.set()

    worker.script = complete
    accepted = await start(running_client)
    await asyncio.wait_for(reported.wait(), 1)
    [done] = (await running_client.get(PATH)).json()["jobs"]

    assert accepted["status"] == "queued"
    assert accepted["label"] == "feed.test"
    assert done["id"] == accepted["id"]
    assert done["status"] == "completed"
    assert done["message"] == "Refresh complete. Updates are ready to view."
    assert done["posts_saved"] == 0
    assert done["run_id"] == "capture-1"
    assert done["finished_at"]


async def test_leaving_the_lifespan_stops_work_and_saves_interruption(
    running_app: FastAPI, selected: Settings, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def hold(_plan: RefreshPlan, _report: Report) -> None:
        entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    worker.script = hold

    async with serving(running_app) as client:
        job = await start(client)
        await asyncio.wait_for(entered.wait(), 1)
        [running] = (await client.get(PATH)).json()["jobs"]

        assert running["status"] == "running"
        assert running["finished_at"] is None

    assert stopped.is_set()
    path = pathlib.Path(selected.data_dir) / "reader-refresh.json"
    [saved] = json.loads(path.read_text())
    assert saved["id"] == job["id"]
    assert saved["status"] == "interrupted"
    assert saved["message"] == "Refresh was interrupted."
    assert saved["finished_at"]
