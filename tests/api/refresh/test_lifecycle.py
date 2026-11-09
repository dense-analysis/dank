"""Real jobs through the real app: its lifespan, spawned workers and storage.

Each job collects from a feed server on this machine, so nothing leaves it. An
empty feed saves no posts, so no embedding model loads.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import pathlib
from typing import Any

import httpx
import pytest

from dank.api.app import create_app
from dank.config import ClickHouseSettings, Settings, SourceConfig
from tests.api.client import serving

pytestmark = pytest.mark.clickhouse

PATH = "/api/reader/refresh"
# A worker imports the scrapers before it does anything else.
WORKER_SECONDS = 60


def with_feed(
    settings: Settings,
    clickhouse: ClickHouseSettings,
    port: int,
    path: str,
) -> Settings:
    return settings._replace(
        clickhouse=clickhouse,
        sources=(
            SourceConfig(
                "feed.test", (),
                feed_urls=(f"http://127.0.0.1:{port}{path}",),
            ),
        ),
    )


async def job_when(
    client: httpx.AsyncClient, job_id: str, statuses: set[str],
) -> dict[str, Any]:
    async with asyncio.timeout(WORKER_SECONDS):
        while True:
            jobs = (await client.get(PATH)).json()["jobs"]
            job = next(item for item in jobs if item["id"] == job_id)

            if job["status"] in statuses:
                return job

            await asyncio.sleep(0.1)


async def test_a_job_runs_in_a_real_worker_to_the_end(
    settings: Settings,
    clickhouse_settings: ClickHouseSettings,
    feed_port: int,
) -> None:
    selected = with_feed(
        settings, clickhouse_settings, feed_port, "/empty.xml",
    )

    async with serving(create_app(selected)) as client:
        response = await client.post(PATH, json={
            "scope": {"kind": "rss", "domains": ["feed.test"]},
        })

        assert response.status_code == 202

        accepted = response.json()["job"]

        assert accepted["status"] == "queued"
        assert accepted["label"] == "feed.test"

        done = await job_when(
            client, accepted["id"], {"completed", "partial", "failed"},
        )

    assert done["status"] == "completed"
    assert done["message"] == "Refresh complete. Updates are ready to view."
    assert done["posts_saved"] == 0
    assert done["run_id"]
    assert done["finished_at"]


async def test_shutdown_stops_a_running_worker_and_interrupts_its_job(
    settings: Settings,
    clickhouse_settings: ClickHouseSettings,
    feed_port: int,
    tmp_path: pathlib.Path,
) -> None:
    selected = with_feed(
        settings, clickhouse_settings, feed_port, "/hang.xml",
    )
    entered = tmp_path / "feed-entered"

    async with serving(create_app(selected)) as client:
        response = await client.post(PATH, json={
            "scope": {"kind": "rss", "domains": ["feed.test"]},
        })
        job = response.json()["job"]

        async with asyncio.timeout(WORKER_SECONDS):
            while not entered.exists():  # noqa: ASYNC110 - another process
                await asyncio.sleep(0.1)

        running = await job_when(client, job["id"], {"running"})

        assert running["finished_at"] is None
        assert [
            child.name for child in multiprocessing.active_children()
            if child.name.startswith("dank-refresh-")
        ] == [f"dank-refresh-{job['id']}"]

    # Leaving the lifespan waited for the worker to go.
    assert not [
        child for child in multiprocessing.active_children()
        if child.name.startswith("dank-refresh-")
    ]

    [saved] = json.loads(
        (pathlib.Path(selected.data_dir) / "reader-refresh.json").read_text(),
    )

    assert saved["id"] == job["id"]
    assert saved["status"] == "interrupted"
    assert saved["message"] == "Refresh was interrupted."
    assert saved["finished_at"]
