"""Both servers answer the refresh routes the same way."""

from __future__ import annotations

import asyncio
import json
import pathlib
import tempfile
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from dank.api.app import create_app as create_api_app
from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.web.app import create_app as create_aiohttp_app
from tests.api.client import BASE_URL, serving
from tests.api.parity import both_servers

pytestmark = pytest.mark.clickhouse

PATH = "/api/reader/refresh"
JSON = {"content-type": "application/json"}
SOURCES = (
    SourceConfig("blog.example", (), tags=("tech",)),
    SourceConfig("reddit.com", (), feed_urls=(
        "https://www.reddit.com/r/Python/",
    )),
    SourceConfig("x.com", ("alice",)),
    SourceConfig("youtube.com", ("alice",)),
)
SCOPES: tuple[dict[str, Any], ...] = (
    {"kind": "all"},
    {"kind": "all", "domains": ["blog.example"], "tags": ["tech"],
     "platforms": ["rss"]},
    {"kind": "rss", "blog_feed": "sources"},
    {"kind": "reddit", "community": "python"},
    {"kind": "x", "account": "alice"},
    {"kind": "x", "feed_id": "for_you"},
    {"kind": "hacker_news"},
    {"kind": "youtube", "channel_id": "UC1234567890123456789012"},
    {"kind": "youtube", "feed_id": "playlists"},
    {"kind": "youtube", "playlist_id": "PLsaved"},
    {"kind": "post", "domain": "blog.example", "post_id": "one"},
)
# A body and its headers, the status both servers answer, and their wording.
WORDED: tuple[tuple[Any, dict[str, str], int, str], ...] = (
    ({"scope": {"kind": "all"}}, {"origin": "https://foreign.example"}, 403,
     "Same-origin request required"),
    ({"scope": {"kind": "all"}}, {"sec-fetch-site": "cross-site"}, 403,
     "Same-origin request required"),
    ({"scope": {"kind": "all"}}, {"content-type": "text/plain"}, 415,
     "JSON request required"),
    ({"scope": {"kind": "all", "domains": ["unknown.example"]}}, {}, 400,
     "Refresh includes a source that is no longer configured"),
    ({"scope": {"kind": "all", "tags": ["unknown"]}}, {}, 400,
     "Refresh includes a tag that is no longer configured"),
    ({"scope": {"kind": "hacker_news"}}, {}, 400,
     "No configured sources match this refresh context"),
    ({"scope": {"kind": "reddit", "community": "bad/name"}}, {}, 400,
     "Invalid Reddit community"),
    ({"scope": {"kind": "rss", "blog_feed": "external", "tags": ["tech"]}},
     {}, 400, "External blog refresh cannot use publisher domains or tags"),
    ({"scope": {"kind": "post", "domain": "blog.example", "post_id": "none"}},
     {}, 400, "This post has no saved collection target"),
    ({"scope": {
        "kind": "post", "domain": "youtube.com", "post_id": "video:x",
    }}, {}, 400, "Only individual YouTube videos can be refreshed"),
    ({"scope": {"kind": "youtube", "channel_id": "UC1234567890123456789012"}},
     {}, 400, "This YouTube channel or feed is not configured"),
    ({"scope": {"kind": "youtube", "channel_id": "http://["}}, {}, 400,
     "Invalid IPv6 URL"),
)
# Rejected by both, in Pydantic's generic wording here: see docs/api.md.
SHAPED: tuple[Any, ...] = (
    b"{not json",
    b"",
    {},
    [],
    {"scope": {"kind": "all"}, "url": "https://foreign.example"},
    {"scope": None},
    {"scope": {}},
    {"scope": {"kind": "unsupported"}},
    {"scope": {"kind": "all", "community": "python"}},
    {"scope": {"kind": "all", "domains": "blog.example"}},
    {"scope": {"kind": "all", "platforms": ["stories"]}},
    {"scope": {"kind": "rss", "blog_feed": "unknown"}},
    {"scope": {"kind": "post", "domain": "blog.example"}},
)


def encoded(body: Any) -> bytes | None:
    if body is None or isinstance(body, bytes):
        return body

    return json.dumps(body).encode()


type Send = Callable[
    [str, Any, dict[str, str]], Awaitable[tuple[int, str]],
]


@asynccontextmanager
async def aiohttp_requests(settings: Settings) -> AsyncGenerator[Send]:
    """Any request to the aiohttp server, headers and bad bodies included."""
    runner = web.AppRunner(create_aiohttp_app(settings))
    await runner.setup()

    try:
        # A Unix socket, since tests may only dial ClickHouse over TCP.
        with tempfile.TemporaryDirectory() as directory:
            path = str(pathlib.Path(directory) / "web.sock")
            await web.UnixSite(runner, path).start()
            connector = aiohttp.UnixConnector(path=path)

            async with aiohttp.ClientSession(connector=connector) as session:

                async def send(
                    method: str, body: Any, headers: dict[str, str],
                ) -> tuple[int, str]:
                    async with session.request(
                        method,
                        BASE_URL + PATH,
                        data=encoded(body),
                        headers=JSON | headers,
                    ) as response:
                        return response.status, await response.text()

                yield send
    finally:
        await runner.cleanup()


@asynccontextmanager
async def fastapi_requests(settings: Settings) -> AsyncGenerator[Send]:
    async with serving(create_api_app(settings)) as client:

        async def send(
            method: str, body: Any, headers: dict[str, str],
        ) -> tuple[int, str]:
            response = await client.request(
                method,
                PATH,
                content=encoded(body),
                headers=JSON | headers,
            )

            return response.status_code, response.text

        yield send


def job(index: int, scope: dict[str, Any], **fields: Any) -> dict[str, Any]:
    return {
        "id": f"job-{index}",
        "scope": scope,
        "label": f"Job {index}",
        "status": "completed",
        "message": "Refresh complete. Updates are ready to view.",
        "created_at": "2026-10-26T20:13:11.066599+00:00",
        "finished_at": "2026-10-26T20:13:34.714449+00:00",
        "posts_saved": index,
        "run_id": f"run-{index}",
        **fields,
    }


def configure(
    settings: Settings, clickhouse: ClickHouseSettings,
) -> Settings:
    return settings._replace(clickhouse=clickhouse, sources=SOURCES)


async def test_saved_history_reads_the_same_on_both_servers(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    saved = [job(index, scope) for index, scope in enumerate(SCOPES)]
    saved[1] = job(1, SCOPES[1], status="partial", run_id=None)
    saved[2] = job(2, SCOPES[2], status="failed")
    saved[3] = job(3, SCOPES[3], status="interrupted")
    # Restored as interrupted, by the first server to start.
    saved.append(job(11, {"kind": "all"}, status="running", finished_at=None))
    (pathlib.Path(settings.data_dir) / "reader-refresh.json").write_text(
        json.dumps(saved),
    )

    async with both_servers(
        configure(settings, clickhouse_settings),
    ) as (old, new):
        (old_status, expected), (new_status, actual) = (
            await old(PATH), await new(PATH),
        )

    assert (old_status, new_status) == (200, 200)
    assert actual == expected
    assert len(actual["jobs"]) == len(saved)
    assert actual["jobs"][0]["status"] == "interrupted"
    assert actual["jobs"][1]["scope"] == SCOPES[-1]


async def test_empty_history_reads_the_same_on_both_servers(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    async with both_servers(
        configure(settings, clickhouse_settings),
    ) as (old, new):
        assert await old(PATH) == await new(PATH) == (200, {"jobs": []})


async def test_both_servers_reject_the_same_requests(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    selected = configure(settings, clickhouse_settings)

    async with aiohttp_requests(selected) as old, fastapi_requests(
        selected,
    ) as new:
        for body, headers, status, wording in WORDED:
            expected = await old("POST", body, headers)
            actual = await new("POST", body, headers)

            assert (expected[0], actual[0]) == (status, status), body
            assert json.loads(expected[1])["error"] == wording, body
            assert json.loads(actual[1])["error"] == wording, body

        for body in SHAPED:
            expected = await old("POST", body, {})
            actual = await new("POST", body, {})

            assert (expected[0], actual[0]) == (400, 400), body
            assert json.loads(expected[1])["error"], body
            assert json.loads(actual[1])["code"] == "invalid_request", body

        # Neither queued anything.
        assert json.loads((await old("GET", None, {}))[1]) == {"jobs": []}
        assert json.loads((await new("GET", None, {}))[1]) == {"jobs": []}


async def finished_job(send: Send, scope: dict[str, Any]) -> dict[str, Any]:
    """Run a job for real and return it without what differs by run."""
    status, body = await send("POST", {"scope": scope}, {})

    assert status == 202, body

    accepted = json.loads(body)["job"]
    queued = {
        key: accepted[key] for key in ("scope", "label", "status", "message")
    }

    assert queued["status"] == "queued"

    async with asyncio.timeout(60):
        while True:
            _, listing = await send("GET", None, {})
            [current] = json.loads(listing)["jobs"]

            if current["status"] not in {"queued", "running", "processing"}:
                break

            await asyncio.sleep(0.1)

    assert current["id"] == accepted["id"]
    assert current["run_id"]
    assert current["finished_at"]

    return {
        **{
            key: value for key, value in current.items()
            if key not in {"id", "created_at", "finished_at", "run_id"}
        },
        "queued": queued,
    }


async def test_both_servers_run_a_job_in_a_real_worker_the_same_way(
    settings: Settings,
    clickhouse_settings: ClickHouseSettings,
    feed_port: int,
    tmp_path: pathlib.Path,
) -> None:
    selected = configure(settings, clickhouse_settings)._replace(sources=(
        SourceConfig("feed.test", (), feed_urls=(
            f"http://127.0.0.1:{feed_port}/empty.xml",
        )),
    ))
    scope = {"kind": "rss", "domains": ["feed.test"]}

    async with aiohttp_requests(
        selected._replace(data_dir=tmp_path / "aiohttp"),
    ) as old:
        expected = await finished_job(old, scope)

    async with fastapi_requests(
        selected._replace(data_dir=tmp_path / "fastapi"),
    ) as new:
        actual = await finished_job(new, scope)

    assert actual == expected
    assert actual["status"] == "completed"
