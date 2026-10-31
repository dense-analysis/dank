"""Both servers answer GET /api/reader/sources with the same JSON.

Each server starts for real, with its own startup work, over one seeded
throwaway database. Deleted with the aiohttp server in DEN-51.
"""

import datetime as dt
import pathlib
import tempfile
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from dank.api.app import create_app as create_api_app
from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import create_app as create_aiohttp_app
from tests.api.clickhouse import (
    COLLECTED_AT,
    insert,
    post,
    raw_post,
    reddit_url,
    x_url,
)
from tests.api.client import BASE_URL, serving

pytestmark = pytest.mark.clickhouse

PATH = "/api/reader/sources"
CONFIGURED = (
    SourceConfig("politics.example", (), tags=("politics",)),
    SourceConfig("empty.example", (), tags=("culture",)),
    SourceConfig("x.com", ("@WatcherGuru", "Empty_Account")),
)
REDDIT = SourceConfig("reddit.com", (), feed_urls=(
    "https://www.reddit.com/r/LocalLLaMA/",
    "https://www.reddit.com/r/accelerate/",
))


async def seed(client: ClickHouseClient) -> None:
    """Posts of every kind, with the extras X and Reddit sources show."""
    await insert(client, "posts", [
        post("politics.example", "1"),
        post(
            "politics.example", "1",
            updated_at=COLLECTED_AT + dt.timedelta(days=1),
        ),
        post("old.example", "2"),
        post("news.ycombinator.com", "3", source="hacker_news"),
        post("youtube.com", "4", source="youtube"),
        post("x.com", "5", url=x_url("WatcherGuru", "5"), source="x"),
        post("x.com", "6", url=x_url("OtherAuthor", "6"), source="x"),
        post("reddit.com", "t3_a", url=reddit_url("LocalLLaMA", "a")),
        post("reddit.com", "t3_b", url=reddit_url("python", "b")),
    ])
    await insert(client, "raw_posts", [
        raw_post("x.com", "5", {
            "core": {"user_results": {"result": {
                "core": {"screen_name": "WatcherGuru", "name": "Watcher"},
            }}},
            "dank_x_home": {"feed": "for_you"},
        }, url=x_url("WatcherGuru", "5"), source="x"),
        raw_post("reddit.com", "t3_a", {"link_flair_text": "News"}),
        raw_post("reddit.com", "t3_b", {"link_flair_text": "Help"}),
    ])
    await insert(client, "reddit_community_totals", [{
        "community": "localllama", "total_posts": 120,
        "source": "arctic_shift", "updated_at": COLLECTED_AT,
        "scraped_at": COLLECTED_AT, "request_url": "", "payload": "{}",
    }])


async def from_aiohttp(settings: Settings) -> Any:
    runner = web.AppRunner(create_aiohttp_app(settings, page_size=50))
    # Runs the aiohttp startup contexts against the throwaway database.
    await runner.setup()

    try:
        # A Unix socket, since tests may only dial ClickHouse over TCP.
        with tempfile.TemporaryDirectory() as directory:
            path = str(pathlib.Path(directory) / "web.sock")
            await web.UnixSite(runner, path).start()
            connector = aiohttp.UnixConnector(path=path)

            async with (
                aiohttp.ClientSession(connector=connector) as session,
                session.get(BASE_URL + PATH) as response,
            ):
                assert response.status == 200

                return await response.json()
    finally:
        await runner.cleanup()


async def from_fastapi(settings: Settings) -> Any:
    async with serving(create_api_app(settings, page_size=50)) as client:
        response = await client.get(PATH)

    assert response.status_code == 200

    return response.json()


async def test_both_servers_return_the_same_sources(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
) -> None:
    await seed(clickhouse)
    configured = settings._replace(
        clickhouse=clickhouse_settings, sources=(*CONFIGURED, REDDIT),
    )

    expected = await from_aiohttp(configured)
    actual = await from_fastapi(configured)

    assert actual == expected
    assert {source["kind"] for source in actual["sources"]} == {
        "rss", "hacker_news", "youtube", "x", "reddit",
    }


async def test_unconfigured_reddit_is_the_one_difference(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
) -> None:
    await seed(clickhouse)
    configured = settings._replace(
        clickhouse=clickhouse_settings, sources=CONFIGURED,
    )

    expected = await from_aiohttp(configured)
    actual = await from_fastapi(configured)
    [reddit] = [
        source for source in expected["sources"]
        if source["domain"] == "reddit.com"
    ]

    # aiohttp leaves these out; the typed variant always has them.
    assert "communities" not in reddit
    reddit |= {"communities": [], "flairs": []}
    assert actual == expected
