"""GET /api/reader/sources: HTTP in, JSON out."""

import uuid

from fastapi import FastAPI
from httpx import AsyncClient

from dank.api import deps
from dank.api.sources.deps import saved_sources
from dank.api.sources.model import (
    Community,
    Flair,
    PostTotal,
    RedditActivity,
    XAccount,
    XActivity,
)
from dank.config import Settings, SourceConfig
from dank.sources.model import Source
from tests.api.sources.fakes import FakeCollectionStats
from tests.sources.fakes import FakeSourceRepository


def configure(
    app: FastAPI, settings: Settings, *sources: SourceConfig,
) -> None:
    configured = settings._replace(sources=sources)
    app.dependency_overrides[deps.settings] = lambda: configured


async def test_sources_join_configured_and_collected_domains(
    client: AsyncClient, stats: FakeCollectionStats,
) -> None:
    stats.counts = {"politics.example": 3, "old.example": 7}
    stats.blog_count = 4

    response = await client.get("/api/reader/sources")
    body = response.json()

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert (body["total_posts"], body["blog_external_count"]) == (10, 4)
    assert [
        (source["domain"], source["configured"], source["tags"])
        for source in body["sources"]
    ] == [
        ("empty.example", True, ["culture"]),
        ("old.example", False, []),
        ("politics.example", True, ["politics"]),
    ]
    assert body["sources"][0] == {
        "kind": "rss", "domain": "empty.example", "name": "empty.example",
        "icon_url": "/api/reader/source-icon?domain=empty.example",
        "configured": True, "tags": ["culture"], "count": 0,
    }


async def test_x_sources_carry_accounts_and_for_you_count(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    stats: FakeCollectionStats,
) -> None:
    configure(app, settings, SourceConfig("x.com", ("@WatcherGuru",)))
    stats.counts = {"x.com": 9}
    stats.x["x.com"] = XActivity(
        accounts=(XAccount("watcherguru", "Watcher.Guru", None, 120, 7),),
        for_you_count=5,
    )

    response = await client.get("/api/reader/sources")
    [source] = response.json()["sources"]

    assert (source["kind"], source["icon_url"], source["for_you_count"]) == (
        "x", None, 5,
    )
    assert source["accounts"] == [{
        "handle": "watcherguru", "name": "Watcher.Guru", "icon_url": None,
        "total_posts": 120, "count": 7,
    }]


async def test_reddit_sources_carry_communities_and_flairs(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    stats: FakeCollectionStats,
) -> None:
    configure(app, settings, SourceConfig("reddit.com", (), feed_urls=(
        "https://www.reddit.com/r/LocalLLaMA/",
    )))
    total = PostTotal(
        120, "arctic_shift", "2026-10-30T00:00:00+00:00",
        "2026-10-30T01:00:00+00:00",
    )
    stats.counts = {"reddit.com": 7}
    stats.reddit = RedditActivity(
        communities=(
            Community("LocalLLaMA", 7, None, (Flair("News", 2),), total),
        ),
        flairs=(Flair("News", 2),),
    )

    response = await client.get("/api/reader/sources")
    [source] = response.json()["sources"]

    assert source["kind"] == "reddit"
    assert source["communities"] == [{
        "name": "LocalLLaMA", "count": 7, "icon_url": None,
        "flairs": [{"name": "News", "count": 2}],
        "post_total": {
            "count": 120, "source": "arctic_shift",
            "updated_at": "2026-10-30T00:00:00+00:00",
            "captured_at": "2026-10-30T01:00:00+00:00",
        },
    }]
    assert source["flairs"] == [{"name": "News", "count": 2}]


async def test_unconfigured_reddit_still_lists_its_empty_communities(
    client: AsyncClient, stats: FakeCollectionStats,
) -> None:
    # The one intended difference from the aiohttp server, which leaves
    # communities and flairs out here.
    stats.counts = {"reddit.com": 3}

    response = await client.get("/api/reader/sources")
    [*_, source] = response.json()["sources"]

    assert (source["kind"], source["configured"]) == ("reddit", False)
    assert (source["communities"], source["flairs"]) == ([], [])


async def test_openapi_describes_each_source_kind(client: AsyncClient) -> None:
    schemas = (await client.get("/api/openapi.json")).json()["components"][
        "schemas"
    ]
    union = schemas["SourcesResponse"]["properties"]["sources"]["items"]

    assert union["discriminator"]["mapping"] == {
        "rss": "#/components/schemas/FeedSourceOut",
        "hacker_news": "#/components/schemas/FeedSourceOut",
        "youtube": "#/components/schemas/FeedSourceOut",
        "x": "#/components/schemas/XSourceOut",
        "reddit": "#/components/schemas/RedditSourceOut",
    }
    assert schemas["XSourceOut"]["additionalProperties"] is False


async def test_the_switch_reads_saved_sources_and_ignores_config(
    app: FastAPI, client: AsyncClient, settings: Settings,
) -> None:
    switched = settings._replace(sources_in_database=True)
    repository = FakeSourceRepository(
        Source(
            uuid.UUID(int=1), enabled=True, domain="saved.example",
            tags=("news",),
        ),
        Source(uuid.UUID(int=2), enabled=False, domain="off.example"),
    )
    app.dependency_overrides[deps.settings] = lambda: switched
    app.dependency_overrides[saved_sources] = lambda: repository

    body = (await client.get("/api/reader/sources")).json()

    assert [
        (source["domain"], source["configured"], source["tags"])
        for source in body["sources"]
    ] == [("off.example", True, []), ("saved.example", True, ["news"])]


async def test_the_switch_with_an_empty_table_shows_no_sources(
    app: FastAPI, client: AsyncClient, settings: Settings,
) -> None:
    switched = settings._replace(sources_in_database=True)
    app.dependency_overrides[deps.settings] = lambda: switched
    app.dependency_overrides[saved_sources] = lambda: FakeSourceRepository()

    response = await client.get("/api/reader/sources")

    assert response.status_code == 200
    assert response.json()["sources"] == []


async def test_the_sidebar_says_where_its_sources_come_from(
    app: FastAPI, client: AsyncClient, settings: Settings,
) -> None:
    # The reader watches for edits only when the sources come from Postgres.
    config = (await client.get("/api/reader/sources")).json()
    switched = settings._replace(sources_in_database=True)
    app.dependency_overrides[deps.settings] = lambda: switched
    app.dependency_overrides[saved_sources] = lambda: FakeSourceRepository()
    saved = (await client.get("/api/reader/sources")).json()

    assert (config["in_database"], saved["in_database"]) == (False, True)
