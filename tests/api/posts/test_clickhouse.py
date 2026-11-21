"""ClickHousePostStore against a real database."""

from __future__ import annotations

import datetime as dt
import pathlib
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.clickhouse import ClickHousePostStore
from dank.api.posts.deps import post_store
from dank.api.posts.model import ReaderFilters, Recommendation
from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.model import parse_datetime
from dank.scrub.runner import run_scrub
from dank.storage.clickhouse import ClickHouseClient
from dank.web.assets import assets_path
from tests.api.clickhouse import (
    COLLECTED_AT,
    asset,
    insert,
    post,
    raw_post,
    x_url,
)

pytestmark = pytest.mark.clickhouse

FOR_YOU = "for_you"
SNAPSHOT = COLLECTED_AT.isoformat()


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    path = assets_path(settings)
    path.mkdir(parents=True, exist_ok=True)

    return path


@pytest.fixture
def store(
    clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> ClickHousePostStore:
    return ClickHousePostStore(clickhouse, assets)


@pytest.fixture
def app(app: FastAPI, store: ClickHousePostStore) -> FastAPI:
    app.dependency_overrides[post_store] = lambda: store

    return app


def home_post(
    post_id: str,
    handle: str,
    position: int,
    *,
    author: str = "",
    scraped_at: dt.datetime = COLLECTED_AT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    url = x_url(handle, post_id)

    return (
        post(
            "x.com", post_id, url=url, source="x", author=author or handle,
            updated_at=scraped_at,
        ),
        raw_post(
            "x.com", post_id,
            {"dank_x_home": {"feed": FOR_YOU, "position": position}},
            url=url, source="x", scraped_at=scraped_at,
        ),
    )


async def seed_home(
    client: ClickHouseClient, *rows: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    await insert(client, "posts", [row[0] for row in rows])
    await insert(client, "raw_posts", [row[1] for row in rows])


async def test_scrub_keeps_saved_article_and_offers_youtube_playback(
    client: AsyncClient, clickhouse: ClickHouseClient,
    settings: Settings, clickhouse_settings: ClickHouseSettings,
    assets: pathlib.Path,
) -> None:
    settings = settings._replace(clickhouse=clickhouse_settings)
    domain = "politics.example"
    post_id = "article"
    video_url = "https://www.youtube.com/watch?v=LKrwhjOI8Nc"
    html = "<p>Saved article text</p>"
    path = assets / "clip.mp4"
    path.write_bytes(b"video")
    age = dt.timedelta(days=1)
    cutoff = COLLECTED_AT + dt.timedelta(milliseconds=123)
    before_cutoff = cutoff - dt.timedelta(milliseconds=1)
    await clickhouse.execute("DROP TABLE asset_retention SYNC")
    await insert(clickhouse, "posts", [post(domain, post_id, html=html)])
    await insert(clickhouse, "raw_posts", [
        raw_post(domain, post_id, {}, scraped_at=stamp)
        for stamp in (before_cutoff, cutoff)
    ])
    await insert(clickhouse, "assets", [
        asset(domain, post_id, video_url, path, "video/mp4"),
    ])
    await insert(clickhouse, "raw_assets", [{
        "domain": domain, "post_id": post_id, "url": video_url,
        "asset_type": "youtube", "source": "rss", "local_path": str(path),
        "scraped_at": stamp,
    } for stamp in (before_cutoff, cutoff)])
    url = f"/api/reader/post?domain={domain}&id={post_id}"
    before = await client.get(url)

    assert before.status_code == 200
    assert before.json()["media"] == [{
        "url": "/assets/clip.mp4", "content_type": "video/mp4",
    }]

    preview = await run_scrub(
        settings, age=age, now=cutoff + age, dry_run=True,
    )

    assert (preview.raw_posts, preview.raw_assets, preview.files) == (1, 1, 1)
    assert path.is_file()
    tables = await clickhouse.fetch_json("SHOW TABLES")
    assert "asset_retention" not in {row["name"] for row in tables.rows}

    result = await run_scrub(settings, age=age, now=cutoff + age)

    assert result._replace(dry_run=True) == preview
    assert not path.exists()

    for table in ("raw_posts", "raw_assets"):
        rows = await clickhouse.fetch_json(f"SELECT scraped_at FROM {table}")
        assert [parse_datetime(row["scraped_at"]) for row in rows.rows] == [
            cutoff,
        ]

    assert (await clickhouse.fetch_json(
        "SELECT post_id FROM asset_retention FINAL "
        "WHERE scrubbed_at IS NOT NULL",
    )).rows == [{"post_id": post_id}]
    after = await client.get(url)
    assert after.status_code == 200
    assert after.json()["html"] == html
    assert after.json()["media"] == [{
        "url": video_url, "content_type": "video/youtube",
    }]


async def test_listing_returns_one_more_than_the_limit_in_order(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "1"), post("b.example", "2"), post("c.example", "3"),
    ])

    listed = await store.listing(ReaderFilters(limit=1), (), None, SNAPSHOT)

    assert [item.post.domain for item in listed] == ["c.example", "b.example"]
    assert all(item.recommendation is None for item in listed)


async def test_listing_applies_tags_from_the_configured_sources(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "1"), post("b.example", "2"),
    ])
    sources = (SourceConfig("a.example", (), tags=("politics",)),)

    listed = await store.listing(
        ReaderFilters(tags=("politics",)), sources, None, SNAPSHOT,
    )
    nothing = await store.listing(
        ReaderFilters(tags=("politics",)), (), None, SNAPSHOT,
    )

    assert [item.post.domain for item in listed] == ["a.example"]
    assert nothing == []


async def test_for_you_listing_reports_where_each_post_appeared(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await seed_home(clickhouse, home_post("2", "bob", 7))
    filters = ReaderFilters(kind="x", x_feed=FOR_YOU)

    [item] = await store.listing(filters, (), None, SNAPSHOT)
    before = await store.listing(
        filters, (), None, (COLLECTED_AT - dt.timedelta(hours=1)).isoformat(),
    )

    assert item.post.post_id == "2"
    assert item.recommendation == Recommendation("2026-10-30 12:00:00", 7)
    assert before == []


async def test_post_finds_one_post_or_nothing(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [post("a.example", "1", title="One")])

    found = await store.post("a.example", "1")

    assert found is not None
    assert found.title == "One"
    assert await store.post("a.example", "2") is None


async def test_authors_rank_by_count_and_disambiguate_shared_names(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse,
        *(home_post(str(10 + n), "alice", n, author="Shared") for n in (1, 2)),
        home_post("20", "bob", 3, author="Shared"),
        home_post("30", "i", 4),
    )
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"

    first = (await client.get(path)).json()
    second = (await client.get(f"{path}&cursor={first['next_cursor']}")).json()
    [alice] = first["authors"]
    [bob] = second["authors"]

    assert (first["total"], first["unattributed_posts"]) == (2, 1)
    assert (alice["handle"], alice["name"], alice["count"]) == (
        "alice", "alice", 2,
    )
    assert {item["id"] for item in alice["posts"]} == {"11", "12"}
    assert (bob["handle"], bob["count"]) == ("bob", 1)
    assert second["next_cursor"] is None


async def test_authors_can_narrow_to_one_account(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, home_post("1", "alice", 1), home_post("2", "bob", 2),
    )

    response = await client.get(
        "/api/reader/x/authors?kind=x&x_feed=for_you&account=@Alice",
    )
    body = response.json()

    assert response.status_code == 200
    assert body["total"] == 1
    assert [item["handle"] for item in body["authors"]] == ["alice"]
    assert [item["id"] for item in body["authors"][0]["posts"]] == ["1"]


async def test_authors_show_only_a_few_example_posts(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, *(home_post(str(n), "alice", n) for n in range(1, 6)),
    )

    response = await client.get("/api/reader/x/authors?kind=x&x_feed=for_you")
    [alice] = response.json()["authors"]

    assert alice["count"] == 5
    assert len(alice["posts"]) == 3
    assert all(item["x"]["handle"] == "alice" for item in alice["posts"])


async def test_authors_cursor_keeps_its_cutoff_for_later_pages(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, home_post("1", "alice", 1), home_post("2", "bob", 2),
    )
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"
    cursor = (await client.get(path)).json()["next_cursor"]
    # Recommended after the first page, so outside its session.
    await seed_home(clickhouse, home_post(
        "3", "carol", 3, scraped_at=COLLECTED_AT + dt.timedelta(minutes=1),
    ))

    response = await client.get(f"{path}&cursor={cursor}")

    assert response.json()["total"] == 2
    assert [item["handle"] for item in response.json()["authors"]] == ["bob"]
