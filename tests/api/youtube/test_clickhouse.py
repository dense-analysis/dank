"""The YouTube adapters and projections against a real database.

Each test states its rows and checks the JSON, over the real router and
service with the real store swapped in for the fake.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api import deps
from dank.api.youtube.clickhouse import ClickHouseYouTubeStore
from dank.api.youtube.collector import ScrapeVideoCollector, collection_gate
from dank.api.youtube.deps import video_collector, youtube_store
from dank.config import Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.assets import assets_path
from tests.api.clickhouse import COLLECTED_AT, insert
from tests.api.youtube.rows import (
    CHANNEL_URL,
    HOME,
    HYPE,
    channel,
    discovery,
    entry,
    playlist,
    reference,
    video_payload,
)

pytestmark = pytest.mark.clickhouse

ROUTE = "/api/reader/youtube"
VIDEO = "NnV_cWeoo5Q"
EARLIER = COLLECTED_AT - dt.timedelta(hours=2)
VIDEOS = CHANNEL_URL + "/videos"
SHORTS = CHANNEL_URL + "/shorts"
SAME_ORIGIN = {"origin": "http://dank.test"}


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    path = assets_path(settings)
    path.mkdir(parents=True, exist_ok=True)

    return path


@pytest.fixture
def real_store(
    clickhouse: ClickHouseClient, settings: Settings, assets: pathlib.Path,
) -> ClickHouseYouTubeStore:
    return ClickHouseYouTubeStore(clickhouse, settings, assets)


@pytest.fixture
def real_collector(
    clickhouse: ClickHouseClient, settings: Settings,
) -> ScrapeVideoCollector:
    return ScrapeVideoCollector(
        clickhouse,
        metadata=collection_gate(settings.scrape).limiter,
        transcript=collection_gate(settings.scrape).limiter,
    )


@pytest.fixture
def app(
    app: FastAPI,
    real_store: ClickHouseYouTubeStore,
    real_collector: ScrapeVideoCollector,
) -> FastAPI:
    app.dependency_overrides[youtube_store] = lambda: real_store
    app.dependency_overrides[video_collector] = lambda: real_collector

    return app


async def test_channel_counts_are_unique_across_every_feed(
    client: AsyncClient, clickhouse: ClickHouseClient, settings: Settings,
) -> None:
    # A video in both of the channel's feeds counts once; News finds more.
    await insert(clickhouse, "youtube_discoveries", [
        *[
            discovery(
                f"video:{index:011d}", VIDEOS, feed_kind="channel",
                feed_name="@Channel4News", run_id="videos",
            )
            for index in range(5)
        ],
        *[
            discovery(
                f"video:{index:011d}", SHORTS, feed_kind="channel",
                feed_name="@Channel4News", run_id="shorts",
            )
            for index in range(4, 7)
        ],
        *[
            discovery(
                f"video:{index:011d}", HYPE, run_id="hype",
            )
            for index in range(7, 9)
        ],
        discovery("post:notcounted", VIDEOS, feed_kind="channel",
                  feed_name="@Channel4News", run_id="videos", kind="post"),
    ])
    catalog = (SourceConfig("youtube.com", (), (VIDEOS, SHORTS)),)
    settings_with_sources = settings._replace(sources=catalog)
    store = ClickHouseYouTubeStore(
        clickhouse, settings_with_sources, assets_path(settings),
    )
    body = await store.overview(catalog)

    assert [
        (found["url"], found["count"]) for found in body["channels"]
    ] == [(CHANNEL_URL, 9)]
    assert body["total"] == 10


async def test_channel_details_are_local_and_keep_the_rounded_count(
    clickhouse: ClickHouseClient,
    real_store: ClickHouseYouTubeStore,
    assets: pathlib.Path,
) -> None:
    icon = assets / "youtube.com" / "channel.png"
    icon.parent.mkdir()
    icon.write_bytes(b"image")
    await insert(clickhouse, "youtube_channels", [channel(
        "UCexample",
        channel_url="https://www.youtube.com/channel/UCexample",
        feed_url="https://www.youtube.com/@Example/shorts",
        local_path=str(icon),
    )])
    catalog = (SourceConfig("youtube.com", ("Example",)),)

    [shown] = (await real_store.overview(catalog))["channels"]

    assert shown["id"] == "UCexample"
    assert shown["name"] == "Channel 4 News"
    assert shown["avatar_url"] == "/assets/youtube.com/channel.png"
    assert shown["reported_video_count"] == 26000
    assert shown["reported_video_count_text"] == "26K videos"

    icon.unlink()
    [shown] = (await real_store.overview(catalog))["channels"]

    assert shown["avatar_url"] is None


async def test_channels_group_saved_examples_and_report_unknowns(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_discoveries", [
        *[
            discovery(
                f"video:{index:011d}", HOME, feed_kind="home", run_id="home",
                rank=index + 1,
                payload=video_payload(f"Video {index}"),
            )
            for index in range(4)
        ],
        discovery(
            "video:" + "s" * 11, HOME, feed_kind="home", run_id="home",
            kind="short", payload=video_payload("A Short"),
        ),
        *[
            discovery(
                f"video:u{index:010d}", HOME, feed_kind="home", run_id="home",
                payload=video_payload(
                    "No channel", channel_id="", channel_url="",
                ),
            )
            for index in range(2)
        ],
    ])

    response = await client.get(
        f"{ROUTE}/channels?feed={quote(HOME, safe='')}",
    )
    body = response.json()

    assert response.status_code == 200
    assert body["total"] == 1
    assert body["unattributed_items"] == 2
    assert body["next_cursor"] is None

    [group] = body["channels"]

    assert group["id"] == "UC4News"
    assert (group["count"], group["video_count"], group["short_count"]) == (
        5, 4, 1,
    )
    # Three examples, videos before Shorts.
    assert len(group["items"]) == 3
    assert {item["kind"] for item in group["items"]} == {"video"}


async def test_a_configured_channel_is_marked_and_filtered(
    client: AsyncClient,
    app: FastAPI,
    clickhouse: ClickHouseClient,
    settings: Settings,
) -> None:
    await insert(clickhouse, "youtube_discoveries", [
        discovery("video:" + "a" * 11, HOME, feed_kind="home", run_id="home"),
        discovery(
            "video:" + "b" * 11, HOME, feed_kind="home", run_id="home",
            payload=video_payload(
                channel_id="UCother", channel_url="https://other.test",
            ),
        ),
        discovery(
            "video:" + "a" * 11, VIDEOS, feed_kind="channel", run_id="c",
        ),
    ])
    configured = settings._replace(sources=(
        SourceConfig("youtube.com", ("Channel4News",)),
    ))
    app.dependency_overrides[deps.settings] = lambda: configured
    home = quote(HOME, safe="")

    grouped = (await client.get(f"{ROUTE}/channels?feed={home}")).json()
    scoped = (await client.get(
        f"{ROUTE}?feed={home}&channel_scope=configured",
    )).json()

    assert {
        group["id"]: group["configured"] for group in grouped["channels"]
    } == {"UC4News": True, "UCother": False}
    assert [item["id"] for item in scoped["items"]] == ["video:" + "a" * 11]
    assert scoped["total"] == 1


async def test_a_new_capture_cant_change_recommendations_already_paging(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_discoveries", [
        discovery(
            f"video:{letter * 11}", HOME, feed_kind="home", run_id=letter,
            observed_at=COLLECTED_AT - dt.timedelta(hours=hours),
        )
        for letter, hours in (("a", 1), ("b", 2), ("c", 3))
    ])
    home = quote(HOME, safe="")
    first = (await client.get(f"{ROUTE}?feed={home}&limit=1")).json()

    await insert(clickhouse, "youtube_discoveries", [discovery(
        "video:" + "z" * 11, HOME, feed_kind="home", run_id="z",
        observed_at=COLLECTED_AT + dt.timedelta(hours=1),
    )])
    seen = [first["items"][0]["id"]]
    cursor = first["next_cursor"]
    totals = {first["total"]}

    while cursor:
        page = (await client.get(
            f"{ROUTE}?feed={home}&limit=1&cursor={quote(cursor)}",
        )).json()
        seen.append(page["items"][0]["id"])
        totals.add(page["total"])
        cursor = page["next_cursor"]

    assert seen == ["video:" + letter * 11 for letter in "abc"]
    assert totals == {3}

    fresh = (await client.get(f"{ROUTE}?feed={home}&limit=1")).json()

    assert fresh["items"][0]["id"] == "video:" + "z" * 11
    assert fresh["total"] == 4


async def test_a_linked_video_can_be_opened_without_a_feed(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_reference_evidence", [reference(VIDEO)])

    body = (await client.get(f"{ROUTE}?id=video:{VIDEO}")).json()

    assert [item["id"] for item in body["items"]] == [f"video:{VIDEO}"]
    assert body["items"][0]["title"] == f"YouTube video · {VIDEO}"
    assert body["items"][0]["references"][0]["reader_url"] == ""


async def test_a_capture_published_between_pages_cant_change_the_rest(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_playlists", [playlist(
        checked_at=EARLIER, entry_count=2,
    )])
    await insert(clickhouse, "youtube_playlist_entries", [
        entry(1),
        entry(2, "bbbbbbbbbbb"),
        entry(1, "bbbbbbbbbbb", snapshot_hash="new-order"),
        entry(2, snapshot_hash="new-order"),
    ])
    path = f"{ROUTE}/playlists/PLexample?limit=1"
    first = (await client.get(path)).json()

    assert first["items"][0]["video_id"] == "aaaaaaaaaaa"

    # Collection finished before the first request, but its pointer publishes
    # afterwards. A wall-clock cutoff would now silently switch to this order.
    await insert(clickhouse, "youtube_playlists", [playlist(
        checked_at=COLLECTED_AT, entry_count=2, snapshot_hash="new-order",
    )])
    second = (await client.get(
        f"{path}&cursor={quote(first['next_cursor'])}",
    )).json()

    assert second["playlist"]["snapshot_id"] == "old-order"
    assert second["items"][0]["video_id"] == "bbbbbbbbbbb"
    assert second["next_cursor"] is None

    fresh = (await client.get(path)).json()

    assert fresh["playlist"]["snapshot_id"] == "new-order"
    assert fresh["items"][0]["video_id"] == "bbbbbbbbbbb"
