"""Both servers answer the YouTube routes with the same JSON."""

from __future__ import annotations

import base64
import datetime as dt
import json
import pathlib
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote, urlencode

import pytest

from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from tests.api.clickhouse import COLLECTED_AT, insert, post
from tests.api.parity import Get, both_servers
from tests.api.youtube.rows import (
    CHANNEL_URL,
    HOME,
    HYPE,
    channel,
    discovery,
    entry,
    metadata,
    playlist,
    reference,
    thumbnail,
    transcript,
    video_payload,
)

pytestmark = pytest.mark.clickhouse

ROUTE = "/api/reader/youtube"
EARLIER = COLLECTED_AT - dt.timedelta(days=1)
LATER = COLLECTED_AT + dt.timedelta(hours=1)
# One ranked feed, a post and a Short, then what only other sites mention.
HYPE_VIDEO = "aaaaaaaaaaa"
SHORT = "bbbbbbbbbbb"
LINKED = "ccccccccccc"
HOME_VIDEO = "ddddddddddd"
HOME_OTHER = "eeeeeeeeeee"
CHANNEL_VIDEO = "fffffffffff"
UNKNOWN = "zzzzzzzzzzz"
VIDEOS = CHANNEL_URL + "/videos"
SHORTS = CHANNEL_URL + "/shorts"
SOURCES = (
    SourceConfig("youtube.com", ("Channel4News",), (SHORTS,)),
    SourceConfig("politics.example", (), tags=("politics",)),
)


def filtered(route: str = ROUTE, **fields: str) -> str:
    return f"{route}?{urlencode(fields)}"


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
) -> AsyncIterator[tuple[Get, Get]]:
    configured = settings._replace(
        clickhouse=clickhouse_settings, sources=SOURCES,
    )
    assets = assets_path(configured)
    assets.mkdir(parents=True, exist_ok=True)
    image = assets / "current.jpg"
    image.write_bytes(b"saved image")
    video = assets / "video.mp4"
    video.write_bytes(b"saved video")
    avatar = assets / "avatar.png"
    avatar.write_bytes(b"saved avatar")
    await seed(clickhouse, image, video, avatar)

    async with both_servers(configured) as pair:
        yield pair


async def seed(
    client: ClickHouseClient,
    image: pathlib.Path,
    video: pathlib.Path,
    avatar: pathlib.Path,
) -> None:
    far_ahead = dt.datetime.now(dt.UTC) + dt.timedelta(days=7)
    await insert(client, "youtube_discoveries", [
        discovery(
            "video:" + HYPE_VIDEO, run_id="run-0", rank=5, observed_at=EARLIER,
            payload=video_payload("The original title"),
        ),
        discovery(
            "video:" + HYPE_VIDEO, rank=2,
            payload=video_payload(
                "A revised title",
                detail_capture={"status": "captured"},
                thumbnail_url="javascript:alert(1)",
            ),
        ),
        discovery(
            "video:" + SHORT, rank=1, kind="short",
            payload=video_payload("A Short", channel_id="UCother"),
        ),
        discovery(
            "post:Ugxpost1", rank=3, kind="post",
            payload=video_payload(text="A community post", channel_id=""),
        ),
        discovery(
            "video:" + HOME_VIDEO, HOME, feed_kind="home", feed_name="Home",
            run_id="home-1", observed_at=EARLIER,
            payload=video_payload("Recommended"),
        ),
        discovery(
            "video:" + HOME_OTHER, HOME, feed_kind="home", feed_name="Home",
            run_id="home-2", observed_at=COLLECTED_AT,
            payload=video_payload("Newly recommended", channel_id="UCother"),
        ),
        discovery(
            "video:" + CHANNEL_VIDEO, VIDEOS, feed_kind="channel",
            feed_name="@Channel4News", run_id="channel-1",
            payload=video_payload("From the channel"),
        ),
        discovery(
            "video:" + CHANNEL_VIDEO, SHORTS, feed_kind="channel",
            feed_name="@Channel4News", run_id="channel-2",
            payload=video_payload("From the channel"),
        ),
    ])
    await insert(client, "posts", [
        post("reddit.com", "t3_a", title="A processed post"),
    ])
    await insert(client, "youtube_reference_evidence", [
        reference(HYPE_VIDEO),
        reference(HYPE_VIDEO, "x.com", "1", observed_at=EARLIER),
        reference(
            HYPE_VIDEO, kind="comment", reference_id="t1_b",
            observed_at=LATER,
        ),
        reference(LINKED, "x.com", "2", format_hint="short"),
    ])
    await insert(client, "youtube_video_metadata", [
        metadata(HYPE_VIDEO, title="A provider title"),
        metadata(
            HYPE_VIDEO, status="failed", observed_at=LATER,
            retry_after=far_ahead,
        ),
        metadata(LINKED, title="A linked title", retry_after=far_ahead),
        metadata(CHANNEL_VIDEO, status="unavailable", retry_after=far_ahead),
    ])
    await insert(client, "youtube_transcripts", [
        transcript(HYPE_VIDEO),
        transcript(HOME_VIDEO, status="failed", retry_after=far_ahead),
        transcript(CHANNEL_VIDEO, status="unavailable", retry_after=far_ahead),
    ])
    await insert(client, "youtube_thumbnail_observations", [
        thumbnail("video:" + HYPE_VIDEO, str(image)),
    ])
    await insert(client, "raw_assets", [{
        "domain": "youtube.com", "post_id": "video:" + SHORT,
        "url": "https://example.test/video", "asset_type": "video",
        "scraped_at": COLLECTED_AT, "source": "youtube",
        "local_path": str(video),
    }])
    await insert(client, "youtube_channels", [channel(
        local_path=str(avatar),
    )])
    await insert(client, "youtube_playlists", [
        playlist(
            checked_at=EARLIER, snapshot_hash="older-order", entry_count=1,
        ),
        playlist(),
        playlist(
            "PLpartial", title="Half saved", status="partial",
            reported_count=None, entry_count=0, snapshot_hash="empty",
            thumbnail_url="javascript:alert(1)",
            channel_url="file:///tmp/private",
        ),
    ])
    await insert(client, "youtube_playlist_library", [{
        "observed_at": COLLECTED_AT,
        "playlist_ids": ["PLpartial", "PLexample"],
    }])
    await insert(client, "youtube_playlist_entries", [
        entry(1, snapshot_hash="older-order", title="An older order"),
        entry(1, HYPE_VIDEO),
        entry(2, HYPE_VIDEO, title="Saved twice"),
        entry(
            3, "", title="", available=False, duration_seconds=None,
            payload={"videoInfo": {"simpleText": "84K • 10h ago"}},
        ),
        entry(
            4, SHORT, title="A saved Short", duration_seconds=59,
            payload={"shortsLockupViewModel": {"overlayMetadata": {}}},
        ),
    ])


@pytest.mark.parametrize("path", [
    ROUTE,
    filtered(limit="2"),
    # int() has always allowed surrounding spaces.
    filtered(limit=" 2 "),
    filtered(feed=HYPE),
    filtered(feed=f"  {HYPE}  "),
    filtered(feed=HYPE, kind="video"),
    filtered(feed=HYPE, kind="short"),
    filtered(feed=HYPE, kind="post"),
    filtered(feed=HYPE, kind=""),
    filtered(feed=HYPE, q="revised"),
    filtered(feed=HYPE, channel="UCother"),
    filtered(feed=HYPE, channel=CHANNEL_URL),
    filtered(feed=HOME),
    filtered(feed=HOME, channel_scope="configured"),
    filtered(feed="linked"),
    filtered(feed="linked", sort="oldest"),
    filtered(feed="linked", sort="most-linked"),
    filtered(feed="linked", q="shared"),
    filtered(id="video:" + LINKED),
    filtered(id="video:" + HYPE_VIDEO),
    filtered(id="post:Ugxpost1"),
    filtered(channel_scope="configured"),
    filtered(q="no such words"),
    filtered(feed="https://www.youtube.com/never/collected"),
    f"{ROUTE}/sources",
    filtered(f"{ROUTE}/channels", feed=HOME),
    filtered(f"{ROUTE}/channels", feed=HOME, channel_scope="configured"),
    filtered(f"{ROUTE}/channels", feed=HOME, kind="short"),
    filtered(f"{ROUTE}/channels", feed="linked"),
    filtered(f"{ROUTE}/channels", feed=HYPE, q="no such words"),
    f"{ROUTE}/playlists",
    filtered(f"{ROUTE}/playlists", sort="az"),
    filtered(f"{ROUTE}/playlists", sort="recent", q="half"),
    filtered(f"{ROUTE}/playlists", q="no such words"),
    f"{ROUTE}/playlists/PLexample",
    filtered(f"{ROUTE}/playlists/PLexample", q="twice"),
    filtered(f"{ROUTE}/playlists/PLexample", q="no such words"),
    f"{ROUTE}/playlists/PLpartial",
    f"{ROUTE}/transcript/{HYPE_VIDEO}",
    f"{ROUTE}/transcript/{HOME_VIDEO}",
    f"{ROUTE}/transcript/{CHANNEL_VIDEO}",
    f"{ROUTE}/transcript/{SHORT}",
    f"{ROUTE}/transcript/{LINKED}",
])
async def test_both_servers_return_the_same_json(
    servers: tuple[Get, Get], path: str,
) -> None:
    old, new = servers
    expected, actual = await old(path), await new(path)

    assert expected[0] == 200, path
    assert actual == expected


async def test_rows_cover_each_shape_of_item(
    servers: tuple[Get, Get],
) -> None:
    _, new = servers
    _, body = await new(filtered(feed=HYPE))
    items = {item["id"]: item for item in body["items"]}
    video = items["video:" + HYPE_VIDEO]

    assert {item["kind"] for item in body["items"]} == {
        "video", "short", "post",
    }
    assert video["thumbnail_variants"]
    assert video["thumbnail_variants"][0]["title_pairs"]
    assert len(video["title_history"]) == 2
    assert len(video["discoveries"]) == 2
    assert video["reference_count"] == 3
    assert video["metadata_capture"]["available"] is True
    assert items["video:" + SHORT]["video_url"] == "/assets/video.mp4"


async def test_playlist_rows_cover_each_shape_of_entry(
    servers: tuple[Get, Get],
) -> None:
    _, new = servers
    _, body = await new(f"{ROUTE}/playlists/PLexample")

    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == [
        "video:" + HYPE_VIDEO, "video:" + HYPE_VIDEO, "unavailable:3",
        "video:" + SHORT,
    ]
    assert [item["kind"] for item in body["items"]] == [
        "video", "video", "video", "short",
    ]


async def walk(old: Get, new: Get, path: str) -> list[Any]:
    """Page through both servers, comparing every page."""
    pages: list[Any] = []
    cursor = ""

    while True:
        target = f"{path}&cursor={quote(cursor)}" if cursor else path
        expected, actual = await old(target), await new(target)

        assert actual == expected, target
        assert actual[0] == 200, target
        pages.append(actual[1])
        cursor = actual[1]["next_cursor"]

        if not cursor:
            return pages


@pytest.mark.parametrize(("path", "pages"), [
    (filtered(feed=HYPE, limit="1"), 3),
    (filtered(feed=HOME, limit="1"), 2),
    (filtered(feed="linked", limit="1"), 2),
    (filtered(feed="linked", sort="oldest", limit="1"), 2),
    (filtered(feed="linked", sort="most-linked", limit="1"), 2),
    (filtered(f"{ROUTE}/channels", feed=HOME, limit="1"), 2),
    (filtered(f"{ROUTE}/playlists", limit="1"), 2),
    (filtered(f"{ROUTE}/playlists", sort="az", limit="1"), 2),
    (filtered(f"{ROUTE}/playlists/PLexample", limit="1"), 4),
    (filtered(f"{ROUTE}/playlists/PLexample", q="saved", limit="1"), 3),
])
async def test_cursors_page_alike_and_work_on_either_server(
    servers: tuple[Get, Get], path: str, pages: int,
) -> None:
    old, new = servers

    assert len(await walk(old, new, path)) == pages


def without_snapshot(cursor: str | None) -> Any:
    """A channel cursor's fields, minus the moment each server was asked."""
    if cursor is None:
        return None

    fields = json.loads(base64.urlsafe_b64decode(cursor))

    return [fields[0], *fields[2:]]


async def test_channel_pages_match_apart_from_each_servers_snapshot(
    servers: tuple[Get, Get],
) -> None:
    old, new = servers
    path = filtered(f"{ROUTE}/channels", feed=HYPE, limit="1")
    (_, expected), (_, actual) = await old(path), await new(path)
    cursor = expected.pop("next_cursor")

    assert without_snapshot(actual.pop("next_cursor")) == without_snapshot(
        cursor,
    )
    assert actual == expected
    assert [channel["id"] for channel in actual["channels"]] == ["UC4News"]

    # A cursor from one server continues the other's session exactly.
    shown: list[str] = []

    while cursor:
        target = f"{path}&cursor={quote(cursor)}"
        status, body = await old(target)

        assert await new(target) == (status, body)
        shown += [channel["id"] for channel in body["channels"]]
        cursor = body["next_cursor"]

    # The community post has no channel ID, so its URL identifies it.
    assert shown == ["UCother", CHANNEL_URL]


@pytest.mark.parametrize(("path", "status"), [
    (f"{ROUTE}/playlists/PLmissing", 404),
    (f"{ROUTE}/transcript/{UNKNOWN}", 404),
    (f"{ROUTE}/transcript/short", 400),
    (f"{ROUTE}/transcript/{HYPE_VIDEO}x", 400),
    (f"{ROUTE}/transcript/{HYPE_VIDEO}/more", 404),
    (f"{ROUTE}/playlists/PL.bad", 400),
    (f"{ROUTE}/playlists/{'a' * 201}", 400),
    (f"{ROUTE}/playlists/PLexample/more", 404),
    (filtered(page="2"), 400),
    (filtered(kind="live"), 400),
    (filtered(limit="0"), 400),
    (filtered(limit="501"), 400),
    (filtered(limit="no"), 400),
    (filtered(limit="5.0"), 400),
    (filtered(limit=""), 400),
    (filtered(q="a" * 2001), 400),
    (filtered(feed="a" * 2001), 400),
    (filtered(channel="a" * 2001), 400),
    (filtered(id="a" * 2001), 400),
    (filtered(cursor="not base64"), 400),
    (filtered(cursor="W10="), 400),
    (filtered(sort="invalid"), 400),
    (filtered(sort=""), 400),
    (filtered(sort="most-linked"), 400),
    (filtered(feed=HYPE, sort="oldest"), 400),
    (filtered(channel_scope="all-old-feeds"), 400),
    (f"{ROUTE}?kind=video&kind=post", 400),
    (f"{ROUTE}?limit=1&limit=2", 400),
    (filtered(f"{ROUTE}/channels", limit="51"), 400),
    (filtered(f"{ROUTE}/channels", limit="0"), 400),
    (filtered(f"{ROUTE}/channels", limit="5.0"), 400),
    (filtered(f"{ROUTE}/channels", cursor="bad"), 400),
    (f"{ROUTE}/channels?cursor=a&cursor=b", 400),
    (filtered(f"{ROUTE}/channels", id="video:x"), 400),
    (filtered(f"{ROUTE}/channels", channel_scope="unknown"), 400),
    (filtered(f"{ROUTE}/channels", unexpected="true"), 400),
    (filtered(f"{ROUTE}/channels", sort="oldest"), 400),
    (filtered(f"{ROUTE}/playlists", limit="0"), 400),
    (filtered(f"{ROUTE}/playlists", limit="101"), 400),
    (filtered(f"{ROUTE}/playlists", limit="x"), 400),
    (filtered(f"{ROUTE}/playlists", limit="5.0"), 400),
    (filtered(f"{ROUTE}/playlists", cursor="bad"), 400),
    (filtered(f"{ROUTE}/playlists", sort="newest"), 400),
    (filtered(f"{ROUTE}/playlists", q="a" * 2001), 400),
    (filtered(f"{ROUTE}/playlists", unknown="x"), 400),
    (f"{ROUTE}/playlists?q=a&q=b", 400),
    (f"{ROUTE}/playlists?limit=1&limit=2", 400),
    (filtered(f"{ROUTE}/playlists/PLexample", sort="az"), 400),
    (filtered(f"{ROUTE}/playlists/PLexample", limit="101"), 400),
    (filtered(f"{ROUTE}/playlists/PLexample", cursor="bad"), 400),
    (f"{ROUTE}/playlists/PLexample?q=a&q=b", 400),
])
async def test_failures_match(
    servers: tuple[Get, Get], path: str, status: int,
) -> None:
    old, new = servers
    (old_status, old_body), (new_status, new_body) = (
        await old(path), await new(path),
    )

    assert (old_status, new_status) == (status, status), path

    if new_body["code"] in {"invalid_input", "not_found"}:
        assert new_body["error"] == old_body["error"]


@pytest.mark.parametrize("path", [
    f"{ROUTE}/playlists/a%2Fb",
    f"{ROUTE}/transcript/aaaaa%2Faaaaa",
])
async def test_an_encoded_slash_can_not_reach_either_route(
    servers: tuple[Get, Get], path: str,
) -> None:
    # uvicorn decodes the path before routing, so FastAPI finds no route.
    # aiohttp routes first, and then its rule rejects the decoded value.
    old, new = servers

    assert (await old(path))[0] == 400
    assert (await new(path))[0] == 404


@pytest.mark.parametrize("path", [
    f"{ROUTE}/metadata/{LINKED}",
    f"{ROUTE}/metadata/{CHANNEL_VIDEO}",
    f"{ROUTE}/transcript/{HYPE_VIDEO}",
    f"{ROUTE}/transcript/{HOME_VIDEO}",
    f"{ROUTE}/transcript/{CHANNEL_VIDEO}",
])
async def test_cached_collection_returns_the_same_json(
    servers: tuple[Get, Get], path: str,
) -> None:
    # Saved rows that aren't due for a retry, so nothing reaches the network.
    old, new = servers
    expected, actual = await old(path, post={}), await new(path, post={})

    assert expected[0] == 200, path
    assert actual == expected


@pytest.mark.parametrize(("path", "status"), [
    (f"{ROUTE}/metadata/{UNKNOWN}", 404),
    (f"{ROUTE}/transcript/{UNKNOWN}", 404),
    (f"{ROUTE}/metadata/short", 400),
    (f"{ROUTE}/transcript/short", 400),
])
async def test_collection_failures_match(
    servers: tuple[Get, Get], path: str, status: int,
) -> None:
    old, new = servers
    (old_status, old_body), (new_status, new_body) = (
        await old(path, post={}), await new(path, post={}),
    )

    assert (old_status, new_status) == (status, status), path

    if new_body["code"] == "not_found":
        assert new_body["error"] == old_body["error"]
