"""Reader projections and query parameters with synthetic driver responses."""

import datetime as dt
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.youtube.clickhouse import ClickHouseYouTubeStore
from dank.api.youtube.collector import ScrapeVideoCollector, collection_gate
from dank.api.youtube.deps import video_collector, youtube_store
from dank.config import Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import COLLECTED_AT, QueryResults
from tests.api.youtube.rows import (
    CHANNEL_URL,
    HOME,
    channel,
    discovery,
    entry,
    metadata,
    playlist,
    transcript,
)

ROUTE = "/api/reader/youtube"
VIDEO = "NnV_cWeoo5Q"
SAME_ORIGIN = {"origin": "http://dank.test"}


@pytest.fixture
def queries(app: FastAPI, settings: Settings) -> QueryResults:
    replies = QueryResults(())
    client = cast(ClickHouseClient, replies)
    store = ClickHouseYouTubeStore(client, settings, Path("/fixture/assets"))
    app.dependency_overrides[youtube_store] = lambda: store

    return replies


@pytest.fixture
def use_saved_captures(
    app: FastAPI, settings: Settings, queries: QueryResults,
) -> None:
    collector = ScrapeVideoCollector(
        cast(ClickHouseClient, queries),
        metadata=collection_gate(settings.scrape).limiter,
        transcript=collection_gate(settings.scrape).limiter,
    )
    app.dependency_overrides[video_collector] = lambda: collector


async def test_the_library_reports_its_count_and_status(
    client: AsyncClient, queries: QueryResults,
) -> None:
    first = {**playlist(status="partial"), "sort_key": "saved music"}
    second = {**playlist("PLsecond", title="Second"), "sort_key": "second"}
    queries.responses.extend((
        [{"snapshot_time": COLLECTED_AT}], [first, second], [{"total": 2}],
        [second], [{"total": 2}],
    ))

    body = (await client.get(f"{ROUTE}/playlists?limit=1")).json()

    assert body["total"] == 2
    assert body["next_cursor"]
    [shown] = body["playlists"]
    assert shown["status"] == "partial"
    assert shown["complete"] is False
    assert shown["snapshot_id"] == "old-order"

    following = (await client.get(
        f"{ROUTE}/playlists?limit=1&cursor={quote(body['next_cursor'])}",
    )).json()

    assert [found["id"] for found in following["playlists"]] == ["PLsecond"]
    assert following["total"] == 2
    assert following["next_cursor"] is None
    assert not queries.responses


async def test_an_empty_library_has_a_valid_cutoff(
    client: AsyncClient, queries: QueryResults,
) -> None:
    queries.responses.extend(([], [], []))
    response = await client.get(f"{ROUTE}/playlists")

    assert (response.status_code, response.json()) == (200, {
        "playlists": [], "total": 0, "next_cursor": None,
    })
    assert not queries.responses


async def test_a_playlist_keeps_duplicates_and_unavailable_positions(
    client: AsyncClient, queries: QueryResults,
) -> None:
    queries.responses.extend((
        [{"snapshot_time": COLLECTED_AT}], [playlist()], [
            entry(1), entry(2),
            entry(3, "", available=False, title="[Private video]"),
        ], [], [], [], [], [],
    ))
    body = (await client.get(f"{ROUTE}/playlists/PLexample")).json()

    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == [
        "video:aaaaaaaaaaa", "video:aaaaaaaaaaa", "unavailable:3",
    ]
    assert [item["playlist_position"] for item in body["items"]] == [1, 2, 3]
    assert [item["playlist_entry_id"] for item in body["items"]] == [
        "entry-1", "entry-2", "entry-3",
    ]
    assert body["items"][0]["duration_text"] == "1:01:01"
    assert body["items"][2]["available"] is False
    assert body["items"][2]["url"] == ""
    assert body["items"][2]["video_url"] is None
    assert not queries.responses


async def test_a_missing_playlist_answers_404(
    client: AsyncClient, queries: QueryResults,
) -> None:
    queries.responses.extend(([], []))
    response = await client.get(f"{ROUTE}/playlists/missing")

    assert (response.status_code, response.json()["error"]) == (
        404, "Playlist not found",
    )
    assert not queries.responses


async def test_playlist_search_keeps_the_snapshot_and_positions(
    client: AsyncClient, queries: QueryResults,
) -> None:
    saved = playlist(entry_count=100)
    earlier = entry(34, title="Findable")
    later = entry(89, title="Findable")
    queries.responses.extend((
        [{"snapshot_time": COLLECTED_AT}], [saved], [earlier, later],
        [{"total": 2}], [], [], [], [], [],
        [saved], [later], [{"total": 2}], [], [], [], [], [],
    ))
    path = f"{ROUTE}/playlists/PLexample?q=Findable&limit=1"
    first = (await client.get(path)).json()
    second = (await client.get(
        f"{path}&cursor={quote(first['next_cursor'])}",
    )).json()

    assert first["total"] == second["total"] == 2
    assert first["playlist"]["collected_count"] == 100
    assert first["items"][0]["playlist_position"] == 34
    assert second["items"][0]["playlist_position"] == 89
    assert second["next_cursor"] is None
    assert queries.requests[1][1] == queries.requests[9][1] == {
        "playlist_id": "PLexample", "snapshot": COLLECTED_AT.isoformat(),
    }
    assert queries.requests[10][1] == {
        "playlist_id": "PLexample", "snapshot_hash": "old-order",
        "q": "Findable", "after": 34, "limit": 2,
    }
    assert not queries.responses


async def test_recommendation_pages_reuse_the_computed_snapshot(
    client: AsyncClient, queries: QueryResults,
) -> None:
    first_row = discovery("video:aaaaaaaaaaa", HOME)
    second_row = discovery("video:bbbbbbbbbbb", HOME)
    queries.responses.extend((
        [{"snapshot_time": COLLECTED_AT}], [first_row, second_row],
        [{"total": 2}], [], [], [], [], [],
        [second_row], [{"total": 2}], [], [], [], [], [],
    ))
    path = f"{ROUTE}?feed={quote(HOME, safe='')}&limit=1"
    first = (await client.get(path)).json()
    second = (await client.get(
        f"{path}&cursor={quote(first['next_cursor'])}",
    )).json()

    assert first["total"] == second["total"] == 2
    assert [page["items"][0]["id"] for page in (first, second)] == [
        first_row["item_id"], second_row["item_id"],
    ]
    assert second["next_cursor"] is None
    assert not queries.responses


async def test_overview_reports_channel_counts_and_local_artwork(
    settings: Settings, memory_path: Path,
) -> None:
    icon = memory_path / "channel.png"
    icon.touch()
    feed = CHANNEL_URL + "/videos"
    saved = channel("UC4News", channel_url=CHANNEL_URL, feed_url=feed,
                    local_path=str(icon))
    catalog = (SourceConfig("youtube.com", (), (feed,)),)
    queries = QueryResults((
        [], [saved], [{"id": "UC4News", "count": 9}],
        [{"total": 10}], [{"total": 3}],
    ))
    store = ClickHouseYouTubeStore(
        cast(ClickHouseClient, queries), settings, memory_path,
    )

    body = await store.overview(catalog)

    assert body["total"] == 10
    assert body["linked_count"] == 3
    assert feed in {found["url"] for found in body["feeds"]}
    [shown] = body["channels"]
    assert shown["url"] == CHANNEL_URL
    assert shown["count"] == 9
    assert shown["avatar_url"] == "/assets/channel.png"
    assert shown["reported_video_count"] == 26000
    assert shown["reported_video_count_text"] == "26K videos"
    assert not queries.responses


async def test_channel_groups_keep_previews_and_report_unattributed_items(
    client: AsyncClient, queries: QueryResults,
) -> None:
    group = {
        "channel_key": "UC4News", "name": "Channel 4 News", "url": CHANNEL_URL,
        "count": 5, "video_count": 4, "short_count": 1, "post_count": 0,
    }
    examples = [{
        **discovery(f"video:{index:011d}", HOME, feed_kind="home"),
        "channel_key": "UC4News",
    } for index in range(3)]
    queries.responses.extend((
        [], [{"snapshot_time": COLLECTED_AT}], [group],
        [{"total": 1, "unattributed_items": 2}], examples, [], [], [], [], [],
    ))
    response = await client.get(
        f"{ROUTE}/channels?feed={quote(HOME, safe='')}",
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["unattributed_items"] == 2
    assert body["next_cursor"] is None
    [found] = body["channels"]
    assert found["id"] == "UC4News"
    assert (found["count"], found["video_count"], found["short_count"]) == (
        5, 4, 1,
    )
    assert [item["id"] for item in found["items"]] == [
        example["item_id"] for example in examples
    ]
    assert not queries.responses


@pytest.mark.parametrize("row", [[], [{"video_id": VIDEO}]])
async def test_video_lookup_checks_both_sources(
    settings: Settings, row: list[dict[str, Any]],
) -> None:
    queries = QueryResults((row,))
    store = ClickHouseYouTubeStore(
        cast(ClickHouseClient, queries), settings, Path("/fixture/assets"),
    )

    assert await store.is_known(VIDEO) is bool(row)
    [(sql, params)] = queries.requests
    assert "FROM youtube_reference_evidence" in sql
    assert "FROM youtube_discoveries" in sql
    assert params == {"id": VIDEO, "item_id": f"video:{VIDEO}"}


@pytest.mark.parametrize("row", [[], [{"observed_at": COLLECTED_AT}]])
async def test_the_last_capture_requests_only_successes(
    settings: Settings, row: list[dict[str, Any]],
) -> None:
    queries = QueryResults((row,))
    store = ClickHouseYouTubeStore(
        cast(ClickHouseClient, queries), settings, Path("/fixture/assets"),
    )

    assert await store.last_metadata_capture(VIDEO) == (
        COLLECTED_AT.isoformat() if row else None
    )
    [(sql, params)] = queries.requests
    assert "status = 'captured'" in sql
    assert "ORDER BY observed_at DESC LIMIT 1" in sql
    assert params == {"id": VIDEO}


async def test_a_saved_transcript_hides_its_raw_fields(
    client: AsyncClient, queries: QueryResults,
) -> None:
    queries.responses.extend(([{"video_id": VIDEO}], [transcript(VIDEO)]))
    response = await client.get(f"{ROUTE}/transcript/{VIDEO}")
    body = response.json()

    assert response.status_code == 200
    assert body["segments"][0] == {
        "start": 1.5, "duration": 2.0, "text": "Saved words",
    }
    assert body["generated"] is True
    assert body["retry_after"] == ""
    assert "private" not in response.text
    assert not queries.responses
    assert "ORDER BY observed_at DESC LIMIT 1" in queries.requests[-1][0]


@pytest.mark.parametrize("status", ["captured", "failed"])
async def test_a_cached_capture_needs_no_web_request(
    client: AsyncClient, queries: QueryResults, use_saved_captures: None,
    status: str,
) -> None:
    retry = dt.datetime(2100, 1, 1, tzinfo=dt.UTC)
    queries.responses.extend((
        [{"video_id": VIDEO}],
        [metadata(VIDEO, status=status, retry_after=retry)],
    ))
    captured = COLLECTED_AT

    if status == "failed":
        captured -= dt.timedelta(hours=2)
        queries.responses.append([{"observed_at": captured}])

    response = await client.post(
        f"{ROUTE}/metadata/{VIDEO}", json={}, headers=SAME_ORIGIN,
    )

    assert (response.status_code, response.json()) == (200, {
        "video_id": VIDEO, "status": status, "provider": "youtube_oembed",
        "cached": True, "observed_at": COLLECTED_AT.isoformat(),
        "retry_after": retry.isoformat(), "available": True,
        "captured_at": captured.isoformat(),
    })
    assert not queries.responses
    assert not queries.writes


async def test_saved_captions_are_reused_without_a_web_request(
    client: AsyncClient, queries: QueryResults, use_saved_captures: None,
) -> None:
    queries.responses.extend(([{"video_id": VIDEO}], [transcript(VIDEO)]))
    response = await client.post(
        f"{ROUTE}/transcript/{VIDEO}", json={}, headers=SAME_ORIGIN,
    )

    assert response.status_code == 200
    assert response.json()["segments"][1]["text"] == "More words"
    assert response.json()["retry_after"] == ""
    assert "private" not in response.text
    assert not queries.responses
    assert not queries.writes
