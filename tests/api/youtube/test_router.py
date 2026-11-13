"""The YouTube routes: HTTP in, JSON out, with fakes behind the ports."""

from __future__ import annotations

import asyncio
from urllib.parse import urlencode

import httpx
import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.youtube.deps import collection_limits
from dank.api.youtube.model import (
    ChannelBoundary,
    ChannelListing,
    ItemBoundary,
    ItemListing,
    PlaylistEntries,
    PlaylistListing,
    YouTubeFilters,
    channel_cursor,
    decode_youtube_cursor,
    encode_youtube_cursor,
    parse_channel_cursor,
    parse_cursor,
    playlist_cursor,
    youtube_cursor_snapshot,
)
from dank.api.youtube.service import CollectionLimits, CollectionLocks
from dank.scrape.youtube.transcripts import TRANSCRIPT_TIMEOUT_SECONDS
from tests.api.client import BASE_URL, http_client
from tests.api.youtube.fakes import (
    EPOCH,
    NOW,
    STAMP,
    VIDEO,
    FakeVideoCollector,
    FakeYouTubeStore,
    group,
    item,
    observation,
    playlist,
    playlist_item,
    saved_transcript,
)

ROUTE = "/api/reader/youtube"
HOME = "https://www.youtube.com/"
HYPE = "https://www.youtube.com/feed/hype"
EARLIER = "2026-10-23T09:00:00+00:00"
ANCHOR = "2026-10-23T12:00:00+00:00"
LATER = "2026-10-24T12:00:00+00:00"
METADATA = f"{ROUTE}/metadata/{VIDEO}"
TRANSCRIPT = f"{ROUTE}/transcript/{VIDEO}"
SAME_ORIGIN = {"origin": BASE_URL}
SHORT_WAITS = CollectionLimits(
    metadata_wait=0.01, transcript_lock_wait=0.01, transcript_lookup=0.01,
)


def wait_briefly(app: FastAPI) -> None:
    app.dependency_overrides[collection_limits] = lambda: SHORT_WAITS


def query(route: str = ROUTE, **fields: str) -> str:
    return f"{route}?{urlencode(fields)}"


def error(
    response: httpx.Response,
) -> tuple[int, str, str]:
    body = response.json()

    return response.status_code, body["code"], body["error"]


async def test_items_come_back_with_their_total(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.listings[""] = ItemListing(items=(item(),), total=7, after=None)

    response = await client.get(ROUTE)

    assert response.status_code == 200
    assert response.json() == {
        "items": [item()], "next_cursor": None, "total": 7,
    }
    assert response.headers["cache-control"] == "no-store"


async def test_next_cursor_continues_after_the_last_item(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.listings[""] = ItemListing(
        items=(item(),), total=2,
        after=ItemBoundary(STAMP, f"video:{VIDEO}", 4, 9),
    )
    filters = YouTubeFilters(feed=HYPE, limit=1)

    body = (await client.get(query(feed=HYPE, limit="1"))).json()

    assert decode_youtube_cursor(
        filters._replace(cursor=body["next_cursor"]),
    ) == (STAMP, f"video:{VIDEO}", 4, 9)


async def test_recommendations_keep_the_first_pages_capture(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    # The fake answers only for the moment each request froze on, so a later
    # page that read the newest capture again would fail here.
    store.recommendations_at = ANCHOR
    store.listings[ANCHOR] = ItemListing(
        items=(item(),), total=3,
        after=ItemBoundary(EARLIER, f"video:{VIDEO}", 1),
    )
    filters = YouTubeFilters(feed=HOME, limit=1)

    first = (await client.get(query(feed=HOME, limit="1"))).json()
    cursor = first["next_cursor"]

    assert youtube_cursor_snapshot(filters._replace(cursor=cursor)) == ANCHOR

    store.recommendations_at = LATER
    second = (await client.get(query(feed=HOME, limit="1", cursor=cursor)))

    assert second.status_code == 200
    assert youtube_cursor_snapshot(
        filters._replace(cursor=second.json()["next_cursor"]),
    ) == ANCHOR


@pytest.mark.parametrize("target", [
    query(feed="linked", sort="most-linked"),
    query(id="video:x"),
    query(kind=""),
    query(channel_scope=""),
    query(limit="500"),
    # int() takes spaces, and always has.
    query(limit=" 5 "),
])
async def test_filters_that_pass(client: AsyncClient, target: str) -> None:
    assert (await client.get(target)).status_code == 200


@pytest.mark.parametrize("target", [
    query(kind="live"),
    query(limit="0"),
    query(limit="501"),
    query(limit="no"),
    query(limit="5.0"),
    query(limit=""),
    query(page="2"),
    query(sort="invalid"),
    query(sort=""),
    query(channel_scope="all-old-feeds"),
    f"{ROUTE}?kind=video&kind=post",
    f"{ROUTE}?limit=1&limit=2",
    f"{ROUTE}?q=a&q=b",
])
async def test_filter_shapes_fail_naming_their_field(
    client: AsyncClient, target: str,
) -> None:
    response = await client.get(target)

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"
    assert response.json()["fields"]


@pytest.mark.parametrize(("target", "message"), [
    (query(q="a" * 2001), "YouTube filters are limited to 2000 characters"),
    (query(sort="most-linked"), "Invalid sort for this YouTube feed"),
    (query(feed=HYPE, sort="oldest"), "Invalid sort for this YouTube feed"),
    (query(cursor="not base64"), "Invalid cursor for these filters"),
])
async def test_filter_rules_keep_their_wording(
    client: AsyncClient, target: str, message: str,
) -> None:
    assert error(await client.get(target)) == (400, "invalid_input", message)


async def test_a_cursor_from_other_filters_is_rejected(
    client: AsyncClient,
) -> None:
    cursor = encode_youtube_cursor(
        YouTubeFilters(feed=HYPE), STAMP, f"video:{VIDEO}",
    )

    response = await client.get(query(feed="linked", cursor=cursor))

    assert error(response) == (
        400, "invalid_input", "Invalid cursor for these filters",
    )


async def test_a_failing_database_answers_503(
    app: FastAPI, store: FakeYouTubeStore,
) -> None:
    store.failing = True

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get(ROUTE)

    assert (response.status_code, response.json()["code"]) == (
        503, "unavailable",
    )


async def test_sources_list_feeds_and_channels(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.overview_payload = {
        "feeds": [{
            "id": HYPE, "url": HYPE, "name": "Hype · Top 100", "kind": "hype",
            "region": "GB", "tab": "", "count": 3,
        }],
        "channels": [{
            "id": "UC1", "name": "Channel", "url": "", "count": 9,
            "avatar_url": None, "reported_video_count": None,
            "reported_video_count_text": None,
        }],
        "total": 9,
        "linked_count": 2,
    }

    response = await client.get(f"{ROUTE}/sources")

    assert response.status_code == 200
    assert response.json() == store.overview_payload


async def test_channels_group_their_saved_examples(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.recommendations_at = ANCHOR
    store.channel_listings[ANCHOR] = ChannelListing(
        channels=(group(),), after=None, total=1, unattributed_items=4,
    )

    response = await client.get(query(f"{ROUTE}/channels", feed=HOME))

    assert response.status_code == 200
    assert response.json() == {
        "channels": [group()], "next_cursor": None, "total": 1,
        "unattributed_items": 4,
    }


async def test_other_feeds_group_channels_as_of_now(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.channel_listings[NOW.isoformat()] = ChannelListing(
        channels=(group(),), after=ChannelBoundary(8, "UC1"), total=2,
        unattributed_items=0,
    )
    filters = YouTubeFilters(feed=HYPE, limit=1)

    body = (await client.get(
        query(f"{ROUTE}/channels", feed=HYPE, limit="1"),
    )).json()

    assert parse_channel_cursor(filters, body["next_cursor"]) == (
        NOW.isoformat(), 8, "UC1",
    )


async def test_a_channel_cursor_keeps_its_snapshot(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    filters = YouTubeFilters(feed=HOME, limit=1)
    cursor = channel_cursor(filters, ANCHOR, 8, "UC1")
    store.recommendations_at = LATER
    store.channel_listings[ANCHOR] = ChannelListing(
        channels=(group("UC2", count=8),), after=None, total=2,
        unattributed_items=0,
    )

    response = await client.get(
        query(f"{ROUTE}/channels", feed=HOME, limit="1", cursor=cursor),
    )

    assert response.status_code == 200
    assert response.json()["channels"][0]["id"] == "UC2"


@pytest.mark.parametrize("fields", [
    "limit=51", "limit=0", "limit=5.0", "cursor=bad", "cursor=a&cursor=b",
    "id=video:x", "channel_scope=unknown", "unexpected=true", "sort=oldest",
    "limit=1&limit=2",
])
async def test_channel_pages_reject_invalid_requests(
    client: AsyncClient, fields: str,
) -> None:
    response = await client.get(f"{ROUTE}/channels?{fields}")

    assert response.status_code == 400


async def test_a_channel_rule_keeps_its_wording(client: AsyncClient) -> None:
    response = await client.get(f"{ROUTE}/channels?id=video:x")

    assert error(response) == (
        400, "invalid_input", "Channel pages allow up to 50 channels",
    )


async def test_the_library_reports_its_count_and_next_cursor(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.playlist_times[""] = STAMP
    shown = playlist(status="partial", complete=False)
    store.libraries[STAMP] = PlaylistListing(
        playlists=(shown,), total=2,
        after=("0000000001saved music", "PLexample"),
    )

    response = await client.get(query(f"{ROUTE}/playlists", limit="1"))
    body = response.json()

    assert response.status_code == 200
    assert body["total"] == 2
    assert body["playlists"] == [shown]
    assert parse_cursor(body["next_cursor"], "", "") == (
        STAMP, ("0000000001saved music", "PLexample"),
    )


async def test_an_empty_library_has_a_valid_cutoff(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.libraries[EPOCH] = PlaylistListing(
        playlists=(), total=0, after=None,
    )

    response = await client.get(f"{ROUTE}/playlists")

    assert (response.status_code, response.json()) == (200, {
        "playlists": [], "total": 0, "next_cursor": None,
    })


async def test_a_library_cursor_binds_its_sort_and_search(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    cursor = playlist_cursor("", "music", STAMP, ("music", "PLid"), "az")
    store.libraries[STAMP] = PlaylistListing(
        playlists=(playlist(),), total=2, after=None,
    )

    fine = await client.get(query(
        f"{ROUTE}/playlists", q="music", sort="az", cursor=cursor,
    ))
    other_sort = await client.get(query(
        f"{ROUTE}/playlists", q="music", cursor=cursor,
    ))
    other_search = await client.get(query(
        f"{ROUTE}/playlists", sort="az", q="else", cursor=cursor,
    ))

    assert fine.status_code == 200
    assert error(other_sort) == (
        400, "invalid_input", "Invalid cursor for this playlist view",
    )
    assert other_search.status_code == 400


@pytest.mark.parametrize("fields", [
    "limit=0", "limit=101", "limit=x", "limit=5.0", "cursor=bad", "q=a&q=b",
    "sort=newest", "limit=1&limit=2", "unknown=x",
])
async def test_library_pages_reject_invalid_requests(
    client: AsyncClient, fields: str,
) -> None:
    response = await client.get(f"{ROUTE}/playlists?{fields}")

    assert response.status_code == 400


async def test_a_playlist_lists_its_entries(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.playlist_times["PLexample"] = STAMP
    shown = playlist(collected_count=100)
    store.entries["PLexample", STAMP] = PlaylistEntries(
        playlist=shown, items=(
            playlist_item(34), playlist_item(
                89, available=False, url="", video_url=None,
            ),
        ), total=2, after=89,
    )

    response = await client.get(f"{ROUTE}/playlists/PLexample")
    body = response.json()

    assert response.status_code == 200
    assert body["playlist"] == shown
    assert body["total"] == 2
    assert [item["playlist_position"] for item in body["items"]] == [34, 89]
    assert body["items"][1]["available"] is False
    assert parse_cursor(body["next_cursor"], "PLexample", "") == (STAMP, 89)


async def test_a_capture_published_between_pages_cant_change_the_rest(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.entries["PLexample", ANCHOR] = PlaylistEntries(
        playlist=playlist(snapshot_id="old-order"),
        items=(playlist_item(2),), total=2, after=None,
    )
    store.playlist_times["PLexample"] = LATER
    cursor = playlist_cursor("PLexample", "", ANCHOR, 1)

    response = await client.get(
        query(f"{ROUTE}/playlists/PLexample", cursor=cursor),
    )

    assert response.status_code == 200
    assert response.json()["playlist"]["snapshot_id"] == "old-order"


async def test_a_missing_playlist_answers_404(client: AsyncClient) -> None:
    response = await client.get(f"{ROUTE}/playlists/missing")

    assert error(response) == (404, "not_found", "Playlist not found")


@pytest.mark.parametrize("target", [
    f"{ROUTE}/playlists/PL.bad",
    f"{ROUTE}/playlists/{'a' * 201}",
    f"{ROUTE}/playlists/PLexample?sort=az",
    f"{ROUTE}/playlists/PLexample?limit=101",
    f"{ROUTE}/playlists/PLexample?cursor=bad",
    f"{ROUTE}/playlists/PLexample?q=a&q=b",
])
async def test_playlist_pages_reject_invalid_requests(
    client: AsyncClient, target: str,
) -> None:
    assert (await client.get(target)).status_code == 400


async def test_a_detail_cursor_for_another_playlist_is_rejected(
    client: AsyncClient,
) -> None:
    cursor = playlist_cursor("PLother", "", STAMP, 3)

    response = await client.get(
        query(f"{ROUTE}/playlists/PLexample", cursor=cursor),
    )

    assert error(response) == (
        400, "invalid_input", "Invalid cursor for this playlist view",
    )


@pytest.mark.parametrize(("method", "path"), [
    ("GET", f"{ROUTE}/playlists/a/b"),
    ("GET", f"{ROUTE}/playlists/a%2Fb"),
    ("GET", f"{ROUTE}/transcript/aaaaa/aaaaa"),
    ("GET", f"{ROUTE}/transcript/aaaaa%2Faaaaa"),
    ("POST", f"{ROUTE}/transcript/aaaaa/aaaaa"),
    ("POST", f"{ROUTE}/metadata/aaaaa/aaaaa"),
    ("POST", f"{ROUTE}/metadata/aaaaa%2Faaaaa"),
])
async def test_path_parameters_cant_contain_a_slash(
    client: AsyncClient, method: str, path: str,
) -> None:
    response = await client.request(method, path, json={})

    assert response.status_code == 404


async def collect(
    client: AsyncClient, path: str, **headers: str,
) -> httpx.Response:
    return await client.post(path, json={}, headers=SAME_ORIGIN | headers)


@pytest.mark.parametrize("path", [METADATA, TRANSCRIPT])
@pytest.mark.parametrize("headers", [
    {"origin": "https://evil.example"},
    {"origin": "https://dank.test"},
    {"sec-fetch-site": "cross-site"},
])
async def test_collection_rejects_other_origins(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    path: str,
    headers: dict[str, str],
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    collector.transcripts[VIDEO] = saved_transcript()

    response = await client.post(path, json={}, headers=headers)

    assert (response.status_code, response.json()["code"]) == (
        403, "forbidden",
    )
    assert collector.saved == []


@pytest.mark.parametrize("path", [METADATA, TRANSCRIPT])
@pytest.mark.parametrize("content_type", [
    "application/x-www-form-urlencoded", "text/plain",
])
async def test_collection_needs_json(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    path: str,
    content_type: str,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    collector.transcripts[VIDEO] = saved_transcript()

    response = await client.post(
        path, content="{}",
        headers={"content-type": content_type, **SAME_ORIGIN},
    )

    assert (response.status_code, response.json()["code"]) == (
        415, "unsupported_media_type",
    )
    assert collector.saved == []


@pytest.mark.parametrize("path", [METADATA, TRANSCRIPT])
@pytest.mark.parametrize("headers", [
    {},
    SAME_ORIGIN,
    {"sec-fetch-site": "same-origin", **SAME_ORIGIN},
    # What the local HTTPS proxy sends once it has checked the real
    # https:// origin: an http:// Origin, plus its forwarded scheme.
    {**SAME_ORIGIN, "x-forwarded-proto": "https"},
])
async def test_collection_accepts_the_same_origin_and_the_proxy(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    path: str,
    headers: dict[str, str],
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    collector.transcripts[VIDEO] = saved_transcript()

    response = await client.post(path, json={}, headers=headers)

    assert response.status_code == 200


@pytest.mark.parametrize("path", [METADATA, TRANSCRIPT])
@pytest.mark.parametrize("body", [
    b"", b"[]", b'{"force": true}', b"not json",
])
async def test_collection_declares_an_empty_object_as_its_body(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    path: str,
    body: bytes,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    collector.transcripts[VIDEO] = saved_transcript()

    response = await client.post(
        path, content=body,
        headers={"content-type": "application/json", **SAME_ORIGIN},
    )

    assert (response.status_code, response.json()["code"]) == (
        400, "invalid_request",
    )
    assert collector.saved == []


@pytest.mark.parametrize("route", ["metadata", "transcript"])
@pytest.mark.parametrize("video_id", ["invalid", VIDEO + "x", "a.b-c_d.e.f"])
async def test_an_invalid_id_cant_start_collection(
    client: AsyncClient,
    collector: FakeVideoCollector,
    route: str,
    video_id: str,
) -> None:
    response = await collect(client, f"{ROUTE}/{route}/{video_id}")

    assert response.status_code == 400
    assert response.json()["fields"][0]["path"] == "path.video_id"
    assert collector.saved == []


@pytest.mark.parametrize("path", [METADATA, TRANSCRIPT])
async def test_an_unknown_video_cant_start_collection(
    client: AsyncClient, collector: FakeVideoCollector, path: str,
) -> None:
    collector.observations[VIDEO] = observation()
    collector.transcripts[VIDEO] = saved_transcript()

    response = await collect(client, path)

    assert error(response) == (404, "not_found", "Video not found")
    assert collector.saved == []


async def test_a_capture_reports_complete_state(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    saved = observation()
    collector.observations[VIDEO] = saved

    response = await collect(client, METADATA)

    assert (response.status_code, response.json()) == (200, {
        "video_id": VIDEO,
        "status": "captured",
        "provider": "youtube_oembed",
        "cached": False,
        "observed_at": saved.observed_at,
        "retry_after": saved.retry_after,
        "available": True,
        "captured_at": saved.observed_at,
    })


async def test_a_failed_refresh_keeps_the_available_preview(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    store.captured_at[VIDEO] = ANCHOR
    failed = observation(status="failed")
    collector.observations[VIDEO] = failed

    body = (await collect(client, METADATA)).json()

    assert body["status"] == "failed"
    assert body["available"] is True
    assert body["captured_at"] == ANCHOR
    assert body["observed_at"] == failed.observed_at


async def test_a_failure_with_no_capture_has_no_preview(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation(status="unavailable")

    body = (await collect(client, METADATA)).json()

    assert body["status"] == "unavailable"
    assert body["available"] is False
    assert body["captured_at"] == ""


async def test_simultaneous_metadata_requests_share_the_saved_result(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()

    responses = await asyncio.gather(
        collect(client, METADATA), collect(client, METADATA),
    )

    assert [response.status_code for response in responses] == [200, 200]
    assert {response.json()["cached"] for response in responses} == {
        True, False,
    }
    assert collector.saved == [VIDEO]


async def test_metadata_is_busy_while_the_lock_stays_taken(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    wait_briefly(app)

    async with locks.metadata:
        response = await collect(client, METADATA)

    assert error(response) == (
        503, "busy", "Metadata collection is busy; try again shortly",
    )
    assert response.headers["cache-control"] == "no-store"
    assert collector.saved == []


async def test_metadata_wait_also_covers_the_collection(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> None:
    store.known.add(VIDEO)
    collector.observations[VIDEO] = observation()
    collector.holding = True
    wait_briefly(app)

    response = await collect(client, METADATA)

    assert error(response) == (
        503, "busy", "Metadata collection is busy; try again shortly",
    )
    # The timeout gave the lock back.
    assert not locks.metadata.locked()


async def test_an_unknown_video_isnt_kept_waiting_by_a_busy_lock(
    app: FastAPI, client: AsyncClient, locks: CollectionLocks,
) -> None:
    wait_briefly(app)

    async with locks.metadata, locks.transcript:
        metadata = await collect(client, METADATA)
        transcript = await collect(client, TRANSCRIPT)

    assert metadata.status_code == 404
    assert transcript.status_code == 404


async def test_transcript_is_busy_while_the_lock_stays_taken(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> None:
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()
    wait_briefly(app)

    async with locks.transcript:
        response = await collect(client, TRANSCRIPT)

    assert error(response) == (
        503, "busy", "Transcript collection is busy; try again shortly",
    )
    assert response.headers["cache-control"] == "no-store"
    assert collector.saved == []


async def test_transcript_waits_for_the_lock_within_its_limit(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> None:
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()
    await locks.transcript.acquire()
    waiting = asyncio.create_task(collect(client, TRANSCRIPT))
    await asyncio.sleep(0.01)
    locks.transcript.release()

    assert (await waiting).status_code == 200
    assert collector.saved == [VIDEO]


async def test_a_slow_transcript_lookup_times_out(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> None:
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()
    collector.holding = True
    wait_briefly(app)

    response = await collect(client, TRANSCRIPT)

    assert error(response) == (
        503, "busy", "Transcript lookup timed out; try again shortly",
    )
    # The timeout gave the lock back.
    assert not locks.transcript.locked()


async def test_a_lookup_in_flight_keeps_others_waiting_only_for_the_lock(
    app: FastAPI,
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    # Longer than the lock wait, so the second request gives up first.
    app.dependency_overrides[collection_limits] = lambda: CollectionLimits(
        metadata_wait=5, transcript_lock_wait=0.01, transcript_lookup=5,
    )
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()
    collector.holding = True
    first = asyncio.create_task(collect(client, TRANSCRIPT))
    await collector.started.wait()

    second = await collect(client, TRANSCRIPT)

    assert error(second) == (
        503, "busy", "Transcript collection is busy; try again shortly",
    )

    collector.release.set()

    assert (await first).status_code == 200


async def test_simultaneous_transcript_requests_share_the_saved_result(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()

    responses = await asyncio.gather(
        collect(client, TRANSCRIPT), collect(client, TRANSCRIPT),
    )

    assert [response.status_code for response in responses] == [200, 200]
    assert collector.saved == [VIDEO]


def test_collection_waits_stay_bounded() -> None:
    assert collection_limits() == CollectionLimits(
        metadata_wait=15,
        transcript_lock_wait=1,
        transcript_lookup=TRANSCRIPT_TIMEOUT_SECONDS + 5,
    )


async def test_a_saved_transcript_comes_back_without_collecting(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    store.transcripts[VIDEO] = saved_transcript()

    response = await client.get(TRANSCRIPT)

    assert (response.status_code, response.json()) == (200, saved_transcript())
    assert collector.saved == []


async def test_a_missing_transcript_is_not_collected_by_reading_it(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)

    response = await client.get(TRANSCRIPT)

    assert response.status_code == 200
    assert response.json()["status"] == "not_collected"
    assert response.json()["segments"] == []
    assert collector.saved == []


async def test_reading_a_transcript_needs_no_same_origin_check(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.known.add(VIDEO)

    response = await client.get(
        TRANSCRIPT, headers={"origin": "https://evil.example"},
    )

    assert response.status_code == 200


async def test_reading_an_unknown_or_invalid_transcript_fails(
    client: AsyncClient,
) -> None:
    unknown = await client.get(TRANSCRIPT)
    invalid = await client.get(f"{ROUTE}/transcript/bad")

    assert error(unknown) == (404, "not_found", "Video not found")
    assert invalid.status_code == 400
    assert invalid.json()["fields"][0]["path"] == "path.video_id"


async def test_a_collected_transcript_comes_back_as_the_reader_expects(
    client: AsyncClient,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
) -> None:
    store.known.add(VIDEO)
    collector.transcripts[VIDEO] = saved_transcript()

    response = await collect(client, TRANSCRIPT)

    assert (response.status_code, response.json()) == (200, saved_transcript())
    assert collector.saved == [VIDEO]


async def test_head_answers_like_get(
    client: AsyncClient, store: FakeYouTubeStore,
) -> None:
    store.known.add(VIDEO)

    response = await client.head(TRANSCRIPT)

    assert response.status_code == 200
    assert response.content == b""
