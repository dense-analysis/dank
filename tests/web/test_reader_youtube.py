from __future__ import annotations

import base64
import datetime as dt
import json
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from multidict import MultiDict

from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_youtube import (
    _local_media,  # pyright: ignore[reportPrivateUsage]
    _thumbnail_history,  # pyright: ignore[reportPrivateUsage]
    ensure_youtube_storage,
    youtube_items,
)
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
    decode_youtube_cursor,
    encode_youtube_cursor,
    parse_youtube_filters,
)


def test_queries_filter_before_deduplicating_feed_appearances() -> None:
    filters = parse_youtube_filters(MultiDict({
        "feed": "https://www.youtube.com/feed/hype", "kind": "video",
        "q": "test's", "channel": "UCexample", "limit": "10",
    }))
    query, params = build_youtube_query(filters)
    assert "WHERE feed_url = %(feed)s AND (feed_kind = 'channel'" in query
    assert "position ASC" in query
    assert "kind = %(kind)s" in query
    assert "test's" not in query
    assert params["q"] == "test's"
    assert params["limit"] == 11


def test_linked_queries_use_index_and_do_not_rescan_raw_payloads() -> None:
    query, _ = build_youtube_query(YouTubeFilters(feed="linked"))
    assert "FROM youtube_reference_evidence FINAL GROUP BY video_id" in query
    assert "raw_posts" not in query
    assert "FROM posts" not in query


def test_cursors_bind_filters_and_keep_stable_item_tie_breaker() -> None:
    filters = YouTubeFilters(feed="linked", kind="video")
    timestamp = "2026-10-17T10:00:00+00:00"
    cursor = encode_youtube_cursor(filters, timestamp, "video:AbCdEfGhIjK")
    paged = filters._replace(cursor=cursor)
    assert decode_youtube_cursor(paged) == (timestamp, "video:AbCdEfGhIjK", 0)
    query, _ = build_youtube_query(paged)
    assert "(observed_at, -toInt64(position), item_id) <" in query
    count_query, _ = build_youtube_query(paged, count=True)
    assert "cursor_time" not in count_query

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_youtube_cursor(paged._replace(feed="different"))


@pytest.mark.parametrize("query", [
    {"kind": "live"}, {"limit": "0"}, {"limit": "101"}, {"limit": "no"},
    {"page": "2"}, {"q": "a" * 2001}, {"cursor": "not base64"},
])
def test_invalid_filters(query: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_youtube_filters(MultiDict(query))


def test_duplicate_filters_rejected() -> None:
    with pytest.raises(ValueError):
        parse_youtube_filters(MultiDict([("kind", "video"), ("kind", "post")]))


async def test_item_keeps_provenance_and_multiple_source_references():
    client = AsyncMock()
    timestamp = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    observations: list[dict[str, Any]] = [
        {"item_id": "video:AbCdEfGhIjK", "feed_name": "Top 100 · UK",
         "feed_url": "https://www.youtube.com/feed/hype", "region": "GB",
         "playlist_id": "OLJexample", "shelf": "Top 100", "rank": 4,
         "run_id": run, "observed_at": timestamp}
        for run in ("first", "second")
    ]
    references: list[dict[str, Any]] = [
        {"video_id": "AbCdEfGhIjK", "domain": domain, "post_id": "123",
         "url": f"https://{domain}/post/123", "title": "Shared video",
         "source": source, "reference_count": 2, "processed": source == "x"}
        for domain, source in (("x.com", "x"), ("reddit.com", "reddit"))
    ]
    client.fetch_json.side_effect = [
        QueryResult(observations), QueryResult(references), QueryResult([]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": "video:AbCdEfGhIjK", "kind": "short",
        "observed_at": timestamp, "payload": json.dumps({
            "kind": "short", "title": "Captured title",
            "channel_name": "Channel", "thumbnail_url": "javascript:bad",
        }),
    }], YouTubeFilters())
    assert len(items) == 1
    item = items[0]
    assert item["kind"] == "short"
    assert item["thumbnail_url"] == (
        "https://i.ytimg.com/vi/AbCdEfGhIjK/hqdefault.jpg"
    )
    assert item["detail_capture"] == {"status": "disabled"}
    assert item["reference_count"] == 2
    assert len(item["discoveries"]) == 2
    assert item["discoveries"][0]["region"] == "GB"
    assert item["references"][0]["reader_url"] == (
        "/reader/?article_source=x.com&article=123"
    )
    assert item["references"][1]["reader_url"] == ""
    assert item["references"][1]["url"] == "https://reddit.com/post/123"


async def test_existing_link_index_is_not_backfilled_on_every_start() -> None:
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{"version": 3}])
    await ensure_youtube_storage(cast(ClickHouseClient, client))
    assert all(
        not call.args[0].startswith("INSERT INTO youtube_links")
        for call in client.execute.call_args_list
    )
    client.insert_rows.assert_not_called()


async def test_missing_video_title_never_uses_a_source_headline():
    client = AsyncMock()
    timestamp = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([{
            "video_id": "NnV_cWeoo5Q", "domain": "reddit.com",
            "post_id": "t3_1x7ijzc", "title": "Unrelated game headline",
            "url": "https://www.reddit.com/comments/1x7ijzc/",
            "kind": "comment", "reference_id": "t1_ph4vac2",
            "reference_url": "https://www.reddit.com/comments/1x7ijzc/comment/ph4vac2/",
            "matched_url": "https://youtu.be/NnV_cWeoo5Q?t=1468",
            "excerpt": "Seen it happen in real time", "author": "James20k",
            "latest_at": timestamp, "reference_count": 1,
        }]), QueryResult([]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": "video:NnV_cWeoo5Q", "kind": "video", "payload": "{}",
        "observed_at": timestamp,
    }], YouTubeFilters(feed="linked"))
    assert items[0]["title"] == "YouTube video · NnV_cWeoo5Q"
    mention = items[0]["references"][0]
    assert mention["kind"] == "comment"
    assert mention["author"] == "James20k"
    assert mention["excerpt"] == "Seen it happen in real time"
    assert mention["matched_url"].endswith("?t=1468")
    assert mention["observed_at"] == timestamp.isoformat()


def test_schema_copies_and_package_data_include_youtube() -> None:
    root = pathlib.Path(__file__).parents[2]
    migration = (root / "src/dank/storage/youtube.sql").read_text()
    schema = (root / "schema.sql").read_text()
    assert migration.replace(
        "EXISTS youtube_", "EXISTS dank.youtube_",
    ).replace(
        "ALTER TABLE youtube_", "ALTER TABLE dank.youtube_",
    ).replace("TO youtube_links", "TO dank.youtube_links").replace(
        "FROM posts", "FROM dank.posts",
    ).replace("FROM raw_posts", "FROM dank.raw_posts") in schema
    assert '"youtube.sql"' in (root / "pyproject.toml").read_text()


def test_rank_is_part_of_cursor_and_snapshots_keep_channels_incremental():
    filters = YouTubeFilters(feed="https://www.youtube.com/feed/hype")
    cursor = encode_youtube_cursor(
        filters, "2026-10-17T10:00:00+00:00", "video:AbCdEfGhIjK", 7,
    )
    query, params = build_youtube_query(filters._replace(cursor=cursor))
    assert params["cursor_position"] == 7
    assert "position ASC" in query
    assert "JSONExtractUInt(d.payload, 'discovery', 'position')" in query
    assert "feed_kind = 'channel' OR run_id =" in query
    assert "argMax(run_id, observed_at)" in query


def test_linked_only_item_can_be_deep_linked_without_feed():
    query, params = build_youtube_query(
        YouTubeFilters(item_id="video:AbCdEfGhIjK"),
    )
    assert "UNION ALL" in query
    assert "youtube_reference_evidence" in query
    assert params["id"] == "video:AbCdEfGhIjK"


async def test_local_media_requires_an_existing_file_inside_assets(
    tmp_path: pathlib.Path,
) -> None:
    client = AsyncMock()
    timestamp = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    assets = tmp_path / "assets"
    assets.mkdir()
    video = assets / "video.mp4"
    video.write_bytes(b"video")
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"not in assets")
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([]), QueryResult([]), QueryResult([
            {"post_id": "video:AbCdEfGhIjK", "url": "https://example/video",
             "asset_type": "video", "local_path": str(video),
             "scraped_at": timestamp},
            {"post_id": "video:AbCdEfGhIjK", "url": "https://example/thumb",
             "asset_type": "image", "local_path": str(outside),
             "scraped_at": timestamp},
        ]), QueryResult([]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": "video:AbCdEfGhIjK", "kind": "video",
        "observed_at": timestamp, "payload": "{}",
    }], YouTubeFilters(), assets)
    assert items[0]["video_url"] == "/assets/video.mp4"
    assert items[0]["thumbnail_url"] == (
        "https://i.ytimg.com/vi/AbCdEfGhIjK/hqdefault.jpg"
    )


async def test_extensionless_cached_thumbnail_uses_type_and_keeps_containment(
    tmp_path: pathlib.Path,
) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
        "/x8AAwMCAO+a9O8AAAAASUVORK5CYII=",
    )
    inside = assets / "opaque-cache-hash"
    outside = tmp_path / "outside-cache-hash"
    inside.write_bytes(image)
    outside.write_bytes(image)
    timestamp = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"post_id": post_id, "url": "https://yt3.ggpht.com/image=s800",
         "asset_type": "image", "local_path": str(path),
         "scraped_at": timestamp}
        for post_id, path in (
            ("post:inside", inside), ("post:outside", outside),
        )
    ])
    media = await _local_media(
        cast(ClickHouseClient, client),
        ["post:inside", "post:outside"], assets,
    )
    assert media == {
        "post:inside": {"thumbnail_url": "/assets/opaque-cache-hash"},
    }


def test_recommendation_pages_and_total_keep_original_snapshot():
    filters = YouTubeFilters(feed="https://www.youtube.com/feed/hype", limit=1)
    captured = "2026-10-17T10:00:00+00:00"
    cursor = encode_youtube_cursor(filters, captured, "video:AbCdEfGhIjK", 1)
    paged = filters._replace(cursor=cursor)

    for count in (False, True):
        query, params = build_youtube_query(paged, count=count)
        assert (
            "WHERE feed_url = %(feed)s AND observed_at <= "
            "parseDateTime64BestEffort(%(snapshot_time)s, 3)"
        ) in query
        assert params["snapshot_time"] == captured
        assert "feed_kind = 'channel' OR" in query

    first_query, first_params = build_youtube_query(filters)
    assert "snapshot_time" not in first_query
    assert "snapshot_time" not in first_params


async def test_changed_titles_keep_one_video_and_preserve_observation_dates():
    client = AsyncMock()
    first = dt.datetime(2026, 10, 15, tzinfo=dt.UTC)
    latest = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    item_id = "video:AbCdEfGhIjK"
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([]), QueryResult([
            {"item_id": item_id, "title": "A revised title",
             "first_seen": latest, "last_seen": latest},
            {"item_id": item_id, "title": "The original title",
             "first_seen": first, "last_seen": first + dt.timedelta(days=1)},
        ]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": item_id, "kind": "video", "observed_at": latest,
        "payload": json.dumps({"title": "A revised title"}),
    }], YouTubeFilters())
    assert len(items) == 1
    assert items[0]["id"] == item_id
    assert items[0]["title"] == "A revised title"
    assert items[0]["title_history"] == [
        {"title": "A revised title", "first_seen": latest.isoformat(),
         "last_seen": latest.isoformat()},
        {"title": "The original title", "first_seen": first.isoformat(),
         "last_seen": (first + dt.timedelta(days=1)).isoformat()},
    ]
    query, params = client.fetch_json.call_args.args
    assert "GROUP BY item_id, title" in query
    assert "min(observed_at) AS first_seen" in query
    assert "max(observed_at) AS last_seen" in query
    assert params == {"ids": [item_id], "limit": 20}

    listing, _ = build_youtube_query(YouTubeFilters())
    assert "GROUP BY item_id" in listing
    assert "GROUP BY item_id, title" not in listing
    assert "argMax(d.payload, d.observed_at)" in listing


async def test_title_history_does_not_replace_selected_feed_snapshot_title():
    client = AsyncMock()
    first = dt.datetime(2026, 10, 15, tzinfo=dt.UTC)
    latest = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    item_id = "video:AbCdEfGhIjK"
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([]), QueryResult([
            {"item_id": item_id, "title": "A later observation",
             "first_seen": latest, "last_seen": latest},
            {"item_id": item_id, "title": "The snapshot title",
             "first_seen": first, "last_seen": first},
        ]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": item_id, "kind": "video", "observed_at": first,
        "payload": json.dumps({"title": "The snapshot title"}),
    }], YouTubeFilters(feed="https://www.youtube.com/feed/hype"))
    assert items[0]["title"] == "The snapshot title"
    assert items[0]["title_history"][0]["title"] == "A later observation"


async def test_thumbnail_history_groups_renditions_and_checks_local_files(
    tmp_path: pathlib.Path,
) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    first = dt.datetime(2026, 10, 15, tzinfo=dt.UTC)
    latest = first + dt.timedelta(days=2)
    rows: list[dict[str, Any]] = []

    for name, fingerprint, color, width, height, captured in (
        ("current.jpg", "0" * 64, "100,100,100", 1280, 720, latest),
        ("recompressed.jpg", "0" * 63 + "1", "101,100,100", 480, 270,
         first + dt.timedelta(days=1)),
        ("old.jpg", "f" * 64, "50,150,200", 1280, 720, first),
    ):
        path = assets / name
        path.write_bytes(b"saved image")
        rows.append({
            "post_id": "video:AbCdEfGhIjK", "local_path": str(path),
            "content_hash": name, "visual_hash": fingerprint,
            "visual_color": color, "width": width, "height": height,
            "first_seen": captured, "last_seen": captured,
        })

    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"outside")
    rows[:0] = [
        {**rows[0], "post_id": "video:Outside1234",
         "local_path": str(outside)},
        {**rows[0], "local_path": str(assets / "missing.jpg")},
    ]
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(rows)
    history = await _thumbnail_history(
        cast(ClickHouseClient, client),
        ["video:AbCdEfGhIjK", "video:Outside1234"], assets,
    )
    assert history == {"video:AbCdEfGhIjK": [
        {"url": "/assets/current.jpg", "title_pairs": [],
         "first_seen": (first + dt.timedelta(days=1)).isoformat(),
         "last_seen": latest.isoformat()},
        {"url": "/assets/old.jpg", "title_pairs": [],
         "first_seen": first.isoformat(),
         "last_seen": first.isoformat()},
    ]}
    query, params = client.fetch_json.call_args_list[0].args
    assert "GROUP BY d.post_id, d.content_hash" in query
    assert "min(d.observed_at) AS first_seen" in query
    assert "WHERE d.post_id IN %(ids)s AND d.local_path != ''" in query
    assert params["limit"] == 64


async def test_thumbnail_history_is_bounded_after_visual_grouping(
    tmp_path: pathlib.Path,
) -> None:
    first = dt.datetime(2026, 10, 15, tzinfo=dt.UTC)
    rows: list[dict[str, Any]] = []

    for index in range(12):
        path = tmp_path / f"capture-{index}.jpg"
        path.write_bytes(b"saved image")
        rows.append({
            "post_id": "video:AbCdEfGhIjK", "local_path": str(path),
            "content_hash": str(index), "visual_hash": "0" * 64,
            "visual_color": f"{index * 10},100,100",
            "width": 1280, "height": 720,
            "first_seen": first, "last_seen": first,
        })

    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(rows)
    history = await _thumbnail_history(
        cast(ClickHouseClient, client), ["video:AbCdEfGhIjK"], tmp_path,
    )
    assert len(history["video:AbCdEfGhIjK"]) == 8
    assert history["video:AbCdEfGhIjK"][0]["url"] == "/assets/capture-0.jpg"


async def test_latest_successful_snapshot_overrides_remote_thumbnail(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "latest.jpg"
    path.write_bytes(b"saved image")
    timestamp = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([]), QueryResult([]), QueryResult([]),
        QueryResult([{
            "post_id": "video:AbCdEfGhIjK", "local_path": str(path),
            "content_hash": "different-image", "visual_hash": "0" * 64,
            "visual_color": "100,100,100", "width": 1280, "height": 720,
            "first_seen": timestamp, "last_seen": timestamp,
        }]), QueryResult([]),
    ]
    items = await youtube_items(cast(ClickHouseClient, client), [{
        "item_id": "video:AbCdEfGhIjK", "kind": "video",
        "observed_at": timestamp, "payload": json.dumps({
            "title": "Captured title",
            "thumbnail_url": "https://i.ytimg.com/vi/AbCdEfGhIjK/missing.jpg",
        }),
    }], YouTubeFilters(), tmp_path)
    assert items[0]["thumbnail_url"] == "/assets/latest.jpg"
    assert items[0]["thumbnail_variants"] == [{
        "url": "/assets/latest.jpg", "title_pairs": [],
        "first_seen": timestamp.isoformat(),
        "last_seen": timestamp.isoformat(),
    }]
