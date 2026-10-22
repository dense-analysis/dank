from __future__ import annotations

import datetime as dt
import json
import pathlib
from typing import Any, cast

import pytest
from multidict import MultiDict

from dank.config import SourceConfig
from dank.storage.clickhouse import QueryResult
from dank.web.app import PostRow
from dank.web.reader_api import (
    _post_payloads,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_links import HN_LINKED_POSTS_SQL, RESOLVED_HN_POSTS_SQL
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)
from dank.web.reader_youtube_feed import add_youtube_post_metadata


@pytest.mark.parametrize("kind,platforms", [
    ("stories", ()), ("all", ("stories",)),
])
def test_blogs_include_collected_publishers_and_hacker_news(
    kind: str, platforms: tuple[str, ...],
) -> None:
    filters = ReaderFilters(kind=kind, platforms=platforms)
    query, params = build_query(filters, (
        SourceConfig("followed.example", ()),
        SourceConfig("empty.example", ()),
        SourceConfig("news.ycombinator.com", ()),
        SourceConfig("youtube.com", ()),
        SourceConfig("reddit.com", ()),
        SourceConfig("twitter.com", ()),
        SourceConfig("x.com", ()),
    ))
    assert "source IN ('rss', 'linked_article', 'hacker_news')" in query
    assert "blog_domains" not in params
    assert RESOLVED_HN_POSTS_SQL in query

    empty, _ = build_query(ReaderFilters(kind=kind, platforms=platforms), ())
    assert "source IN ('rss', 'linked_article', 'hacker_news')" in empty


def test_platforms_are_unioned_then_intersected_with_other_filters() -> None:
    filters = parse_filters(MultiDict([
        ("kind", "all"), ("platform", "youtube"), ("platform", "reddit"),
        ("platform", "Reddit"), ("domain", "reddit.com"),
        ("tag", "followed"), ("q", "search term"), ("after", "2026-10-01"),
    ]))
    query, params = build_query(filters, (
        SourceConfig("reddit.com", (), tags=("followed",)),
    ))
    assert filters.platforms == ("reddit", "youtube")
    assert (
        "WHERE (domain = 'reddit.com' OR domain = 'youtube.com') "
        "AND domain IN %(domains)s AND domain IN %(tag_domains)s" in query
    )
    assert "created_at >= toDateTime64(%(after)s" in query
    assert params["term_0"] == "search"
    assert params["term_1"] == "term"


def test_hacker_news_platform_keeps_canonical_publishers_and_unresolved_posts(
) -> None:
    query, params = build_query(ReaderFilters(
        kind="all", platforms=("hacker_news", "stories"),
    ), (SourceConfig("followed.example", ()),))
    assert "domain = 'news.ycombinator.com' OR (domain, post_id) IN (" in query
    assert HN_LINKED_POSTS_SQL in query
    assert "SELECT DISTINCT target.domain, target.post_id" in query
    assert "(domain, post_id) NOT IN (" + RESOLVED_HN_POSTS_SQL in query
    assert "OR (source IN ('rss', 'linked_article', 'hacker_news')" in query
    assert "blog_domains" not in params
    assert "hn_filter" not in params

    for safeguard in (
        "origin.updated_at = l.observed_at",
        "origin.content_hash = l.content_hash",
        "u.observed_at <= target.updated_at",
        "l.kind = 'primary'", "l.domain = 'news.ycombinator.com'",
    ):
        assert safeguard in HN_LINKED_POSTS_SQL


def test_unfiltered_all_keeps_discovered_publishers() -> None:
    query, params = build_query(ReaderFilters(kind="all"), ())
    assert "blog_domains" not in params
    assert HN_LINKED_POSTS_SQL not in query
    assert RESOLVED_HN_POSTS_SQL in query


@pytest.mark.parametrize("query", [
    [("platform", "unknown")], [("platform", "")],
    [("kind", "stories"), ("platform", "reddit")],
    [("kind", "reddit"), ("platform", "reddit")],
    [("kind", "x"), ("platform", "youtube")],
    [("platform", "hacker_news"), ("hn_filter", "top10")],
    [("platform", "x"), ("account", "author")],
])
def test_invalid_platform_combinations_are_rejected(
    query: list[tuple[str, str]],
) -> None:
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


def test_platform_cursor_identity_is_normalized_and_bound() -> None:
    first = parse_filters(MultiDict([
        ("platform", "reddit"), ("platform", "youtube"),
    ]))
    reordered = parse_filters(MultiDict([
        ("platform", "youtube"), ("platform", "reddit"),
        ("platform", "reddit"),
    ]))
    stamp = dt.datetime(2026, 10, 22, 12, 34, 56, 789000, dt.UTC)
    token = encode_cursor(first, stamp, "reddit.com", "post")
    assert decode_cursor(reordered._replace(cursor=token)) == (
        stamp, "reddit.com", "post",
    )

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(first._replace(platforms=("reddit",), cursor=token))


class _VideoClient:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        self.calls.append((query, params))

        return QueryResult(self.results)


async def test_video_metadata_is_batched_and_version_matched() -> None:
    stamp = dt.datetime(2026, 10, 22, 12, 34, 56, 789000, dt.UTC)
    rows = [
        PostRow(
            "youtube.com", "video:abcdefghijk",
            "https://youtube.com/shorts/abcdefghijk", "Fallback channel",
            "Video title", "", stamp, stamp, "youtube",
        ),
        PostRow(
            "youtube.com", "post:community", "https://youtube.com/post/community",
            "Other channel", "Community post", "", stamp, stamp, "youtube",
        ),
        PostRow("publisher.test", "article", "https://publisher.test/article",
                "Author", "Article", "", stamp, stamp, "rss"),
    ]
    posts: list[dict[str, Any]] = [
        {"domain": row.domain, "id": row.post_id, "url": row.url,
         "author": row.author, "thumbnail": "/assets/saved.jpg",
         "media": [{"content_type": "video/mp4", "url": "/assets/saved.mp4"}]}
        for row in rows
    ]
    client = _VideoClient([{
        "post_id": "video:abcdefghijk", "payload": json.dumps({
            "channel_name": "Saved channel", "channel_id": "channel1",
            "channel_url": "https://youtube.com/@saved",
            "duration_text": "0:59", "views_text": "5K views",
            "thumbnail_url": "javascript:unsafe()",
        }),
    }])
    await add_youtube_post_metadata(cast(Any, client), posts, rows)
    assert len(client.calls) == 1
    query, params = client.calls[0]
    assert "toUnixTimestamp64Milli(scraped_at)" in query
    assert params == {"youtube_feed_versions": [
        ("video:abcdefghijk", int(stamp.timestamp() * 1000)),
        ("post:community", int(stamp.timestamp() * 1000)),
    ]}
    assert posts[0]["youtube"] == {
        "kind": "short", "video_id": "abcdefghijk",
        "channel": {"id": "channel1", "name": "Saved channel",
                    "url": "https://youtube.com/@saved"},
        "duration_text": "0:59", "views_text": "5K views",
        "thumbnail_url": "/assets/saved.jpg",
        "video_url": "/assets/saved.mp4",
    }
    assert posts[1]["youtube"]["kind"] == "post"
    assert posts[1]["youtube"]["video_id"] is None
    assert posts[1]["youtube"]["channel"]["name"] == "Other channel"
    assert "youtube" not in posts[2]


async def test_non_video_pages_need_no_additional_query() -> None:
    client = _VideoClient([])
    await add_youtube_post_metadata(cast(Any, client), [], [])
    assert not client.calls


async def test_generic_posts_payload_includes_saved_video_preview(
    tmp_path: pathlib.Path,
) -> None:
    class Client:
        async def fetch_json(
            self, query: str, params: dict[str, Any] | None = None,
        ) -> QueryResult:
            if params and "youtube_feed_versions" in params:
                return QueryResult([{
                    "post_id": "video:abcdefghijk", "payload": json.dumps({
                        "duration_text": "12:34", "views_text": "2K views",
                    }),
                }])

            return QueryResult([])

    stamp = dt.datetime(2026, 10, 22, tzinfo=dt.UTC)
    posts = await _post_payloads(cast(Any, Client()), [PostRow(
        "youtube.com", "video:abcdefghijk",
        "https://youtube.com/watch?v=abcdefghijk", "Saved channel", "Video",
        "", stamp, stamp, "youtube",
    )], tmp_path)
    assert posts[0]["youtube"]["duration_text"] == "12:34"
    assert posts[0]["youtube"]["views_text"] == "2K views"
    assert posts[0]["youtube"]["channel"]["name"] == "Saved channel"
    assert posts[0]["youtube"]["thumbnail_url"] == (
        "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
    )
