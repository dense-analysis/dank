import datetime as dt
import json
import pathlib
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.config import HackerNewsFilter, SourceConfig, load_settings
from dank.storage.clickhouse import QueryResult
from dank.web.app import PostRow, create_app
from dank.web.reader_accounts import source_kind
from dank.web.reader_api import handle_sources
from dank.web.reader_hacker_news import add_hacker_news_metadata
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)

NOW = dt.datetime(2026, 10, 17, 15, tzinfo=dt.UTC)
POST = PostRow(
    "news.ycombinator.com", "123", "https://news.ycombinator.com/item?id=123",
    "Ada", "A story", "", NOW, NOW, "hacker_news",
)


def summary() -> dict[str, Any]:
    return {
        "domain": POST.domain, "id": POST.post_id, "url": POST.url,
        "created_at": POST.created_at.isoformat(),
    }


def snapshot() -> dict[str, Any]:
    return {
        "hckrnews": {"link_text": "A story", "points": 100, "comments": 22,
                     "link": "https://example.test/article"},
        "hacker_news": {"day": "2026-10-17", "rank": 3,
                        "filters": ["top10", "top20", "top50", "all"]},
    }


def stored(payload: object, when: dt.datetime = NOW) -> dict[str, Any]:
    return {
        "post_id": "123", "payload": json.dumps(payload), "scraped_at": when,
    }


@pytest.mark.parametrize("ranking", list(HackerNewsFilter))
def test_daily_filters_use_captured_eligibility_and_processed_version(
    ranking: HackerNewsFilter,
):
    filters = parse_filters(MultiDict({
        "kind": "hacker_news", "hn_filter": ranking, "q": "narrow search",
    }))
    query, params = build_query(filters, ())
    assert "domain = 'news.ycombinator.com'" in query
    assert "positionCaseInsensitiveUTF8" in query
    assert "rank()" not in query.lower()

    if ranking == "all":
        assert "raw_posts" not in query
    else:
        assert "(post_id, toUnixTimestamp64Milli(updated_at)) IN" in query
        assert "SELECT post_id, toUnixTimestamp64Milli(scraped_at)" in query
        assert "'hacker_news', 'filters', 'Array(String)'" in query
        assert params["hn_filter"] == ranking


def test_default_ranking_and_cursor_are_bound():
    filters = parse_filters(MultiDict({"kind": "hacker_news"}))
    assert filters.hn_filter == "top50"
    token = encode_cursor(filters, NOW, POST.domain, POST.post_id)
    assert decode_cursor(filters._replace(cursor=token)) == (
        NOW, POST.domain, POST.post_id,
    )

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(filters._replace(cursor=token, hn_filter="top10"))


@pytest.mark.parametrize("query", [
    {"kind": "hacker_news", "hn_filter": "homepage"},
    {"kind": "hacker_news", "hn_filter": ""},
    {"kind": "reddit", "hn_filter": "top10"},
    {"hn_filter": "all"},
])
def test_invalid_rankings_rejected(query: dict[str, str]):
    with pytest.raises(ValueError, match="hn_filter"):
        parse_filters(MultiDict(query))


def test_hacker_news_is_a_separate_source_kind():
    assert source_kind(POST.domain) == "hacker_news"
    query, params = build_query(ReaderFilters(kind="stories"), (
        SourceConfig(POST.domain, ()), SourceConfig("blog.test", ()),
    ))
    assert "domain IN %(blog_domains)s" in query
    assert params["blog_domains"] == ["blog.test"]


async def test_hacker_news_source_has_readable_name(tmp_path: pathlib.Path):
    settings = load_settings("config.example.toml")._replace(
        sources=(SourceConfig(POST.domain, ()),), data_dir=tmp_path,
    )
    app = create_app(settings, page_size=50)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"domain": POST.domain, "count": 3},
    ])
    app["clickhouse"] = client
    response = await handle_sources(make_mocked_request(
        "GET", "/api/reader/sources", app=app,
    ))
    source = json.loads(response.text or "")["sources"][0]
    assert source["name"] == "Hacker News"
    assert source["kind"] == "hacker_news"


async def test_listing_metadata_uses_exact_version_and_no_comment_history():
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored(snapshot())])
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=False,
    )
    assert payloads[0]["hacker_news"] == {
        "points": 100, "comments": 22, "day": "2026-10-17", "rank": 3,
        "discussion_url": POST.url, "external_url": "https://example.test/article",
        "submitted_at": None,
    }
    assert client.fetch_json.await_count == 1
    query, params = client.fetch_json.call_args.args
    assert "IN %(hacker_news_versions)s" in query
    assert params["hacker_news_versions"] == [
        ("123", round(NOW.timestamp() * 1000)),
    ]


async def test_detail_renders_safe_comments_in_thread_order():
    payload = snapshot()
    payload.update(
        comment_capture={"status": "partial", "total": 22, "reason": "limit"},
        comments=[
            {"id": 1, "parent": 123, "by": "Alice", "time": 10,
             "text": '<p>Hello <a href="javascript:alert(1)">link</a>'
                     '<script>bad()</script><img onerror="bad()"></p>'},
            {"id": 3, "parent": 123, "by": "Bob", "text": "Sibling"},
            {"id": 2, "parent": 1, "by": "Carol", "text": "Reply"},
            {"id": 4, "parent": 123, "deleted": True, "text": "Deleted"},
            {"id": 5, "parent": 123, "dead": True, "text": "Dead"},
        ],
    )
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored(payload)])
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=True,
    )
    hn = payloads[0]["hacker_news"]
    comments = hn["comments_items"]
    assert [comment["id"] for comment in comments] == ["1", "2", "3"]
    assert [comment["depth"] for comment in comments] == [0, 1, 0]
    assert comments[1]["parent_id"] == "1"
    assert comments[0]["created_at"] == "1970-01-01T00:00:10+00:00"
    assert "javascript:" not in comments[0]["html"]
    assert "<script" not in comments[0]["html"]
    assert "onerror" not in comments[0]["html"]
    assert hn["comment_capture"] == {
        "status": "partial", "captured": 3, "total": 22, "reason": "limit",
    }
    assert client.fetch_json.await_count == 1


@pytest.mark.parametrize("latest_status", [None, "disabled", "failed"])
async def test_earlier_comments_survive_listing_and_failed_detail(
    latest_status: str | None,
):
    current = snapshot()

    if latest_status:
        current["comment_capture"] = {"status": latest_status}

    old = {
        "hckrnews": {"points": 5},
        "comment_capture": {"status": "captured", "total": 1},
        "comments": [{"id": 1, "parent": 123, "text": "Archived"}],
    }
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([stored(current)]),
        QueryResult([stored(old, NOW - dt.timedelta(hours=1))]),
    ]
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=True,
    )
    hn = payloads[0]["hacker_news"]
    assert hn["points"] == 100
    assert hn["comments"] == 22
    assert hn["comments_items"][0]["html"] == "Archived"
    assert hn["comment_capture"]["status"] == "captured"
    assert "earlier detailed scrape" in hn["comment_capture"]["reason"]
    query, params = client.fetch_json.call_args.args
    assert "toUnixTimestamp64Milli(scraped_at) < %(before_0)s" in query
    assert params["before_0"] == round(NOW.timestamp() * 1000)


@pytest.mark.parametrize("when", [NOW, NOW + dt.timedelta(hours=1)])
async def test_future_comments_cannot_leak_into_processed_version(
    when: dt.datetime,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([stored(snapshot())]),
        QueryResult([stored({
            "comment_capture": {"status": "captured"},
            "comments": [{"id": 1, "parent": 123, "text": "Future"}],
        }, when)]),
    ]
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=True,
    )
    assert payloads[0]["hacker_news"]["comments_items"] == []
    capture = payloads[0]["hacker_news"]["comment_capture"]
    assert capture["status"] == "disabled"


MALFORMED_PAYLOADS: list[object] = [None, [], {
    "hckrnews": [], "hacker_news": {"day": []}, "story": [],
    "comment_capture": {"status": []}, "comments": "wrong",
}, {
    "comment_capture": {"status": "captured", "total": True},
    "comments": [None, [], {"id": [], "text": "Bad"},
                 {"id": 1, "parent": [], "time": 2**52, "text": "Okay"}],
}]


@pytest.mark.parametrize("payload", MALFORMED_PAYLOADS)
async def test_malformed_raw_nesting_is_safe(payload: object):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([stored(payload)]), QueryResult([]),
    ]
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=True,
    )
    hn = payloads[0]["hacker_news"]
    assert hn["day"] == "2026-10-17"
    assert hn["comment_capture"]["total"] is None


@pytest.mark.parametrize("points,comments,expected", [
    ("100", "22", (100, 22)), (0, "0", (0, 0)),
    (True, False, (None, None)), ("-1", "1.5", (None, None)),
    ("9007199254740992", [], (None, None)),
])
async def test_homepage_counts_allow_only_bounded_integer_text(
    points: object, comments: object, expected: tuple[int | None, int | None],
):
    payload = snapshot()
    payload["hckrnews"].update(points=points, comments=comments)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored(payload)])
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=False,
    )
    hn = payloads[0]["hacker_news"]
    assert (hn["points"], hn["comments"]) == expected


@pytest.mark.parametrize("story_time,listing_time,expected", [
    (10, 20, "1970-01-01T00:00:10+00:00"),
    (None, "20", "1970-01-01T00:00:20+00:00"),
    (2**52, 20, "1970-01-01T00:00:20+00:00"),
    (True, None, None),
    (None, None, None),
])
async def test_submission_timestamp_never_uses_front_page_arrival(
    story_time: object, listing_time: object, expected: str | None,
):
    payload = snapshot()
    payload["story"] = {"time": story_time}
    payload["hckrnews"].update(time=listing_time, date=int(NOW.timestamp()))
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored(payload)])
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=False,
    )
    assert payloads[0]["hacker_news"]["submitted_at"] == expected
    assert payloads[0]["hacker_news"]["day"] == "2026-10-17"


async def test_full_discussions_preserve_over_500_comments_without_recursion():
    payload = snapshot()
    payload.update(
        comment_capture={"status": "captured", "total": 750},
        comments=[
            {"id": 1000 + index, "parent": 999 + index if index else 123,
             "text": f"Reply {index}"}
            for index in range(750)
        ] + [
            {"id": 2000, "parent": 2001, "text": "Cycle"},
            {"id": 2001, "parent": 2000, "text": "Cycle"},
            {"id": 1000, "parent": 1001, "text": "Duplicate"},
        ],
    )
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored(payload)])
    payloads = [summary()]
    await add_hacker_news_metadata(
        client, payloads, [POST], include_discussion=True,
    )
    hn = payloads[0]["hacker_news"]
    assert len(hn["comments_items"]) == 750
    assert hn["comments_items"][-1]["html"] == "Reply 749"
    assert hn["comments_items"][-1]["depth"] == 32
    assert hn["comment_capture"]["captured"] == 750
    assert hn["comment_capture"]["status"] == "captured"


async def test_rss_story_metadata_keeps_unknown_counts_and_native_link():
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([stored({"rss": {
        "title": "Feed title", "url": "https://publisher.test/story",
        "time": int(NOW.timestamp()),
    }})])
    result = summary()
    await add_hacker_news_metadata(
        client, [result], [POST], include_discussion=False,
    )
    metadata = result["hacker_news"]
    assert metadata["points"] is None
    assert metadata["comments"] is None
    assert metadata["rank"] is None
    assert metadata["external_url"] == "https://publisher.test/story"
    assert metadata["discussion_url"] == POST.url
    assert metadata["submitted_at"] == NOW.isoformat()
