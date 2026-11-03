"""Both servers answer the browsing routes with the same JSON.

The rows cover every kind of post. Only invalid queries differ on purpose:
FastAPI answers them with the error envelope.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import pytest

from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.embeddings import get_embedding_model
from dank.links import LinkPost, post_index_rows
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import (
    COLLECTED_AT,
    asset,
    insert,
    post,
    raw_post,
    reddit_url,
    x_url,
)
from tests.api.parity import Get, both_servers

pytestmark = pytest.mark.clickhouse

BODY = "<p>Article body about space</p>"
ICON = "https://pbs.twimg.com/profile_images/1/account_normal.png"
SEARCH_VECTOR = (1.0, 0.0)
SOURCES = (
    SourceConfig("politics.example", (), tags=("politics",)),
    SourceConfig("news.ycombinator.com", ()),
    SourceConfig("x.com", ("alice",)),
    SourceConfig("reddit.com", (), feed_urls=(
        "https://www.reddit.com/r/python/",
    )),
)


def at(hours: int) -> dt.datetime:
    return COLLECTED_AT - dt.timedelta(hours=hours)


def tweet(
    post_id: str,
    handle: str,
    parent: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "__typename": "Tweet", "rest_id": post_id,
        "core": {"user_results": {"result": {
            "rest_id": f"9{handle}",
            "core": {"screen_name": handle, "name": handle.title()},
            "avatar": {"image_url": ICON},
        }}},
        "legacy": {
            "id_str": post_id, "conversation_id_str": "100",
            "in_reply_to_status_id_str": parent,
            "full_text": f"About space from {handle}",
            "created_at": "Wed Oct 07 10:00:00 +0000 2026",
            "reply_count": 1, "retweet_count": 2, "favorite_count": 3,
        },
        "views": {"count": "40"},
        **extra,
    }


def x_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    conversation = {
        "status": "captured", "target_id": "100", "conversation_id": "100",
        "captured_at": COLLECTED_AT.isoformat(), "limit": 50,
        "has_more": False, "reason": None,
        "posts": [tweet("103", "carol", "100")],
    }
    quoted = tweet("100", "alice", conversation_capture=conversation)
    quoted["legacy"]["quoted_status_id_str"] = "90"
    quoted["quoted_status_result"] = {"result": tweet("90", "dave")}
    captures = {
        "100": (quoted, "alice", 1),
        "101": (tweet("101", "alice", "100"), "alice", 2),
        "102": (tweet("102", "bob"), "bob", 3),
    }
    posts = [
        post(
            "x.com", post_id, url=x_url(handle, post_id), source="x",
            author=handle, title=f"Space post {post_id}", html=BODY,
            created_at=at(hours),
        )
        for post_id, (_, handle, hours) in captures.items()
    ]
    home = {
        "100": 1, "102": 2,
    }
    raws = [
        raw_post(
            "x.com", post_id,
            payload | (
                {"dank_x_home": {"feed": "for_you", "position": home[post_id]}}
                if post_id in home else {}
            ),
            url=x_url(handle, post_id), source="x",
        )
        for post_id, (payload, handle, _) in captures.items()
    ]

    return posts, raws


def link_rows(
    domain: str, post_id: str, url: str, html: str, source: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    return post_index_rows(LinkPost(
        domain, post_id, url, html, source, COLLECTED_AT,
    ))


async def seed(client: ClickHouseClient, assets: Any) -> None:
    image = "https://politics.example/lead.jpg"
    local = assets / "lead.jpg"
    local.touch()
    x_posts, x_raws = x_rows()
    hn_url = "https://news.ycombinator.com/item?id=1"
    publisher = "https://publisher.example/story"
    hn_html = (
        f'<p><a href="{publisher}" data-dank-hn-destination="1">Story</a></p>'
    )
    await insert(client, "posts", [
        post(
            "politics.example", "1", html=f'<img src="{image}">{BODY}',
            author="Reporter", title="Space politics", created_at=at(4),
            embedding=SEARCH_VECTOR,
        ),
        post(
            "culture.example", "2", html="<p>No match</p>", author="Critic",
            created_at=at(5), embedding=(0.0, 1.0),
        ),
        post(
            "publisher.example", "article", url=publisher, html=BODY,
            source="linked_article", title="Publisher story",
            created_at=at(6),
        ),
        post(
            "news.ycombinator.com", "1", url=hn_url, source="hacker_news",
            title="Ask HN about space", html=hn_html, created_at=at(7),
        ),
        post(
            "reddit.com", "t3_a", url=reddit_url("python", "a"),
            source="reddit", title="Reddit space", html=BODY,
            created_at=at(8),
        ),
        post(
            "youtube.com", "video:NnV_cWeoo5Q", source="youtube",
            url="https://www.youtube.com/watch?v=NnV_cWeoo5Q",
            title="Space video", author="Channel", created_at=at(9),
        ),
        *x_posts,
    ])
    await insert(client, "assets", [
        asset("politics.example", "1", image, local),
    ])
    await insert(client, "raw_posts", [
        raw_post("politics.example", "1", {"page_fetch_status": "failed"}),
        raw_post(
            "publisher.example", "article",
            {"page_html": BODY, "page_fetch_status": "ok"},
            url=publisher, source="linked_article",
        ),
        raw_post("news.ycombinator.com", "1", {
            "hckrnews": {"points": 10, "comments": 1, "link": publisher,
                         "time": 1},
            "hacker_news": {"day": "2026-10-30", "rank": 2,
                            "filters": ["top10", "top20"]},
            "comments": [{"id": 2, "by": "a", "text": "Hi", "time": 2}],
            "comment_capture": {"status": "captured", "total": 1},
        }, url=hn_url, source="hacker_news"),
        raw_post("reddit.com", "t3_a", {
            "score": 5, "num_comments": 1, "link_flair_text": "News",
            "url": publisher,
            "media": [{"url": "https://i.redd.it/a.jpg",
                       "asset_type": "photo"}],
            "comment_capture": {
                "status": "captured", "sort": "top", "limit": 10,
                "comments": [{
                    "id": "c1", "body": "Hello", "created_utc": 1.5,
                    "score": 2, "permalink": "/r/python/comments/a/c1/",
                }],
            },
        }, url=reddit_url("python", "a"), source="reddit"),
        raw_post(
            "youtube.com", "video:NnV_cWeoo5Q",
            {"channel_id": "c1", "duration_text": "1:00"}, source="youtube",
        ),
        *x_raws,
    ])
    hn_urls, hn_links = link_rows(
        "news.ycombinator.com", "1", hn_url, hn_html, "hacker_news",
    )
    article_urls, _ = link_rows(
        "publisher.example", "article", publisher, BODY, "linked_article",
    )
    await insert(client, "post_urls", hn_urls + article_urls)
    await insert(client, "post_links", hn_links)
    await insert(client, "post_reference_evidence", [{
        "domain": "news.ycombinator.com", "post_id": "1", "url": hn_url,
        "source": "hacker_news", "title": "Ask HN about space",
        "target_url": publisher, "kind": "post", "reference_id": "",
        "reference_url": hn_url, "author": "", "matched_url": publisher,
        "excerpt": "Story", "observed_at": at(10),
    }])
    # Semantic search reads this rather than loading the model in a test.
    await insert(client, "web_embedding_cache", [{
        "model_name": get_embedding_model().model_name,
        "search_text": "space",
        "embedding": list(SEARCH_VECTOR),
        # The table's TTL would drop a row dated like the others.
        "created_at": dt.datetime.now(dt.UTC),
    }])


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
    tmp_path: Any,
) -> AsyncIterator[tuple[Get, Get]]:
    assets = tmp_path / "assets"
    assets.mkdir(exist_ok=True)
    await seed(clickhouse, assets)
    configured = settings._replace(
        clickhouse=clickhouse_settings, sources=SOURCES,
    )

    async with both_servers(configured) as pair:
        yield pair


def without_snapshot(cursor: str | None) -> Any:
    """A For You cursor's fields, minus the moment each server was asked."""
    if cursor is None:
        return None

    fields = json.loads(base64.urlsafe_b64decode(cursor))

    return [*fields[:2], *fields[3:]]


async def same_json(old: Get, new: Get, path: str) -> Any:
    expected, actual = await old(path), await new(path)

    assert actual == expected, path

    return actual[1]


@pytest.mark.parametrize("path", [
    "/api/reader/posts",
    "/api/reader/posts?limit=100",
    "/api/reader/posts?kind=all",
    "/api/reader/posts?kind=all&platform=x&platform=reddit",
    "/api/reader/posts?kind=stories",
    "/api/reader/posts?kind=stories&blog_feed=sources",
    "/api/reader/posts?kind=stories&blog_feed=external",
    "/api/reader/posts?kind=reddit",
    "/api/reader/posts?kind=reddit&community=Python&flair=News",
    "/api/reader/posts?kind=x",
    "/api/reader/posts?kind=x&x_view=replies",
    "/api/reader/posts?kind=x&account=@Alice",
    "/api/reader/posts?kind=youtube",
    "/api/reader/posts?kind=hacker_news",
    "/api/reader/posts?kind=hacker_news&hn_filter=top10",
    "/api/reader/posts?kind=hacker_news&hn_filter=all",
    "/api/reader/posts?domain=politics.example&domain=CULTURE.example",
    "/api/reader/posts?tag=politics",
    "/api/reader/posts?tag=missing",
    "/api/reader/posts?author=reporter",
    "/api/reader/posts?author=Reporter&author_match=exact",
    "/api/reader/posts?after=2026-10-30&before=2026-10-30",
    "/api/reader/posts?sort=oldest",
    "/api/reader/posts?q=space",
    "/api/reader/posts?q=space&sort=relevance",
    "/api/reader/posts?q=space&mode=meaning",
    "/api/reader/posts?q=space&mode=meaning&sort=relevance&limit=1",
    "/api/reader/posts?q=space&mode=combined",
    "/api/reader/posts?q=space&mode=combined&sort=relevance",
    "/api/reader/posts?q=nothing-matches",
    "/api/reader/post?domain=politics.example&id=1",
    "/api/reader/post?domain=culture.example&id=2",
    "/api/reader/post?domain=publisher.example&id=article",
    "/api/reader/post?domain=news.ycombinator.com&id=1",
    "/api/reader/post?domain=reddit.com&id=t3_a",
    "/api/reader/post?domain=x.com&id=100",
    "/api/reader/post?domain=x.com&id=101",
    "/api/reader/post?domain=youtube.com&id=video:NnV_cWeoo5Q",
    "/api/reader/post?domain=politics.example&id=1&from=feed",
])
async def test_both_servers_return_the_same_json(
    servers: tuple[Get, Get], path: str,
) -> None:
    old, new = servers

    await same_json(old, new, path)


async def test_rows_cover_every_kind_of_post(
    servers: tuple[Get, Get],
) -> None:
    old, new = servers
    # The all feed hides a story once its publisher's article is collected.
    feed = await same_json(old, new, "/api/reader/posts?limit=100")
    stories = await same_json(
        old, new, "/api/reader/posts?kind=hacker_news&hn_filter=all",
    )
    posts = feed["posts"] + stories["posts"]
    detail = await same_json(
        old, new, "/api/reader/post?domain=x.com&id=100",
    )
    reply = await same_json(old, new, "/api/reader/post?domain=x.com&id=101")

    assert {key for item in posts for key in item} >= {
        "x", "reddit", "hacker_news", "youtube", "linked_post", "links",
        "links_count", "collection_info",
    }
    assert {item["source"] for item in posts} >= {
        "rss", "linked_article", "x", "reddit", "hacker_news", "youtube",
    }
    assert detail["x"]["quote"]["post"]["id"] == "90"
    assert detail["x"]["conversation"]["replies"][0]["id"] == "103"
    assert reply["x"]["reply_to"]["collected"] is True


@pytest.mark.parametrize("query", [
    "limit=2", "limit=2&sort=oldest", "limit=1&kind=x", "limit=1&q=space",
    "limit=2&q=space&mode=combined",
])
async def test_cursors_page_alike_and_work_on_either_server(
    servers: tuple[Get, Get], query: str,
) -> None:
    old, new = servers
    path = f"/api/reader/posts?{query}"
    page = await same_json(old, new, path)
    pages = 1

    while page["next_cursor"]:
        path_with_cursor = f"{path}&cursor={quote(page['next_cursor'])}"
        page = await same_json(old, new, path_with_cursor)
        pages += 1

    assert pages > 1


async def test_for_you_pages_match_apart_from_each_servers_snapshot(
    servers: tuple[Get, Get],
) -> None:
    old, new = servers
    path = "/api/reader/posts?kind=x&x_feed=for_you&limit=1"
    (_, expected), (_, actual) = await old(path), await new(path)
    cursor = expected.pop("next_cursor")

    assert without_snapshot(actual.pop("next_cursor")) == without_snapshot(
        cursor,
    )
    assert actual == expected
    assert [item["id"] for item in actual["posts"]] == ["100"]

    # A cursor from one server continues the other's session exactly.
    second = await same_json(old, new, f"{path}&cursor={quote(cursor)}")

    assert [item["id"] for item in second["posts"]] == ["102"]
    assert second["next_cursor"] is None


async def test_for_you_authors_match_apart_from_each_servers_snapshot(
    servers: tuple[Get, Get],
) -> None:
    old, new = servers
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"
    (_, expected), (_, actual) = await old(path), await new(path)
    actual_cursor = without_snapshot(actual.pop("next_cursor"))
    cursor = expected.pop("next_cursor")

    assert actual_cursor == without_snapshot(cursor)
    assert actual == expected
    assert actual["total"] == 2
    assert actual["authors"][0]["posts"]

    second = await same_json(old, new, f"{path}&cursor={quote(cursor)}")

    assert second["next_cursor"] is None


@pytest.mark.parametrize("path", [
    "/api/reader/x/authors?kind=x&x_feed=for_you&q=space",
    "/api/reader/x/authors?kind=x&x_feed=for_you&q=space&mode=meaning",
    "/api/reader/x/authors?kind=x&x_feed=for_you&author=alice",
])
async def test_filtered_authors_match(
    servers: tuple[Get, Get], path: str,
) -> None:
    old, new = servers
    (_, expected), (_, actual) = await old(path), await new(path)

    assert actual == expected


@pytest.mark.parametrize("query", [
    "unknown=value", "limit=0", "limit=101", "limit=oops", "limit=5.0",
    "limit=", "limit=1&limit=2", "q=a&q=b", "mode=typo", "mode=",
    "sort=random", "sort=", "sort=relevance", "author_match=typo",
    "author_match=",
    "after=2026-02-30", "after=20261008", "before=2026-10-08T00:00:00",
    "after=1700000000", "after=2026-10-09&before=2026-10-08", "cursor=bad",
    "cursor=a&cursor=b", "tag=", "domain=", "domain=+", "flair=",
    "kind=unknown", "kind=stories&community=python", "community=python",
    "kind=reddit&community=python'+OR+1",
    "kind=reddit&community=r/python",
    "flair=News", "account=someone", "kind=x&account=bad/handle",
    "hn_filter=top10", "hn_filter=", "kind=hacker_news&hn_filter=",
    "kind=hacker_news&hn_filter=top5", "x_view=replies", "x_view=",
    "kind=x&x_view=", "kind=x&x_view=sideways", "blog_feed=sources",
    "blog_feed=", "kind=stories&blog_feed=", "kind=stories&blog_feed=other",
    "x_feed=for_you", "kind=x&x_feed=other", "kind=x&platform=x",
    "platform=unknown", "q=" + "+".join(["word"] * 51),
])
async def test_both_servers_reject_the_same_queries(
    servers: tuple[Get, Get], query: str,
) -> None:
    old, new = servers
    path = "/api/reader/posts?" + query
    (old_status, old_body), (new_status, new_body) = (
        await old(path), await new(path),
    )

    assert (old_status, new_status) == (400, 400), path
    assert new_body["code"] in {"invalid_input", "invalid_request"}

    if new_body["code"] == "invalid_input":
        assert new_body["error"] == old_body["error"]


@pytest.mark.parametrize("query", [
    "kind=", "x_feed=", "cursor=", "after=", "before=", "q=", "q=+",
    "author=", "community=", "account=", "limit=+5", "limit=1_0",
    "limit=%205",
    "kind=all&platform=stories", "kind=hacker_news&hn_filter=all",
    "kind=x&x_view=posts", "kind=stories&blog_feed=external",
    "kind=reddit&flair=News&flair=News",
    "domain=a.example&domain=a.example", "after=2026-10-08",
])
async def test_both_servers_accept_the_same_queries(
    servers: tuple[Get, Get], query: str,
) -> None:
    old, new = servers
    await same_json(old, new, "/api/reader/posts?" + query)


@pytest.mark.parametrize(("query", "expected"), [
    ("domain=politics.example&id=gone", 404),
    ("domain=&id=1", 400),
    ("id=1", 400),
    ("domain=+&id=1", 400),
])
async def test_post_failures_match(
    servers: tuple[Get, Get], query: str, expected: int,
) -> None:
    old, new = servers
    path = "/api/reader/post?" + query
    (old_status, _), (new_status, _) = await old(path), await new(path)

    assert (old_status, new_status) == (expected, expected)


@pytest.mark.parametrize("query", [
    "", "kind=x", "kind=reddit&x_feed=for_you",
    "kind=x&x_feed=for_you&limit=51", "kind=x&x_feed=for_you&cursor=bad",
    "kind=x&x_feed=for_you&cursor=a&cursor=b",
    "kind=x&x_feed=for_you&account=invalid/handle",
])
async def test_both_servers_reject_the_same_author_queries(
    servers: tuple[Get, Get], query: str,
) -> None:
    old, new = servers
    path = "/api/reader/x/authors?" + query
    (old_status, _), (new_status, _) = await old(path), await new(path)

    assert (old_status, new_status) == (400, 400)
