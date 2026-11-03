"""GET /api/reader/posts, /post and /x/authors: HTTP in, JSON out."""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.model import (
    AuthorGroup,
    AuthorListing,
    ListedPost,
    Recommendation,
)
from tests.api.client import http_client
from tests.api.posts.fakes import (
    NOW,
    FakePostStore,
    FakeSearchEmbeddings,
    payload,
    row,
)

SNAPSHOT = NOW.isoformat()


def listed(*domains: str) -> list[ListedPost]:
    return [ListedPost(row(domain)) for domain in domains]


def decoded(cursor: str) -> list[Any]:
    return json.loads(base64.urlsafe_b64decode(cursor))


async def test_empty_page_has_no_cursor(client: AsyncClient) -> None:
    response = await client.get("/api/reader/posts")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "posts": [], "next_cursor": None, "limited": False,
    }


async def test_posts_paginate_with_a_cursor_for_the_next_page(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.listed = listed("c.example", "b.example", "a.example")

    response = await client.get("/api/reader/posts?limit=2")
    body = response.json()

    assert [post["domain"] for post in body["posts"]] == [
        "c.example", "b.example",
    ]
    assert body["limited"] is False
    assert decoded(body["next_cursor"])[3:] == ["b.example", "same"]

    second = await client.get(
        "/api/reader/posts?limit=2&cursor=" + quote(body["next_cursor"]),
    )

    assert second.status_code == 200


async def test_a_cursor_cannot_cross_filters(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.listed = listed("x.com", "x.com")
    path = "/api/reader/posts?kind=x&account=Example&q=article&limit=1"

    body = (await client.get(path)).json()
    response = await client.get(
        path.replace("Example", "other")
        + "&cursor=" + quote(body["next_cursor"]),
    )

    assert (response.status_code, response.json()) == (400, {
        "error": "Invalid cursor for these filters",
        "code": "invalid_input",
        "fields": [],
    })


async def test_relevance_reports_capped_results_without_a_cursor(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.listed = listed("b.example", "a.example")

    response = await client.get(
        "/api/reader/posts?q=article&sort=relevance&limit=1",
    )
    body = response.json()

    assert len(body["posts"]) == 1
    assert (body["limited"], body["next_cursor"]) == (True, None)


@pytest.mark.parametrize("mode", ["meaning", "combined"])
async def test_semantic_search_reaches_the_store_with_its_embedding(
    client: AsyncClient,
    store: FakePostStore,
    embeddings: FakeSearchEmbeddings,
    mode: str,
) -> None:
    embeddings.vectors["article"] = (1.0, 0.0)
    store.semantic[(1.0, 0.0)] = listed("b.example", "a.example")

    response = await client.get(
        f"/api/reader/posts?q=article&mode={mode}&sort=relevance&limit=1",
    )
    body = response.json()

    assert [post["domain"] for post in body["posts"]] == ["b.example"]
    assert body["limited"] is True


@pytest.mark.parametrize("path", [
    "/api/reader/posts?mode=words",
    "/api/reader/posts?mode=meaning",
    "/api/reader/posts?mode=combined",
    "/api/reader/posts?q=article&mode=words",
])
async def test_only_a_semantic_search_needs_an_embedding(
    client: AsyncClient, path: str,
) -> None:
    # The fake has no vectors, so asking it for one would fail the request.
    assert (await client.get(path)).status_code == 200


@pytest.mark.parametrize("embedding", [None, ()])
async def test_unavailable_embedding_is_an_explicit_503(
    app: FastAPI,
    embeddings: FakeSearchEmbeddings,
    embedding: tuple[float, ...] | None,
) -> None:
    embeddings.vectors["article"] = embedding

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get(
            "/api/reader/posts?q=article&mode=combined",
        )

    assert response.status_code == 503
    assert response.json()["code"] == "unavailable"


async def test_repeated_list_filters_all_apply(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.listed = listed("a.example", "b.example", "c.example")

    response = await client.get(
        "/api/reader/posts?domain=A.example&domain=b.example&domain=a.example",
    )

    assert [post["domain"] for post in response.json()["posts"]] == [
        "a.example", "b.example",
    ]


async def test_for_you_cursor_keeps_its_snapshot_and_position(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.listed = [
        ListedPost(
            row("x.com", "2"), Recommendation("2026-10-20 10:00:00", 5),
        ),
        ListedPost(
            row("x.com", "1"), Recommendation("2026-10-20 09:00:00", 6),
        ),
    ]

    response = await client.get(
        "/api/reader/posts?kind=x&x_feed=for_you&limit=1",
    )
    cursor = response.json()["next_cursor"]

    assert decoded(cursor)[2:] == [
        SNAPSHOT, "2026-10-20T10:00:00+00:00", 5, "x.com", "2",
    ]

    second = await client.get(
        "/api/reader/posts?kind=x&x_feed=for_you&limit=1&cursor="
        + quote(cursor),
    )

    assert decoded(second.json()["next_cursor"])[2] == SNAPSHOT


async def test_backend_failure_is_an_explicit_503(
    app: FastAPI, store: FakePostStore,
) -> None:
    store.failing = True

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get("/api/reader/posts")

    assert response.status_code == 503
    assert response.json()["error"] == (
        "The collection is unavailable. Please try again."
    )
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize(("query", "field"), [
    ("unknown=value", "query.unknown"),
    ("limit=broken", "query.limit"),
    ("limit=0", "query.limit"),
    ("limit=101", "query.limit"),
    ("limit=5.0", "query.limit"),
    ("limit=", "query.limit"),
    ("mode=typo", "query.mode"),
    ("mode=", "query.mode"),
    ("sort=random", "query.sort"),
    ("author_match=typo", "query.author_match"),
    ("kind=unknown", "query.kind"),
    ("hn_filter=top5&kind=hacker_news", "query.hn_filter"),
    ("hn_filter=&kind=hacker_news", "query.hn_filter"),
    ("x_view=sideways&kind=x", "query.x_view"),
    ("x_view=&kind=x", "query.x_view"),
    ("blog_feed=other&kind=stories", "query.blog_feed"),
    ("x_feed=other&kind=x", "query.x_feed"),
    ("q=a&q=b", "query.q"),
    ("limit=1&limit=2", "query.limit"),
    ("cursor=a&cursor=b", "query.cursor"),
])
async def test_malformed_queries_name_their_field(
    client: AsyncClient, query: str, field: str,
) -> None:
    response = await client.get("/api/reader/posts?" + query)
    body = response.json()

    assert response.status_code == 400
    assert (body["error"], body["code"]) == (
        "The request is invalid.", "invalid_request",
    )
    assert [item["path"] for item in body["fields"]] == [field]


@pytest.mark.parametrize(("query", "message"), [
    ("sort=relevance", "Relevance ordering requires a search"),
    ("after=2026-02-30", "Dates must be YYYY-MM-DD between 1900 and 2298"),
    ("before=20261008", "Dates must be YYYY-MM-DD between 1900 and 2298"),
    ("after=2026-10-09&before=2026-10-08",
     "after must not be later than before"),
    ("cursor=bad", "Invalid cursor for these filters"),
    ("tag=", "Source and tag filters must be nonempty and bounded"),
    ("flair=&kind=reddit", "Flair filters must be nonempty and bounded"),
    ("community=python",
     "community requires kind=reddit and a community name"),
    ("flair=News", "flair requires kind=reddit"),
    ("account=someone", "account requires kind=x and an account handle"),
    ("hn_filter=top10", "hn_filter requires a valid Hacker News ranking"),
    ("x_view=replies", "x_view requires kind=x"),
    ("blog_feed=sources",
     "blog_feed requires kind=stories and sources or external"),
    ("x_feed=for_you", "x_feed requires kind=x and for_you"),
    ("platform=x&kind=x",
     "platform requires kind=all and stories, reddit, x, youtube "
     "or hacker_news"),
    ("q=" + "+".join(["word"] * 51),
     "Search is limited to 2000 characters and 50 terms"),
])
async def test_rule_violations_keep_the_wording_the_reader_shows(
    client: AsyncClient, query: str, message: str,
) -> None:
    response = await client.get("/api/reader/posts?" + query)

    assert (response.status_code, response.json()) == (400, {
        "error": message, "code": "invalid_input", "fields": [],
    })


@pytest.mark.parametrize("query", [
    "kind=", "x_feed=", "cursor=", "after=", "before=", "q=", "author=",
    "community=", "account=", "q=+", "kind=all",
    "kind=stories&blog_feed=external",
    "kind=hacker_news&hn_filter=all", "kind=x&x_view=replies",
    "kind=x&x_feed=for_you", "platform=x&platform=reddit",
    "kind=reddit&flair=News&flair=Help&community=Python",
    "domain=a.example&tag=politics", "limit=+5", "limit=100",
])
async def test_queries_the_aiohttp_server_accepts_stay_accepted(
    client: AsyncClient, query: str,
) -> None:
    assert (await client.get("/api/reader/posts?" + query)).status_code == 200


async def test_post_detail_returns_a_direct_object(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.posts["a.example", "same"] = row("a.example")

    response = await client.get(
        "/api/reader/post?domain=+a.example+&id=same",
    )

    assert response.status_code == 200
    assert response.json() == payload(row("a.example"))


async def test_missing_post_is_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/reader/post?domain=gone&id=gone")

    assert (response.status_code, response.json()) == (404, {
        "error": "Article not found", "code": "not_found", "fields": [],
    })


@pytest.mark.parametrize("query", [
    "", "domain=a.example", "id=1", "domain=+&id=1", "domain=a.example&id=",
])
async def test_post_needs_a_domain_and_id(
    client: AsyncClient, query: str,
) -> None:
    response = await client.get("/api/reader/post?" + query)

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"


async def test_post_ignores_other_keys_as_the_aiohttp_server_does(
    client: AsyncClient, store: FakePostStore,
) -> None:
    store.posts["a.example", "same"] = row("a.example")

    response = await client.get(
        "/api/reader/post?domain=a.example&id=same&from=feed",
    )

    assert response.status_code == 200


async def test_left_out_fields_stay_left_out_and_nulls_stay_null(
    client: AsyncClient, store: FakePostStore,
) -> None:
    post = row("reddit.com", "t3_a", source="reddit")
    store.posts["reddit.com", "t3_a"] = post
    detail: dict[str, Any] = payload(post) | {
        "reddit": {
            "community": "python", "score": None, "comments": 3,
            "flair": None, "icon_url": None, "video": None, "videos": [],
            "comment_capture": {
                "status": "not_captured", "sort": "best", "limit": 50,
                "total_available": None, "has_more": False, "comments": [],
            },
        },
    }
    store.payloads_by_key["reddit.com", "t3_a"] = detail

    response = await client.get("/api/reader/post?domain=reddit.com&id=t3_a")

    assert response.json() == detail
    assert "x" not in response.json()
    assert "images" not in response.json()["reddit"]


async def test_unexpected_payload_keys_fail_rather_than_leak(
    app: FastAPI, store: FakePostStore,
) -> None:
    post = row("a.example")
    store.posts["a.example", "same"] = post
    store.payloads_by_key["a.example", "same"] = payload(post) | {"new": 1}

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get(
            "/api/reader/post?domain=a.example&id=same",
        )

    assert response.status_code == 503


async def test_authors_page_shapes_groups_and_a_cursor(
    client: AsyncClient, store: FakePostStore,
) -> None:
    example = payload(row("x.com", "9", source="x"))
    store.author_listing = AuthorListing(
        authors=(
            AuthorGroup("alice", "Alice", None, 2, (example,)),
            AuthorGroup("bob", "bob", "https://pbs.twimg.com/b.png", 1, ()),
        ),
        has_more=True, total=5, unattributed_posts=1,
    )

    response = await client.get(
        "/api/reader/x/authors?kind=x&x_feed=for_you&limit=2",
    )
    body = response.json()

    assert response.status_code == 200
    assert body["authors"] == [
        {"handle": "alice", "name": "Alice", "icon_url": None, "count": 2,
         "posts": [example]},
        {"handle": "bob", "name": "bob",
         "icon_url": "https://pbs.twimg.com/b.png", "count": 1, "posts": []},
    ]
    assert (body["total"], body["unattributed_posts"]) == (5, 1)
    assert decoded(body["next_cursor"])[2:] == [SNAPSHOT, 1, "bob"]

    second = await client.get(
        "/api/reader/x/authors?kind=x&x_feed=for_you&limit=2&cursor="
        + quote(body["next_cursor"]),
    )

    assert second.status_code == 200


async def test_last_authors_page_has_no_cursor(client: AsyncClient) -> None:
    response = await client.get("/api/reader/x/authors?kind=x&x_feed=for_you")

    assert response.json() == {
        "authors": [], "next_cursor": None, "total": 0,
        "unattributed_posts": 0,
    }


@pytest.mark.parametrize("query", [
    "kind=x&x_feed=for_you&limit=51",
    "kind=x&x_feed=for_you&cursor=a&cursor=b",
    "kind=x&x_feed=for_you&surprise=1",
])
async def test_malformed_author_queries_are_rejected(
    client: AsyncClient, query: str,
) -> None:
    response = await client.get("/api/reader/x/authors?" + query)

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"


@pytest.mark.parametrize(("query", "message"), [
    ("kind=x", "Authors require X For You and a limit up to 50"),
    ("", "Authors require X For You and a limit up to 50"),
    ("kind=x&x_feed=for_you&cursor=bad",
     "Invalid author cursor for these filters"),
])
async def test_author_rule_violations_keep_their_wording(
    client: AsyncClient, query: str, message: str,
) -> None:
    response = await client.get("/api/reader/x/authors?" + query)

    assert (response.status_code, response.json()["error"]) == (400, message)


async def test_openapi_describes_filters_and_each_post_shape(
    client: AsyncClient,
) -> None:
    document = (await client.get("/api/openapi.json")).json()
    schemas = document["components"]["schemas"]
    posts = document["paths"]["/api/reader/posts"]["get"]
    parameters = {item["name"]: item for item in posts["parameters"]}

    assert parameters["domain"]["schema"]["type"] == "array"
    assert parameters["limit"]["schema"]["maximum"] == 100
    assert parameters["mode"]["schema"]["enum"] == [
        "words", "meaning", "combined",
    ]
    assert "conversation" in schemas["XDetailOut"]["properties"]
    assert "conversation" not in schemas["XOut"]["properties"]
    assert "comments_items" not in schemas["HackerNewsOut"]["properties"]
    assert schemas["PostOut"]["additionalProperties"] is False
