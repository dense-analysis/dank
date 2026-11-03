"""Connections over HTTP from real rows in a throwaway ClickHouse database."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.connections.clickhouse import ClickHouseConnectionStore
from dank.api.connections.deps import connection_store
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import COLLECTED_AT, insert, post

pytestmark = pytest.mark.clickhouse

ORIGIN = "https://origin.test/post"
TARGET = "https://target.test/article"
STAMP = COLLECTED_AT - dt.timedelta(hours=1)
ROOT = {
    "domain": "origin.test", "id": "123", "url": ORIGIN, "title": "Post 123",
    "source": "rss",
}


@pytest.fixture
def app(app: FastAPI, clickhouse: ClickHouseClient) -> FastAPI:
    app.dependency_overrides[connection_store] = lambda: (
        ClickHouseConnectionStore(clickhouse)
    )

    return app


def evidence(
    target: str = TARGET,
    *,
    domain: str = "origin.test",
    post_id: str = "123",
    url: str = ORIGIN,
    source: str = "rss",
    kind: str = "comment",
    reference_id: str = "comment-2",
    observed_at: dt.datetime = STAMP,
) -> dict[str, Any]:
    return {
        "domain": domain, "post_id": post_id, "url": url, "source": source,
        "title": f"Post {post_id}", "target_url": target, "kind": kind,
        "reference_id": reference_id, "reference_url": url + "#comment-2",
        "author": "Reader", "matched_url": target + "#exact",
        "excerpt": "Read the evidence", "observed_at": observed_at,
    }


def identity(url: str, domain: str, post_id: str) -> dict[str, Any]:
    return {
        "url": url, "domain": domain, "post_id": post_id,
        "observed_at": STAMP,
    }


async def seed_root(clickhouse: ClickHouseClient) -> None:
    await insert(clickhouse, "posts", [post("origin.test", "123", url=ORIGIN)])


async def test_connections_keep_directions_counts_and_uncollected_targets(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_root(clickhouse)
    await insert(clickhouse, "posts", [
        post("reader.test", str(index), url=f"https://reader.test/{index}")
        for index in range(3)
    ])
    await insert(clickhouse, "post_urls", [
        identity(ORIGIN, "origin.test", "123"),
    ])
    await insert(clickhouse, "post_reference_evidence", [
        evidence(),
        *(
            evidence(
                ORIGIN, domain="reader.test", post_id=str(index),
                url=f"https://reader.test/{index}",
            )
            for index in range(3)
        ),
    ])

    response = await client.get(
        "/api/reader/connections?domain=origin.test&id=123",
    )
    body = response.json()
    outgoing = body["outgoing"][0]

    assert response.status_code == 200
    assert body["root"] == ROOT
    assert (body["incoming_count"], body["outgoing_count"]) == (3, 1)
    assert (body["limit"], body["coverage"]) == (100, "saved_evidence")
    assert (body["context"], body["related"], body["related_count"]) == (
        {}, [], 0,
    )
    assert body["incoming"][0]["target"]["post"] == ROOT
    assert body["incoming"][0]["direction"] == "incoming"
    assert outgoing["direction"] == "outgoing"
    assert outgoing["target"] == {
        "url": TARGET, "post": None, "youtube_item_id": None, "title": TARGET,
    }
    assert outgoing["evidence"]["matched_url"] == TARGET + "#exact"
    assert outgoing["evidence"]["observed_at"] == STAMP.isoformat()


async def test_collected_targets_use_alias_identity_and_their_own_title(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    canonical = "https://target.test/canonical"
    await seed_root(clickhouse)
    await insert(clickhouse, "posts", [post(
        "target.test", "canonical", url=canonical, source="linked_article",
        title="Original", html="<p>Saved</p>",
    )])
    await insert(clickhouse, "post_urls", [
        identity(TARGET, "target.test", "canonical"),
    ])
    await insert(clickhouse, "post_reference_evidence", [evidence()])

    response = await client.get(
        "/api/reader/connections?domain=origin.test&id=123",
    )
    target = response.json()["outgoing"][0]["target"]

    assert target["post"] == {
        "domain": "target.test", "id": "canonical", "url": canonical,
        "title": "Original", "source": "linked_article",
    }
    assert target["title"] == "Original"


async def test_metadata_only_youtube_is_a_real_reader_destination(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    url = "https://www.youtube.com/watch?v=NnV_cWeoo5Q"
    await seed_root(clickhouse)
    await insert(clickhouse, "post_reference_evidence", [evidence(url)])
    await insert(clickhouse, "youtube_video_metadata", [{
        "video_id": "NnV_cWeoo5Q", "observed_at": STAMP,
        "payload": '{"title": "Security in the LLM age"}',
    }])

    response = await client.get(
        "/api/reader/connections?domain=origin.test&id=123",
    )

    assert response.json()["outgoing"][0]["target"] == {
        "url": url, "post": None, "youtube_item_id": "video:NnV_cWeoo5Q",
        "title": "Security in the LLM age",
    }


async def test_unknown_root_is_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/reader/connections?domain=no&id=no")

    assert (response.status_code, response.json()["code"]) == (
        404, "not_found",
    )


async def test_context_narrows_related_publishers_but_not_outgoing_links(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    root_url = "https://reddit.com/r/kernel/comments/123/"
    canonical = "https://target.test/canonical"
    alias = "https://target.test/observed-alias"
    sibling_url = "https://www.reddit.com/r/kernel/comments/456/"
    other_url = "https://www.reddit.com/r/python/comments/789/"
    await insert(clickhouse, "posts", [
        post("reddit.com", "123", url=root_url, source="reddit"),
        post("reddit.com", "456", url=sibling_url, source="reddit"),
        post("reddit.com", "789", url=other_url, source="reddit"),
        post(
            "target.test", "canonical", url=canonical,
            source="linked_article", title="Original", html="<p>Saved</p>",
        ),
    ])
    await insert(clickhouse, "post_urls", [
        identity(TARGET, "target.test", "canonical"),
        identity(alias, "target.test", "canonical"),
    ])
    await insert(clickhouse, "post_reference_evidence", [
        evidence(
            domain="reddit.com", url=root_url, source="reddit", kind="post",
            reference_id="",
        ),
        evidence(
            alias, domain="reddit.com", post_id="456", url=sibling_url,
            source="reddit",
        ),
        evidence(
            alias, domain="reddit.com", post_id="789", url=other_url,
            source="reddit",
        ),
    ])

    scoped = await client.get(
        "/api/reader/connections?domain=reddit.com&id=123&source=reddit"
        "&community=kernel",
    )
    everywhere = await client.get(
        "/api/reader/connections?domain=reddit.com&id=123",
    )
    body = scoped.json()
    [sibling] = body["related"]

    assert body["context"] == {"source": "reddit", "community": "kernel"}
    assert body["related_count"] == 1
    assert sibling["direction"] == "related"
    assert sibling["origin"]["id"] == "456"
    assert sibling["target"]["post"]["id"] == "canonical"
    assert sibling["target"]["url"] == alias
    assert sibling["evidence"]["matched_url"] == alias + "#exact"
    assert body["outgoing_count"] == 1
    assert everywhere.json()["context"] == {}
    assert {item["origin"]["id"] for item in everywhere.json()["related"]} == {
        "456", "789",
    }


async def test_exact_lookup_finds_evidence_beyond_the_neighbourhood_limit(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_root(clickhouse)
    await insert(clickhouse, "post_reference_evidence", [
        evidence(
            f"https://target.test/{index}", reference_id=f"c{index}",
            observed_at=COLLECTED_AT,
        )
        for index in range(101)
    ] + [evidence()])

    listing = await client.get(
        "/api/reader/connections?domain=origin.test&id=123",
    )
    response = await client.get("/api/reader/connection?" + urlencode({
        "domain": "origin.test", "id": "123",
        "target": TARGET + "#fragment", "kind": "comment",
        "reference_id": "comment-2", "observed_at": STAMP.isoformat(),
    }))
    missing = await client.get("/api/reader/connection?" + urlencode({
        "domain": "origin.test", "id": "123", "target": TARGET,
        "kind": "comment", "reference_id": "another",
    }))

    assert listing.json()["outgoing_count"] == 102
    assert len(listing.json()["outgoing"]) == 100
    assert TARGET not in {
        item["target"]["url"] for item in listing.json()["outgoing"]
    }
    assert response.status_code == 200
    assert response.json()["target"]["url"] == TARGET
    assert response.json()["evidence"]["observed_at"] == STAMP.isoformat()
    assert missing.status_code == 404
