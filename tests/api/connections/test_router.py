"""GET /api/reader/connections and /connection: HTTP in, JSON out."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import urlencode

import pytest
from httpx import AsyncClient

from dank.api.connections.model import ConnectionKey, ConnectionScope
from tests.api.connections.fakes import FakeConnectionStore

STAMP = dt.datetime(2026, 10, 17, 12, microsecond=123000, tzinfo=dt.UTC)
ROOT = {
    "domain": "origin.test", "id": "123", "url": "https://origin.test/post",
    "title": "The root", "source": "rss",
}


def connection(direction: str = "outgoing") -> dict[str, Any]:
    return {
        "id": "abc", "direction": direction, "origin": ROOT,
        "target": {
            "url": "https://target.test/article", "post": None,
            "title": "https://target.test/article", "youtube_item_id": None,
        },
        "evidence": {
            "domain": "origin.test", "post_id": "123", "kind": "comment",
            "reference_id": "comment-2",
            "reference_url": "https://origin.test/post#comment-2",
            "author": "Reader", "matched_url": "https://target.test/article",
            "excerpt": "Read the evidence", "observed_at": STAMP.isoformat(),
        },
    }


def neighbourhood(context: dict[str, str]) -> dict[str, Any]:
    return {
        "root": ROOT, "incoming": [connection("incoming")],
        "outgoing": [connection()], "related": [connection("related")],
        "context": context, "related_count": 1, "incoming_count": 135,
        "outgoing_count": 1, "limit": 100, "coverage": "saved_evidence",
    }


async def test_connections_return_each_direction_with_its_counts(
    client: AsyncClient, store: FakeConnectionStore,
) -> None:
    body = neighbourhood({})
    store.neighbourhoods[ConnectionScope("origin.test", "123")] = body

    response = await client.get(
        "/api/reader/connections?domain=+origin.test+&id=123",
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == body


async def test_connections_context_keeps_only_what_was_asked_for(
    client: AsyncClient, store: FakeConnectionStore,
) -> None:
    store.neighbourhoods[
        ConnectionScope("a", "b", "reddit", "kernel")
    ] = neighbourhood({"source": "reddit", "community": "kernel"})
    store.neighbourhoods[
        ConnectionScope("a", "b", "x")
    ] = neighbourhood({"source": "x"})

    both = await client.get(
        "/api/reader/connections?domain=a&id=b&source=reddit&community=Kernel",
    )
    source = await client.get(
        "/api/reader/connections?domain=a&id=b&source=www.Twitter.com",
    )

    assert both.json()["context"] == {
        "source": "reddit", "community": "kernel",
    }
    assert source.json()["context"] == {"source": "x"}


async def test_unknown_root_is_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/reader/connections?domain=a&id=b")

    assert (response.status_code, response.json()) == (404, {
        "error": "Article not found", "code": "not_found", "fields": [],
    })


@pytest.mark.parametrize("query", [
    "", "domain=origin.test", "id=123", "domain=a&id=b&unknown=x",
    "domain=a&domain=b&id=c", "domain=a&id=b&limit=100000",
    "domain=a&id=b&source=reddit&source=x", "domain=+&id=b",
    "domain=" + "a" * 2001 + "&id=b",
])
async def test_malformed_connection_queries_are_rejected(
    client: AsyncClient, query: str,
) -> None:
    response = await client.get("/api/reader/connections?" + query)

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"


@pytest.mark.parametrize(("query", "message"), [
    ("domain=a&id=b&source=unknown", "Invalid connection source"),
    ("domain=a&id=b&community=kernel",
     "community requires source=reddit and a community name"),
    ("domain=a&id=b&source=x&community=kernel",
     "community requires source=reddit and a community name"),
    ("domain=a&id=b&source=reddit&community=bad/name",
     "community requires source=reddit and a community name"),
])
async def test_invalid_connection_context_keeps_its_wording(
    client: AsyncClient, query: str, message: str,
) -> None:
    response = await client.get("/api/reader/connections?" + query)

    assert (response.status_code, response.json()) == (400, {
        "error": message, "code": "invalid_input", "fields": [],
    })


def lookup(**replacement: str) -> str:
    return "/api/reader/connection?" + urlencode({
        "domain": "origin.test", "id": "123",
        "target": "https://target.test/article#exact", "kind": "comment",
        "reference_id": "comment-2", "observed_at": STAMP.isoformat(),
        **replacement,
    })


async def test_exact_lookup_normalizes_its_target_and_timestamp(
    client: AsyncClient, store: FakeConnectionStore,
) -> None:
    key = ConnectionKey(
        "origin.test", "123", "https://target.test/article", "comment",
        "comment-2", STAMP.isoformat(),
    )
    store.connections[key] = connection()

    response = await client.get(lookup())

    assert response.status_code == 200
    assert response.json() == connection()


async def test_exact_lookup_without_a_timestamp_finds_the_latest(
    client: AsyncClient, store: FakeConnectionStore,
) -> None:
    key = ConnectionKey(
        "origin.test", "123", "https://target.test/article", "post", "",
    )
    store.connections[key] = connection()

    response = await client.get("/api/reader/connection?" + urlencode({
        "domain": "origin.test", "id": "123",
        "target": "https://target.test/article", "kind": "post",
    }))

    assert response.status_code == 200


async def test_missing_evidence_is_not_found(client: AsyncClient) -> None:
    response = await client.get(lookup())

    assert (response.status_code, response.json()) == (404, {
        "error": "Saved evidence not found", "code": "not_found", "fields": [],
    })


@pytest.mark.parametrize("replacement", [
    {"target": "javascript:alert(1)"},
    {"target": "https://a.test/" + "b" * 8192},
    {"reference_id": ""}, {"observed_at": "not a date"}, {"kind": "post"},
    {"reference_id": "r" * 2001},
])
async def test_exact_lookup_validates_identity(
    client: AsyncClient, replacement: dict[str, str],
) -> None:
    response = await client.get(lookup(**replacement))

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_input"


@pytest.mark.parametrize("query", [
    "kind=quote", "kind=comment&kind=post", "domain=", "source=reddit",
])
async def test_malformed_lookups_are_rejected(
    client: AsyncClient, query: str,
) -> None:
    key = query.split("=")[0]
    base = {
        "domain": "a", "id": "b", "target": "https://t.test", "kind": "post",
    }
    base.pop(key, None)

    response = await client.get(
        "/api/reader/connection?" + urlencode(base) + "&" + query,
    )

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"


async def test_openapi_describes_connections(client: AsyncClient) -> None:
    schemas = (await client.get("/api/openapi.json")).json()["components"][
        "schemas"
    ]

    assert schemas["ConnectionsResponse"]["additionalProperties"] is False
    assert schemas["ConnectionOut"]["properties"]["direction"]["enum"] == [
        "incoming", "outgoing", "related",
    ]
