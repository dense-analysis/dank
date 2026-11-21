"""Read processed posts and their enrichment from supplied driver results."""

import datetime as dt
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.clickhouse import ClickHousePostStore
from dank.api.posts.deps import post_store
from dank.api.posts.model import ReaderFilters
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import (
    COLLECTED_AT,
    QueryResults,
    asset,
    post,
    raw_post,
    x_url,
)


@pytest.fixture
def queries(app: FastAPI, memory_path: Path) -> QueryResults:
    results = QueryResults(())
    store = ClickHousePostStore(cast(ClickHouseClient, results), memory_path)
    app.dependency_overrides[post_store] = lambda: store

    return results


async def test_listings_keep_assets_and_failed_versions_with_their_post(
    client: AsyncClient, queries: QueryResults, memory_path: Path,
) -> None:
    image = memory_path / "b.jpg"
    image.touch()
    url = "https://example.com/b.jpg"
    earlier = COLLECTED_AT - dt.timedelta(milliseconds=1)
    queries.responses.extend((
        [post("c.example", "same"), post(
            "b.example", "same", html=f'<img src="{url}"><p>Body</p>',
            updated_at=earlier,
        ), post("a.example", "same")],
        [asset("b.example", "same", url, image)],
        [{"domain": "b.example", "post_id": "same"}],
        [], [], [], [],
    ))
    response = await client.get("/api/reader/posts?limit=2")
    assert response.status_code == 200
    body = response.json()
    first, second = body["posts"]

    assert (first["domain"], second["domain"]) == ("c.example", "b.example")
    assert first["thumbnail"] is None
    assert first["collection_incomplete"] is False
    assert second["thumbnail"] == "/assets/b.jpg"
    assert second["collection_incomplete"] is True
    assert body["next_cursor"]
    params = queries.requests[2][1]
    assert params is not None
    assert params["post_versions"] == [
        ("c.example", "same", round(COLLECTED_AT.timestamp() * 1000)),
        ("b.example", "same", round(earlier.timestamp() * 1000)),
    ]


@pytest.mark.parametrize(("raw", "score", "comments", "flair"), [
    ({"subreddit": "other", "score": 123, "num_comments": 42,
      "link_flair_text": "Discussion"}, 123, 42, "Discussion"),
    ("broken", None, None, None),
    ("[]", None, None, None),
    ('{"score":true}', None, None, None),
    ('{"score":9007199254740992,"num_comments":-1}', None, None, None),
])
async def test_reddit_metadata_uses_the_processed_capture_and_safe_html(
    client: AsyncClient, queries: QueryResults, raw: dict[str, Any] | str,
    score: int | None, comments: int | None, flair: str | None,
) -> None:
    url = "https://www.reddit.com/r/python/comments/abc/title/"
    queries.responses.extend((
        [post("reddit.com", "t3_abc", source="reddit", url=url,
              html="<p>Body</p><script>alert(1)</script>")],
        [], [raw_post("reddit.com", "t3_abc", raw, source="reddit", url=url)],
        [], [], [], [], [],
    ))
    response = await client.get("/api/reader/posts?kind=reddit")
    assert response.status_code == 200
    [item] = response.json()["posts"]

    assert item["reddit"] == {
        "community": "python", "score": score, "comments": comments,
        "flair": flair, "icon_url": None, "video": None, "videos": [],
    }
    assert "<script>" not in item["html"]
    version = round(COLLECTED_AT.timestamp() * 1000)
    assert queries.requests[2][1] == {"reddit_versions": [("t3_abc", version)]}


async def test_authors_keep_examples_and_the_snapshot_across_pages(
    client: AsyncClient, queries: QueryResults,
) -> None:
    alice = {"handle": "alice", "name": "Shared", "count": 2}
    bob = {"handle": "bob", "name": "Shared", "count": 1}
    totals = [{"total": 2, "unattributed_posts": 1}]
    examples = [{
        **post("x.com", item, source="x", url=x_url("alice", item)),
        "handle": "alice",
    } for item in ("11", "12")]
    queries.responses.extend((
        [alice, bob], totals, examples, [], [], [], [],
        [bob], totals, [],
    ))
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"
    first = (await client.get(path)).json()
    second = (await client.get(f"{path}&cursor={first['next_cursor']}")).json()

    [first_author] = first["authors"]
    [second_author] = second["authors"]
    assert (first["total"], first["unattributed_posts"]) == (2, 1)
    assert first_author["handle"] == "alice"
    assert first_author["name"] == "alice"
    assert first_author["count"] == 2
    assert [item["id"] for item in first_author["posts"]] == ["11", "12"]
    assert second_author["handle"] == "bob"
    assert second_author["name"] == "Shared"
    assert second["next_cursor"] is None
    params = queries.requests[7][1]
    assert params is not None
    assert params["after_handle"] == "alice"
    assert params["after_count"] == 2
    assert not queries.responses


async def test_lookups_and_listings_keep_driver_identity_and_recommendation(
    memory_path: Path,
) -> None:
    known = post("x.com", "1", source="x", url=x_url("alice", "1"))
    recommendation = {**known, "recommended_at": "2026-10-30 12:00:00",
                      "recommendation_position": 7}
    queries = QueryResults(([known], [], [recommendation]))
    store = ClickHousePostStore(cast(ClickHouseClient, queries), memory_path)

    found = await store.post("x.com", "1")
    assert found is not None
    assert found.url == x_url("alice", "1")
    assert await store.post("x.com", "2") is None
    [item] = await store.listing(
        ReaderFilters(kind="x", x_feed="for_you"), (), None,
        COLLECTED_AT.isoformat(),
    )
    assert item.post.post_id == "1"
    assert item.recommendation == ("2026-10-30 12:00:00", 7)
    assert not queries.responses
