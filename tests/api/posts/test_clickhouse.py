"""ClickHousePostStore and search embeddings against a real database."""

from __future__ import annotations

import datetime as dt
import pathlib
import threading
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.clickhouse import ClickHousePostStore
from dank.api.posts.deps import post_store
from dank.api.posts.embeddings import CachedSearchEmbeddings
from dank.api.posts.model import ReaderFilters, Recommendation
from dank.config import Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.assets import assets_path
from tests.api.clickhouse import COLLECTED_AT, insert, post, raw_post, x_url

pytestmark = pytest.mark.clickhouse

MODEL = "test-model"
FOR_YOU = "for_you"
SNAPSHOT = COLLECTED_AT.isoformat()


class FakeEmbedder:
    """Stands in for the model, which tests can't download."""

    model_name = MODEL

    def __init__(self, vectors: list[tuple[float, ...]]) -> None:
        self._vectors = vectors
        self.threads: list[threading.Thread] = []

    def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
        self.threads.append(threading.current_thread())

        return self._vectors


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    path = assets_path(settings)
    path.mkdir(parents=True, exist_ok=True)

    return path


@pytest.fixture
def store(
    clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> ClickHousePostStore:
    return ClickHousePostStore(clickhouse, assets)


@pytest.fixture
def app(app: FastAPI, store: ClickHousePostStore) -> FastAPI:
    app.dependency_overrides[post_store] = lambda: store

    return app


def home_post(
    post_id: str,
    handle: str,
    position: int,
    *,
    author: str = "",
    scraped_at: dt.datetime = COLLECTED_AT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    url = x_url(handle, post_id)

    return (
        post(
            "x.com", post_id, url=url, source="x", author=author or handle,
            updated_at=scraped_at,
        ),
        raw_post(
            "x.com", post_id,
            {"dank_x_home": {"feed": FOR_YOU, "position": position}},
            url=url, source="x", scraped_at=scraped_at,
        ),
    )


async def seed_home(
    client: ClickHouseClient, *rows: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    await insert(client, "posts", [row[0] for row in rows])
    await insert(client, "raw_posts", [row[1] for row in rows])


async def test_listing_returns_one_more_than_the_limit_in_order(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "1"), post("b.example", "2"), post("c.example", "3"),
    ])

    listed = await store.listing(ReaderFilters(limit=1), (), None, SNAPSHOT)

    assert [item.post.domain for item in listed] == ["c.example", "b.example"]
    assert all(item.recommendation is None for item in listed)


async def test_listing_applies_tags_from_the_configured_sources(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "1"), post("b.example", "2"),
    ])
    sources = (SourceConfig("a.example", (), tags=("politics",)),)

    listed = await store.listing(
        ReaderFilters(tags=("politics",)), sources, None, SNAPSHOT,
    )
    nothing = await store.listing(
        ReaderFilters(tags=("politics",)), (), None, SNAPSHOT,
    )

    assert [item.post.domain for item in listed] == ["a.example"]
    assert nothing == []


async def test_for_you_listing_reports_where_each_post_appeared(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await seed_home(clickhouse, home_post("2", "bob", 7))
    filters = ReaderFilters(kind="x", x_feed=FOR_YOU)

    [item] = await store.listing(filters, (), None, SNAPSHOT)
    before = await store.listing(
        filters, (), None, (COLLECTED_AT - dt.timedelta(hours=1)).isoformat(),
    )

    assert item.post.post_id == "2"
    assert item.recommendation == Recommendation("2026-10-30 12:00:00", 7)
    assert before == []


async def test_post_finds_one_post_or_nothing(
    clickhouse: ClickHouseClient, store: ClickHousePostStore,
) -> None:
    await insert(clickhouse, "posts", [post("a.example", "1", title="One")])

    found = await store.post("a.example", "1")

    assert found is not None
    assert found.title == "One"
    assert await store.post("a.example", "2") is None


async def test_authors_rank_by_count_and_disambiguate_shared_names(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse,
        *(home_post(str(10 + n), "alice", n, author="Shared") for n in (1, 2)),
        home_post("20", "bob", 3, author="Shared"),
        home_post("30", "i", 4),
    )
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"

    first = (await client.get(path)).json()
    second = (await client.get(f"{path}&cursor={first['next_cursor']}")).json()
    [alice] = first["authors"]
    [bob] = second["authors"]

    assert (first["total"], first["unattributed_posts"]) == (2, 1)
    assert (alice["handle"], alice["name"], alice["count"]) == (
        "alice", "alice", 2,
    )
    assert {item["id"] for item in alice["posts"]} == {"11", "12"}
    assert (bob["handle"], bob["count"]) == ("bob", 1)
    assert second["next_cursor"] is None


async def test_authors_can_narrow_to_one_account(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, home_post("1", "alice", 1), home_post("2", "bob", 2),
    )

    response = await client.get(
        "/api/reader/x/authors?kind=x&x_feed=for_you&account=@Alice",
    )
    body = response.json()

    assert response.status_code == 200
    assert body["total"] == 1
    assert [item["handle"] for item in body["authors"]] == ["alice"]
    assert [item["id"] for item in body["authors"][0]["posts"]] == ["1"]


async def test_authors_show_only_a_few_example_posts(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, *(home_post(str(n), "alice", n) for n in range(1, 6)),
    )

    response = await client.get("/api/reader/x/authors?kind=x&x_feed=for_you")
    [alice] = response.json()["authors"]

    assert alice["count"] == 5
    assert len(alice["posts"]) == 3
    assert all(item["x"]["handle"] == "alice" for item in alice["posts"])


async def test_authors_cursor_keeps_its_cutoff_for_later_pages(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await seed_home(
        clickhouse, home_post("1", "alice", 1), home_post("2", "bob", 2),
    )
    path = "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1"
    cursor = (await client.get(path)).json()["next_cursor"]
    # Recommended after the first page, so outside its session.
    await seed_home(clickhouse, home_post(
        "3", "carol", 3, scraped_at=COLLECTED_AT + dt.timedelta(minutes=1),
    ))

    response = await client.get(f"{path}&cursor={cursor}")

    assert response.json()["total"] == 2
    assert [item["handle"] for item in response.json()["authors"]] == ["bob"]


async def test_a_cached_embedding_avoids_the_model(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "web_embedding_cache", [{
        "model_name": MODEL, "search_text": "hello", "embedding": [0.5, 0.25],
        "created_at": dt.datetime.now(dt.UTC),
    }])
    embedder = FakeEmbedder([(9.0, 9.0)])

    found = await CachedSearchEmbeddings(
        clickhouse, lambda: embedder,
    ).embed("hello")

    assert found == (0.5, 0.25)


async def test_a_new_search_runs_the_model_in_a_thread_and_saves_the_result(
    clickhouse: ClickHouseClient,
) -> None:
    embedder = FakeEmbedder([(0.5, 0.25)])
    embeddings = CachedSearchEmbeddings(clickhouse, lambda: embedder)

    found = await embeddings.embed("hello")
    embedder_later = FakeEmbedder([(9.0, 9.0)])
    again = await CachedSearchEmbeddings(
        clickhouse, lambda: embedder_later,
    ).embed("hello")

    assert found == (0.5, 0.25)
    assert again == (0.5, 0.25)
    assert embedder.threads[0] is not threading.main_thread()


async def test_a_model_with_no_answer_means_no_embedding(
    clickhouse: ClickHouseClient,
) -> None:
    embedder = FakeEmbedder([])

    assert await CachedSearchEmbeddings(
        clickhouse, lambda: embedder,
    ).embed("hello") is None
