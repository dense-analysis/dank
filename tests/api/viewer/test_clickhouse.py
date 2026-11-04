"""ClickHouseViewerStore against a real, throwaway database."""

import datetime as dt
import pathlib

import pytest

from dank.api.posts.model import Embedding
from dank.api.viewer.clickhouse import ClickHouseViewerStore
from dank.api.viewer.model import IndexFilters, index_filters
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import COLLECTED_AT, asset, insert, post

pytestmark = pytest.mark.clickhouse

VECTOR = (1.0, 0.0)
OPPOSITE = (-1.0, 0.0)


class Embeddings:
    def __init__(self, vector: Embedding | None = VECTOR) -> None:
        self.vector = vector

    async def embed(self, text: str) -> Embedding | None:
        return self.vector


def at(hours: int) -> dt.datetime:
    return COLLECTED_AT - dt.timedelta(hours=hours)


def filters(**query: str) -> IndexFilters:
    return index_filters(query, page_size=50)


@pytest.fixture
async def store(clickhouse: ClickHouseClient) -> ClickHouseViewerStore:
    await insert(clickhouse, "posts", [
        post("a.example", "3", created_at=at(0), author="Ada Lovelace"),
        post("a.example", "2", created_at=at(1), author="Grace"),
        post("b.example", "1", created_at=at(48), author="ada"),
    ])

    return ClickHouseViewerStore(clickhouse, Embeddings())


async def test_posts_list_newest_first_up_to_the_limit(
    store: ClickHouseViewerStore,
) -> None:
    every = await store.posts(filters(), None)
    limited = await store.posts(filters(limit="2"), None)

    assert [row.post_id for row in every] == ["3", "2", "1"]
    assert [row.post_id for row in limited] == ["3", "2"]


async def test_the_list_filters_by_domain_account_and_date(
    store: ClickHouseViewerStore,
) -> None:
    domain = await store.posts(filters(domain="a.example"), None)
    # Part of a name, in any case.
    account = await store.posts(filters(account="ADA"), None)
    recent = await store.posts(filters(), at(2))

    assert [row.post_id for row in domain] == ["3", "2"]
    assert [row.post_id for row in account] == ["3", "1"]
    assert [row.post_id for row in recent] == ["3", "2"]


async def test_cursors_page_both_ways_keeping_newest_first(
    store: ClickHouseViewerStore,
) -> None:
    cursor = {
        "cursor_created_at": at(1).isoformat(), "cursor_post_id": "2",
    }

    older = await store.posts(filters(**cursor), None)
    newer = await store.posts(
        filters(**cursor, cursor_direction="prev"), None,
    )

    assert [row.post_id for row in older] == ["1"]
    assert [row.post_id for row in newer] == ["3"]


async def test_neighbours_respect_the_filters(
    store: ClickHouseViewerStore,
) -> None:
    posts = await store.posts(filters(), None)
    newest, middle, oldest = posts

    assert not await store.has_newer(newest, filters(), None)
    assert await store.has_older(newest, filters(), None)
    assert await store.has_newer(oldest, filters(), None)
    assert not await store.has_older(oldest, filters(), None)
    assert not await store.has_older(middle, filters(domain="a.example"), None)
    assert not await store.has_older(middle, filters(), at(2))
    assert not await store.has_newer(middle, filters(account="Grace"), None)


async def test_search_ranks_filters_and_drops_distant_posts(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "near", embedding=VECTOR, author="Ada"),
        post("b.example", "other-domain", embedding=VECTOR),
        post("a.example", "far", embedding=OPPOSITE),
        post("a.example", "unprocessed"),
        post("a.example", "old", embedding=VECTOR, created_at=at(240)),
    ])
    store = ClickHouseViewerStore(clickhouse, Embeddings())

    every = await store.search(filters(q="space"), None)
    domain = await store.search(filters(q="space", domain="b.example"), None)
    account = await store.search(filters(q="space", account="ada"), None)
    recent = await store.search(filters(q="space"), at(24))

    assert {row.post_id for row in every} == {"near", "other-domain", "old"}
    # Fresher posts come first among equal matches.
    assert every[-1].post_id == "old"
    assert [row.post_id for row in domain] == ["other-domain"]
    assert [row.post_id for row in account] == ["near"]
    assert {row.post_id for row in recent} == {"near", "other-domain"}


async def test_search_without_an_embedding_finds_nothing(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "near", embedding=VECTOR),
    ])
    store = ClickHouseViewerStore(clickhouse, Embeddings(None))

    assert await store.search(filters(q="space"), None) == []


async def test_a_post_is_found_by_domain_and_id(
    store: ClickHouseViewerStore,
) -> None:
    found = await store.post("a.example", "2")

    assert found is not None
    assert found.author == "Grace"
    assert await store.post("b.example", "2") is None


async def test_assets_are_grouped_by_post(
    clickhouse: ClickHouseClient, store: ClickHouseViewerStore,
) -> None:
    data = pathlib.Path("/data")
    await insert(clickhouse, "assets", [
        asset("a.example", "3", "https://cdn/1", data / "1.png"),
        asset("a.example", "3", "https://cdn/2", data / "2.png"),
        asset("b.example", "1", "https://cdn/3", data / "3.png"),
    ])

    found = await store.assets(["3", "1", "2"])

    assert {row.url for row in found["3"]} == {"https://cdn/1", "https://cdn/2"}
    assert [row.local_path for row in found["1"]] == ["/data/3.png"]
    assert "2" not in found
    assert await store.assets([]) == {}
