"""A search saved before never loads the embedding model."""

import threading
from typing import Any, cast

from dank.api.posts.embeddings import CachedSearchEmbeddings
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from tests.api.clickhouse import QueryResults


class Unloadable:
    model_name = "model-a"

    def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
        raise AssertionError("A cached search must not load the model")


class CacheHit:
    async def fetch_json(
        self, query: str, params: dict[str, Any],
    ) -> QueryResult:
        assert params == {"model_name": "model-a", "search_text": "hello"}

        return QueryResult([{"embedding": [0.4, 0.6]}])


async def test_a_cached_search_uses_the_saved_vector() -> None:
    embeddings = CachedSearchEmbeddings(
        cast(ClickHouseClient, CacheHit()), Unloadable,
    )

    assert await embeddings.embed("hello") == (0.4, 0.6)


MODEL = "test-model"


class FakeEmbedder:
    """Stands in for the model, which tests can't download."""

    model_name = MODEL

    def __init__(self, vectors: list[tuple[float, ...]]) -> None:
        self._vectors = vectors
        self.threads: list[threading.Thread] = []

    def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
        self.threads.append(threading.current_thread())

        return self._vectors


async def test_a_new_search_encodes_off_thread_and_saves_the_vector() -> None:
    queries = QueryResults(([],))
    embedder = FakeEmbedder([(0.5, 0.25)])
    embeddings = CachedSearchEmbeddings(
        cast(ClickHouseClient, queries), lambda: embedder,
    )

    assert await embeddings.embed("hello") == (0.5, 0.25)
    assert embedder.threads[0] is not threading.main_thread()
    [(table, [row])] = queries.writes
    assert table == "web_embedding_cache"
    assert row["search_text"] == "hello"
    assert row["model_name"] == MODEL
    assert row["embedding"] == [0.5, 0.25]


async def test_empty_model_output_does_not_touch_the_cache() -> None:
    queries = QueryResults(([],))
    embedder = FakeEmbedder([])
    embeddings = CachedSearchEmbeddings(
        cast(ClickHouseClient, queries), lambda: embedder,
    )

    assert await embeddings.embed("hello") is None
    assert not queries.writes
