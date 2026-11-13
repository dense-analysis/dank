"""A search saved before never loads the embedding model."""

from typing import Any, cast

from dank.api.posts.embeddings import CachedSearchEmbeddings
from dank.storage.clickhouse import ClickHouseClient, QueryResult


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
