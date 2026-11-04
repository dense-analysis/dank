from __future__ import annotations

from typing import Any, cast

import pytest

from dank.storage.clickhouse import QueryResult
from dank.web import app as web_app


@pytest.mark.asyncio
async def test_search_embedding_uses_cache_when_available(
    monkeypatch: Any,
) -> None:
    class _DummyEmbedder:
        model_name = "model-a"

        def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
            raise AssertionError(
                "Should not recompute embeddings on cache hit",
            )

    class _CacheHit:
        async def fetch_json(
            self, query: str, params: dict[str, str],
        ) -> QueryResult:
            assert params == {"model_name": "model-a", "search_text": "hello"}

            return QueryResult([{"embedding": [0.4, 0.6]}])

    monkeypatch.setattr(
        web_app,
        "get_embedding_model",
        lambda: _DummyEmbedder(),
    )

    result = await web_app._search_embedding(  # pyright: ignore[reportPrivateUsage]
        clickhouse_client=cast(Any, _CacheHit()),
        search_text="hello",
    )

    assert result == (0.4, 0.6)
