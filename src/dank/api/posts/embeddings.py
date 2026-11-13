"""SearchEmbeddings: the saved vector for a search, or the model's."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable
from typing import Protocol

from dank.api.posts.model import Embedding
from dank.storage.clickhouse import ClickHouseClient

CACHED_EMBEDDING_SQL = (
    "SELECT embedding FROM web_embedding_cache FINAL "
    "WHERE model_name = %(model_name)s "
    "AND search_text = %(search_text)s "
    "ORDER BY created_at DESC "
    "LIMIT 1"
)


class Embedder(Protocol):
    @property
    def model_name(self) -> str: ...

    def embed_texts(self, items: list[str]) -> list[Embedding]: ...


class CachedSearchEmbeddings:
    def __init__(
        self, client: ClickHouseClient, embedder: Callable[[], Embedder],
    ) -> None:
        self._client = client
        self._embedder = embedder

    async def embed(self, text: str) -> Embedding | None:
        embedder = self._embedder()
        result = await self._client.fetch_json(CACHED_EMBEDDING_SQL, {
            "model_name": embedder.model_name, "search_text": text,
        })

        if result.rows:
            return tuple(float(item) for item in result.rows[0]["embedding"])

        # Loading the model and encoding both block for seconds.
        embeddings = await asyncio.to_thread(embedder.embed_texts, [text])

        if not embeddings:
            return None

        embedding = embeddings[0]
        await self._client.insert_rows("web_embedding_cache", [{
            "model_name": embedder.model_name,
            "search_text": text,
            "embedding": [float(value) for value in embedding],
            "created_at": dt.datetime.now(dt.UTC),
        }])

        return embedding
