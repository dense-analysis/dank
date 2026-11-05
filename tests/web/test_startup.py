"""Database startup work that both web servers run before serving."""

from __future__ import annotations

from typing import Any, cast

from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_youtube import INDEX_VERSION
from dank.web.startup import prepare_clickhouse


class RecordingClient:
    """Reports every backfill as finished, so only schema work runs."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement: str, *_: Any) -> None:
        self.statements.append(" ".join(statement.split()))

    async def fetch_json(self, *_: Any) -> QueryResult:
        return QueryResult([{"version": INDEX_VERSION}])


async def test_startup_applies_the_category_schema() -> None:
    client = RecordingClient()

    await prepare_clickhouse(cast(ClickHouseClient, client))

    applied = "\n".join(client.statements)

    # The reader selects categories, so it can't wait for a process run.
    assert "ALTER TABLE posts ADD COLUMN IF NOT EXISTS categories " in applied
    assert (
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS embedding_algorithm "
        in applied
    )
    assert "CREATE TABLE IF NOT EXISTS category_training " in applied
