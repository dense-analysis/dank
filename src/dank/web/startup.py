"""ClickHouse startup work that both web servers run before serving."""

from __future__ import annotations

import pathlib
from collections.abc import Awaitable, Callable

from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_links import ensure_link_storage
from dank.web.reader_reference_index import ensure_reference_storage
from dank.web.reader_youtube import ensure_youtube_storage

REDDIT_SCHEMA = pathlib.Path(__file__).parents[1] / "storage/reddit.sql"

type StartupStep = Callable[[ClickHouseClient], Awaitable[None]]


async def apply_reddit_schema(client: ClickHouseClient) -> None:
    for statement in REDDIT_SCHEMA.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)


STARTUP_STEPS: tuple[StartupStep, ...] = (
    apply_reddit_schema,
    ensure_youtube_storage,
    ensure_link_storage,
    ensure_reference_storage,
)
"""DDL and backfills, in the order they must run."""


async def prepare_clickhouse(client: ClickHouseClient) -> None:
    for step in STARTUP_STEPS:
        await step(client)
