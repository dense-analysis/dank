"""Opens long-lived resources at startup and closes them at shutdown."""

from __future__ import annotations

import pathlib
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import NamedTuple

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dank.api.youtube.collector import CollectionGate, collection_gate
from dank.config import Settings
from dank.db.engine import async_engine, async_sessions
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from dank.web.reader_refresh import RefreshManager, running_refresh
from dank.web.reader_source_icons import SourceIconCache
from dank.web.startup import prepare_clickhouse, prepare_postgres

type Lifespan = Callable[
    [FastAPI], AbstractAsyncContextManager[dict[str, Resources]],
]


class Resources(NamedTuple):
    """Everything routes reach through dependencies, opened once."""

    settings: Settings
    clickhouse: ClickHouseClient
    postgres: async_sessionmaker[AsyncSession]
    refresh: RefreshManager
    source_icons: SourceIconCache
    assets_dir: pathlib.Path
    page_size: int
    metadata_gate: CollectionGate
    transcript_gate: CollectionGate


def lifespan(settings: Settings, *, page_size: int) -> Lifespan:
    @asynccontextmanager
    async def run(app: FastAPI) -> AsyncGenerator[dict[str, Resources]]:
        assets_dir = assets_path(settings)
        # Keep assets discovered after the first startup readable.
        assets_dir.mkdir(parents=True, exist_ok=True)
        # First, so a bad schema stops startup before any long backfills.
        await prepare_postgres(settings.postgres)
        # Connects on first use, so a disabled Postgres costs nothing.
        postgres = async_engine(settings.postgres)

        try:
            async with ClickHouseClient(settings.clickhouse) as clickhouse:
                # The same DDL and backfills as the aiohttp server, in order.
                await prepare_clickhouse(clickhouse)

                # Shutdown waits here for refresh workers (about 95 s).
                async with running_refresh(settings) as refresh:
                    yield {"resources": Resources(
                        settings=settings,
                        clickhouse=clickhouse,
                        postgres=async_sessions(postgres),
                        refresh=refresh,
                        source_icons=SourceIconCache(assets_dir),
                        assets_dir=assets_dir,
                        page_size=page_size,
                        metadata_gate=collection_gate(settings.scrape),
                        transcript_gate=collection_gate(settings.scrape),
                    )}
        finally:
            await postgres.dispose()

    return run
