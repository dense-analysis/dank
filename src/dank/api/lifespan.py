"""Opens long-lived resources at startup and closes them at shutdown."""

from __future__ import annotations

import pathlib
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import NamedTuple

from fastapi import FastAPI

from dank.config import Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from dank.web.reader_refresh import RefreshManager, running_refresh
from dank.web.startup import prepare_clickhouse

type Lifespan = Callable[
    [FastAPI], AbstractAsyncContextManager[dict[str, Resources]],
]


class Resources(NamedTuple):
    """Everything routes reach through dependencies, opened once."""

    settings: Settings
    clickhouse: ClickHouseClient
    refresh: RefreshManager
    assets_dir: pathlib.Path
    page_size: int


def lifespan(settings: Settings, *, page_size: int) -> Lifespan:
    @asynccontextmanager
    async def run(app: FastAPI) -> AsyncGenerator[dict[str, Resources]]:
        assets_dir = assets_path(settings)
        # Keep assets discovered after the first startup readable.
        assets_dir.mkdir(parents=True, exist_ok=True)

        async with ClickHouseClient(settings.clickhouse) as clickhouse:
            # The same DDL and backfills as the aiohttp server, in order.
            await prepare_clickhouse(clickhouse)

            # Shutdown waits here for refresh workers (about 95 s).
            async with running_refresh(settings) as refresh:
                yield {"resources": Resources(
                    settings=settings,
                    clickhouse=clickhouse,
                    refresh=refresh,
                    assets_dir=assets_dir,
                    page_size=page_size,
                )}

    return run
