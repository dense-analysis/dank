"""The sources an aiohttp request reads, until DEN-51 removes this server.

FastAPI picks its SourceCatalog in dank.api.sources.deps.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

from aiohttp import web

from dank.api.sources.config_catalog import ConfigSourceCatalog
from dank.api.sources.saved_catalog import SavedSourceCatalog
from dank.api.sources.service import SourceCatalog
from dank.config import Settings, SourceConfig
from dank.db.engine import async_engine, async_sessions
from dank.db.sources import PostgresSourceRepository

SOURCE_CATALOG = web.AppKey("source_catalog", SourceCatalog)


async def saved_sources_context(app: web.Application) -> AsyncIterator[None]:
    """Open the sources table for the app's lifetime, if the switch is on."""
    settings: Settings = app["state"].settings

    if not settings.sources_in_database:
        yield

        return

    # The engine connects on first use, so a Postgres outage fails the
    # requests that need sources rather than startup.
    engine = async_engine(settings.postgres)
    app[SOURCE_CATALOG] = SavedSourceCatalog(
        PostgresSourceRepository(async_sessions(engine)),
    )

    try:
        yield
    finally:
        await engine.dispose()


def source_catalog(request: web.Request) -> SourceCatalog:
    settings: Settings = request.app["state"].settings

    if not settings.sources_in_database:
        return ConfigSourceCatalog(settings)

    # No fallback: with the switch on, config.toml's sources are ignored,
    # so a missing catalog fails the request.
    return request.app[SOURCE_CATALOG]


async def current_sources(request: web.Request) -> tuple[SourceConfig, ...]:
    """Every source and target, read again for each request."""
    return await source_catalog(request).sources()


async def current_settings(request: web.Request) -> Settings:
    """Settings whose sources are the ones in use, for code that takes both."""
    settings: Settings = request.app["state"].settings

    return settings._replace(sources=await current_sources(request))
