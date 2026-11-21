"""Provides the source loader and collection statistics."""

from typing import Annotated

from fastapi import Depends, Request

from dank.api.deps import ResourcesDep, SettingsDep, resources
from dank.api.sources.clickhouse import ClickHouseCollectionStats
from dank.api.sources.service import CollectionStats, LoadSources
from dank.db.sources import PostgresSourceRepository
from dank.sources.model import CompiledSources, compile_sources
from dank.sources.service import SourceRepository


def saved_sources(
    request: Request, settings: SettingsDep,
) -> SourceRepository | None:
    """The sources table, when `sources_in_database` says to read it."""
    # Asks for the app's resources only when the switch is on, so reading
    # config.toml's sources never needs Postgres.
    if not settings.sources_in_database:
        return None

    return PostgresSourceRepository(resources(request).postgres)


def source_catalog(
    settings: SettingsDep,
    saved: Annotated[SourceRepository | None, Depends(saved_sources)],
) -> LoadSources:
    """Read saved sources only when a use case asks for them."""

    async def load() -> CompiledSources:
        if saved is None:
            # Every TOML source is enabled.
            return CompiledSources(settings.sources, settings.sources)

        return compile_sources(await saved.list())

    return load


def collection_stats(resources: ResourcesDep) -> CollectionStats:
    return ClickHouseCollectionStats(
        resources.clickhouse, resources.assets_dir,
    )


LoadSourcesDep = Annotated[LoadSources, Depends(source_catalog)]
CollectionStatsDep = Annotated[CollectionStats, Depends(collection_stats)]
