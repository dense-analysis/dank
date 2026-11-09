"""Wires SourceService to its adapters. Tests override the adapters here."""

from typing import Annotated

from fastapi import Depends, Request

from dank.api.deps import ResourcesDep, SettingsDep, resources
from dank.api.sources.clickhouse import ClickHouseCollectionStats
from dank.api.sources.config_catalog import ConfigSourceCatalog
from dank.api.sources.saved_catalog import SavedSourceCatalog
from dank.api.sources.service import (
    CollectionStats,
    SourceCatalog,
    SourceService,
)
from dank.db.sources import PostgresSourceRepository
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
) -> SourceCatalog:
    # One place or the other, never both: with the switch on, config.toml's
    # sources are ignored even when the table is empty.
    if saved is None:
        return ConfigSourceCatalog(settings)

    return SavedSourceCatalog(saved)


def collection_stats(resources: ResourcesDep) -> CollectionStats:
    return ClickHouseCollectionStats(
        resources.clickhouse, resources.assets_dir,
    )


def source_service(
    catalog: Annotated[SourceCatalog, Depends(source_catalog)],
    stats: Annotated[CollectionStats, Depends(collection_stats)],
) -> SourceService:
    return SourceService(catalog, stats)


SourceServiceDep = Annotated[SourceService, Depends(source_service)]
