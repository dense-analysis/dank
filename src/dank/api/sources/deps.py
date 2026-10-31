"""Wires SourceService to its adapters. Tests override the adapters here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep, SettingsDep
from dank.api.sources.clickhouse import ClickHouseCollectionStats
from dank.api.sources.config_catalog import ConfigSourceCatalog
from dank.api.sources.service import (
    CollectionStats,
    SourceCatalog,
    SourceService,
)


def source_catalog(settings: SettingsDep) -> SourceCatalog:
    return ConfigSourceCatalog(settings)


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
