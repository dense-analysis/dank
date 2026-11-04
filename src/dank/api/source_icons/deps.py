"""Wires SourceIconService to its adapters. Tests override them here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.api.source_icons.clickhouse import ClickHouseCollectedDomains
from dank.api.source_icons.service import (
    CollectedDomains,
    IconCache,
    SourceIconService,
)
from dank.api.sources.deps import source_catalog
from dank.api.sources.service import SourceCatalog


def collected_domains(resources: ResourcesDep) -> CollectedDomains:
    return ClickHouseCollectedDomains(resources.clickhouse)


def icon_cache(resources: ResourcesDep) -> IconCache:
    return resources.source_icons


def source_icon_service(
    catalog: Annotated[SourceCatalog, Depends(source_catalog)],
    collected: Annotated[CollectedDomains, Depends(collected_domains)],
    icons: Annotated[IconCache, Depends(icon_cache)],
) -> SourceIconService:
    return SourceIconService(catalog, collected, icons)


SourceIconServiceDep = Annotated[
    SourceIconService, Depends(source_icon_service),
]
