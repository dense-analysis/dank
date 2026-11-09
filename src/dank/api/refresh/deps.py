"""Wires RefreshService to its adapters. Tests override the adapters here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep, SettingsDep
from dank.api.refresh.clickhouse import ClickHouseRefreshTargets
from dank.api.refresh.service import (
    RefreshQueue,
    RefreshService,
    RefreshTargets,
)
from dank.api.sources.deps import source_catalog
from dank.api.sources.service import SourceCatalog


def refresh_targets(resources: ResourcesDep) -> RefreshTargets:
    return ClickHouseRefreshTargets(resources.clickhouse)


def refresh_queue(resources: ResourcesDep) -> RefreshQueue:
    return resources.refresh


def refresh_service(
    settings: SettingsDep,
    catalog: Annotated[SourceCatalog, Depends(source_catalog)],
    targets: Annotated[RefreshTargets, Depends(refresh_targets)],
    queue: Annotated[RefreshQueue, Depends(refresh_queue)],
) -> RefreshService:
    return RefreshService(settings, catalog, targets, queue)


RefreshServiceDep = Annotated[RefreshService, Depends(refresh_service)]
