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


def refresh_targets(resources: ResourcesDep) -> RefreshTargets:
    return ClickHouseRefreshTargets(resources.clickhouse)


def refresh_queue(resources: ResourcesDep) -> RefreshQueue:
    return resources.refresh


def refresh_service(
    settings: SettingsDep,
    targets: Annotated[RefreshTargets, Depends(refresh_targets)],
    queue: Annotated[RefreshQueue, Depends(refresh_queue)],
) -> RefreshService:
    return RefreshService(settings, targets, queue)


RefreshServiceDep = Annotated[RefreshService, Depends(refresh_service)]
