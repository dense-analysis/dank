"""Provides the icon cache and the collected-domain lookup."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.api.source_icons.clickhouse import ClickHouseCollectedDomains
from dank.api.source_icons.service import CollectedDomains, IconCache


def collected_domains(resources: ResourcesDep) -> CollectedDomains:
    return ClickHouseCollectedDomains(resources.clickhouse)


def icon_cache(resources: ResourcesDep) -> IconCache:
    return resources.source_icons


CollectedDomainsDep = Annotated[CollectedDomains, Depends(collected_domains)]
IconCacheDep = Annotated[IconCache, Depends(icon_cache)]
