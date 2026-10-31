"""Source use cases, and the ports they need from the outside world.

Each port is a Protocol owned by the code that calls it. Adapters satisfy
it by shape and never inherit from it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Protocol

from dank.api.sources.model import (
    Overview,
    RedditActivity,
    SourceSummary,
    XActivity,
    reddit_communities,
    summarize,
    x_accounts,
)
from dank.config import SourceConfig


class SourceCatalog(Protocol):
    """Configured sources: config.toml today, Postgres from DEN-22."""

    async def sources(self) -> tuple[SourceConfig, ...]: ...


class CollectionStats(Protocol):
    """Read-only facts about what has been collected."""

    async def domain_counts(self) -> Mapping[str, int]: ...

    async def external_blog_count(self) -> int: ...

    async def x_activity(
        self, domain: str, accounts: tuple[str, ...],
    ) -> XActivity: ...

    async def reddit_activity(
        self, communities: Mapping[str, str],
    ) -> RedditActivity: ...


class SourceService:
    def __init__(
        self, catalog: SourceCatalog, stats: CollectionStats,
    ) -> None:
        self._catalog = catalog
        self._stats = stats

    async def overview(self) -> Overview:
        configured = await self._catalog.sources()
        counts, blog_count = await asyncio.gather(
            self._stats.domain_counts(),
            self._stats.external_blog_count(),
        )
        sources = await asyncio.gather(*(
            self._with_activity(source, configured)
            for source in summarize(configured, counts)
        ))

        return Overview(
            sources=tuple(sources),
            total_posts=sum(counts.values()),
            blog_external_count=blog_count,
        )

    async def _with_activity(
        self, source: SourceSummary, configured: tuple[SourceConfig, ...],
    ) -> SourceSummary:
        match source.kind:
            case "x":
                activity = await self._stats.x_activity(
                    source.domain, x_accounts(configured, source.domain),
                )
            case "reddit":
                activity = await self._stats.reddit_activity(
                    reddit_communities(configured),
                )
            case _:
                return source

        return source._replace(activity=activity)
