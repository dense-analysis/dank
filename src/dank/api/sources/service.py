"""Source use cases, and the ports they need from the outside world.

Each port is a Protocol owned by the code that calls it. Adapters satisfy
it by shape and never inherit from it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
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
from dank.sources.model import CompiledSources

type LoadSources = Callable[[], Awaitable[CompiledSources]]


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


async def source_overview(
    load_sources: LoadSources, stats: CollectionStats,
) -> Overview:
    configured = (await load_sources()).known
    counts, blog_count = await asyncio.gather(
        stats.domain_counts(),
        stats.external_blog_count(),
    )
    sources = await asyncio.gather(*(
        _with_activity(source, configured, stats)
        for source in summarize(configured, counts)
    ))

    return Overview(
        sources=tuple(sources),
        total_posts=sum(counts.values()),
        blog_external_count=blog_count,
    )


async def _with_activity(
    source: SourceSummary,
    configured: tuple[SourceConfig, ...],
    stats: CollectionStats,
) -> SourceSummary:
    match source.kind:
        case "x":
            activity = await stats.x_activity(
                source.domain, x_accounts(configured, source.domain),
            )
        case "reddit":
            activity = await stats.reddit_activity(
                reddit_communities(configured),
            )
        case _:
            return source

    return source._replace(activity=activity)
