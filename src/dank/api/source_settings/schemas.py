"""Wire contract for /api/reader/source-settings.

A write always sends the whole source, so leaving a field out is an error
rather than a way to clear it. Limits here only bound what is parsed; the
rules, with their own limits, are dank.sources.model's.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from pydantic import Field

from dank.api.schemas import ApiModel
from dank.config import (
    HackerNewsFilter,
    HackerNewsSourceConfig,
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
    SourceAdapter,
)
from dank.sources.model import (
    MAX_ADAPTERS,
    MAX_TAGS,
    MAX_TARGETS,
    MAX_TEXT,
    Adapter,
    NewSource,
    Source,
    Target,
)

Text = Annotated[str, Field(max_length=MAX_TEXT)]


class SourceTargetModel(ApiModel):
    """An account handle or feed URL."""

    value: Text
    enabled: bool


Targets = Annotated[
    tuple[SourceTargetModel, ...], Field(max_length=MAX_TARGETS),
]


class SourceAdapterModel(ApiModel):
    kind: SourceAdapter
    # Only RSS adapters have feeds of their own. Null means the source's.
    feeds: Targets | None


class RedditOptions(ApiModel):
    sort: RedditSort
    time_filter: RedditTimeFilter


class HackerNewsOptions(ApiModel):
    filter: HackerNewsFilter


class SourceBody(ApiModel):
    """A whole source, without its ID."""

    enabled: bool
    domain: Text
    accounts: Targets
    feeds: Targets
    tags: Annotated[tuple[Text, ...], Field(max_length=MAX_TAGS)]
    # Null collects with the domain's default adapter.
    adapters: Annotated[
        tuple[SourceAdapterModel, ...], Field(max_length=MAX_ADAPTERS),
    ] | None
    reddit: RedditOptions | None
    hacker_news: HackerNewsOptions | None

    def new_source(self) -> NewSource:
        return NewSource(
            enabled=self.enabled,
            domain=self.domain,
            accounts=_targets(self.accounts),
            feeds=_targets(self.feeds),
            tags=self.tags,
            adapters=None if self.adapters is None else tuple(
                Adapter(
                    adapter.kind,
                    None if adapter.feeds is None else _targets(adapter.feeds),
                )
                for adapter in self.adapters
            ),
            reddit=None if self.reddit is None else RedditSourceConfig(
                self.reddit.sort, self.reddit.time_filter,
            ),
            hacker_news=(
                None if self.hacker_news is None
                else HackerNewsSourceConfig(self.hacker_news.filter)
            ),
        )

    def source(self, source_id: uuid.UUID) -> Source:
        return self.new_source().with_id(source_id)


def _targets(targets: tuple[SourceTargetModel, ...]) -> tuple[Target, ...]:
    return tuple(
        Target(target.value, enabled=target.enabled) for target in targets
    )


class SourceOut(SourceBody):
    id: uuid.UUID


class SourceListResponse(ApiModel):
    # In the order they were added.
    sources: tuple[SourceOut, ...]
    # False while `sources_in_database` is off: collection and the reader
    # still use config.toml's sources, not these.
    in_database: bool
