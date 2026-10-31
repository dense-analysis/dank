"""What DANK collects from each source: values, and the rules joining them.

Plain Python only. No FastAPI, Pydantic, SQL or drivers in this module.
"""

# `count` is the API's name for post counts, and these NamedTuples keep it,
# although it shadows tuple.count. Nothing here calls tuple.count.
# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Literal, NamedTuple
from urllib.parse import urlsplit

from dank.config import SourceConfig

type SourceKind = Literal["rss", "hacker_news", "youtube", "x", "reddit"]


class XAccount(NamedTuple):
    handle: str
    name: str
    icon_url: str | None
    total_posts: int | None
    count: int


class XActivity(NamedTuple):
    accounts: tuple[XAccount, ...]
    for_you_count: int


class Flair(NamedTuple):
    name: str
    count: int


class PostTotal(NamedTuple):
    """A community's post count as reported by an outside archive."""

    count: int
    source: str
    updated_at: str
    captured_at: str


class Community(NamedTuple):
    name: str
    count: int
    icon_url: str | None
    flairs: tuple[Flair, ...]
    post_total: PostTotal | None


class RedditActivity(NamedTuple):
    communities: tuple[Community, ...]
    flairs: tuple[Flair, ...]


class SourceSummary(NamedTuple):
    domain: str
    name: str
    kind: SourceKind
    configured: bool
    tags: tuple[str, ...]
    count: int
    activity: XActivity | RedditActivity | None = None


class Overview(NamedTuple):
    sources: tuple[SourceSummary, ...]
    total_posts: int
    blog_external_count: int


def summarize(
    configured: tuple[SourceConfig, ...], counts: Mapping[str, int],
) -> tuple[SourceSummary, ...]:
    """One summary per domain that is configured, collected, or both."""
    tags: dict[str, set[str]] = {}

    for source in configured:
        tags.setdefault(source.domain, set()).update(source.tags)

    return tuple(
        SourceSummary(
            domain=domain,
            name=display_name(domain),
            kind=source_kind(domain),
            configured=domain in tags,
            tags=tuple(sorted(tags.get(domain, ()))),
            count=counts.get(domain, 0),
        )
        for domain in sorted(counts.keys() | tags.keys())
    )


def source_kind(domain: str) -> SourceKind:
    match domain:
        case "x.com" | "twitter.com":
            return "x"
        case "youtube.com":
            return "youtube"
        case "news.ycombinator.com":
            return "hacker_news"
        case "reddit.com":
            return "reddit"
        case _:
            return "rss"


def display_name(domain: str) -> str:
    return "Hacker News" if domain == "news.ycombinator.com" else domain


def x_accounts(
    configured: tuple[SourceConfig, ...], domain: str,
) -> tuple[str, ...]:
    return tuple(
        account
        for source in configured if source.domain == domain
        for account in source.accounts
    )


def reddit_communities(
    configured: tuple[SourceConfig, ...],
) -> dict[str, str]:
    """Configured subreddit names keyed by their lower-case form."""
    names = (
        urlsplit(url).path.split("/")[2]
        for source in configured if source.domain == "reddit.com"
        for url in source.feed_urls
    )

    return {name.lower(): name for name in names}


def flair_totals(flairs: Iterable[Flair]) -> tuple[Flair, ...]:
    """Each flair name once, counted across communities, sorted by name."""
    totals: Counter[str] = Counter()

    for flair in flairs:
        totals[flair.name] += flair.count

    return tuple(Flair(name, count) for name, count in sorted(totals.items()))
