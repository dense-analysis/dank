"""Pure source rules, so nothing is faked."""

import pytest

from dank.api.sources.model import (
    Flair,
    SourceKind,
    flair_totals,
    reddit_communities,
    source_kind,
    summarize,
    x_accounts,
)
from dank.config import SourceConfig


def test_summaries_cover_configured_and_collected_domains() -> None:
    configured = (
        SourceConfig("a.example", (), tags=("news", "daily")),
        SourceConfig("a.example", (), tags=("news",)),
    )

    summaries = summarize(configured, {"b.example": 2})

    assert [
        (s.domain, s.configured, s.tags, s.count) for s in summaries
    ] == [
        ("a.example", True, ("daily", "news"), 0),
        ("b.example", False, (), 2),
    ]


def test_hacker_news_has_a_readable_name() -> None:
    [summary] = summarize((), {"news.ycombinator.com": 3})

    assert (summary.name, summary.kind) == ("Hacker News", "hacker_news")


@pytest.mark.parametrize(("domain", "kind"), [
    ("x.com", "x"),
    ("twitter.com", "x"),
    ("youtube.com", "youtube"),
    ("reddit.com", "reddit"),
    ("news.ycombinator.com", "hacker_news"),
    ("blog.example", "rss"),
])
def test_source_kind(domain: str, kind: SourceKind) -> None:
    assert source_kind(domain) == kind


def test_x_accounts_come_from_every_entry_for_the_domain() -> None:
    configured = (
        SourceConfig("x.com", ("@WatcherGuru",)),
        SourceConfig("twitter.com", ("elsewhere",)),
        SourceConfig("x.com", ("Empty_Account",)),
    )

    assert x_accounts(configured, "x.com") == ("@WatcherGuru", "Empty_Account")


def test_reddit_communities_come_from_feed_urls() -> None:
    configured = (SourceConfig("reddit.com", (), feed_urls=(
        "https://www.reddit.com/r/LocalLLaMA/",
        "https://www.reddit.com/r/python/top/.rss",
    )),)

    assert reddit_communities(configured) == {
        "localllama": "LocalLLaMA", "python": "python",
    }


def test_flair_totals_add_up_across_communities() -> None:
    flairs = (Flair("News", 2), Flair("Discussion", 1), Flair("News", 3))

    assert flair_totals(flairs) == (Flair("Discussion", 1), Flair("News", 5))
