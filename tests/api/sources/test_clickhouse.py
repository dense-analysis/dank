"""ClickHouseCollectionStats against a real, throwaway ClickHouse database.

Runs with --clickhouse-url; see tests/api/conftest.py.
"""

import datetime as dt
import pathlib

import pytest

from dank.api.sources.clickhouse import ClickHouseCollectionStats
from dank.api.sources.model import (
    Community,
    Flair,
    PostTotal,
    RedditActivity,
    XAccount,
    XActivity,
)
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import (
    COLLECTED_AT,
    insert,
    post,
    raw_post,
    reddit_url,
    x_url,
)

pytestmark = pytest.mark.clickhouse

ICON = "https://pbs.twimg.com/profile_images/123/profile_normal.png"
FOR_YOU = {"dank_x_home": {"feed": "for_you"}}


@pytest.fixture
def stats(
    clickhouse: ClickHouseClient, tmp_path: pathlib.Path,
) -> ClickHouseCollectionStats:
    return ClickHouseCollectionStats(clickhouse, tmp_path)


async def test_domain_counts_count_reprocessed_posts_once(
    clickhouse: ClickHouseClient, stats: ClickHouseCollectionStats,
) -> None:
    await insert(clickhouse, "posts", [
        post("a.example", "1"),
        # Processed again; FINAL keeps only the newer version.
        post("a.example", "1", updated_at=COLLECTED_AT + dt.timedelta(days=1)),
        post("a.example", "2"),
        post("b.example", "3"),
    ])

    assert await stats.domain_counts() == {"a.example": 2, "b.example": 1}


async def test_x_activity_lists_configured_accounts_with_profiles(
    clickhouse: ClickHouseClient, stats: ClickHouseCollectionStats,
) -> None:
    await insert(clickhouse, "posts", [
        post("x.com", "1", url=x_url("WatcherGuru", "1"), source="x"),
        post("x.com", "2", url=x_url("WatcherGuru", "2"), source="x"),
        post("x.com", "3", url=x_url("OtherAuthor", "3"), source="x"),
    ])
    profile = {"core": {"user_results": {"result": {
        "core": {"screen_name": "WatcherGuru", "name": "Watcher.Guru"},
        "avatar": {"image_url": ICON},
    }}}}
    await insert(clickhouse, "raw_posts", [
        raw_post(
            "x.com", "1", profile | FOR_YOU,
            url=x_url("WatcherGuru", "1"), source="x",
        ),
        raw_post(
            "x.com", "3", FOR_YOU, url=x_url("OtherAuthor", "3"), source="x",
        ),
    ])

    activity = await stats.x_activity(
        "x.com", ("@WatcherGuru", "Empty_Account"),
    )

    assert activity == XActivity(
        accounts=(
            XAccount("empty_account", "empty_account", None, None, 0),
            XAccount("watcherguru", "Watcher.Guru", ICON, None, 2),
        ),
        for_you_count=2,
    )


async def test_reddit_activity_covers_each_configured_community(
    clickhouse: ClickHouseClient, stats: ClickHouseCollectionStats,
) -> None:
    await insert(clickhouse, "posts", [
        post(
            "reddit.com", f"t3_{post_id}",
            url=reddit_url(community, post_id), source="reddit",
        )
        for community, post_id in (
            ("LocalLLaMA", "a"), ("LocalLLaMA", "b"), ("python", "c"),
        )
    ])
    await insert(clickhouse, "raw_posts", [
        raw_post(
            "reddit.com", f"t3_{post_id}", {"link_flair_text": flair},
            source="reddit",
        )
        for post_id, flair in (("a", " News "), ("b", "News"), ("c", "News"))
    ])
    updated = COLLECTED_AT - dt.timedelta(hours=1)
    await insert(clickhouse, "reddit_community_totals", [{
        "community": "localllama", "total_posts": 120,
        "source": "arctic_shift", "updated_at": updated,
        "scraped_at": COLLECTED_AT, "request_url": "", "payload": "{}",
    }])

    activity = await stats.reddit_activity(
        {"localllama": "LocalLLaMA", "accelerate": "accelerate"},
    )

    assert activity == RedditActivity(
        communities=(
            Community("accelerate", 0, None, (), None),
            Community(
                "LocalLLaMA", 2, None, (Flair("News", 2),),
                PostTotal(
                    120, "arctic_shift", updated.isoformat(),
                    COLLECTED_AT.isoformat(),
                ),
            ),
        ),
        # Flairs from communities that aren't configured count too.
        flairs=(Flair("News", 3),),
    )


async def test_reddit_activity_without_communities_queries_nothing(
    stats: ClickHouseCollectionStats,
) -> None:
    assert await stats.reddit_activity({}) == RedditActivity((), ())
