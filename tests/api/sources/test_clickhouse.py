"""Collection stats projected from synthetic ClickHouse result rows."""

import datetime as dt
import json
import pathlib

from dank.api.sources.clickhouse import ClickHouseCollectionStats
from dank.api.sources.model import (
    Community,
    Flair,
    PostTotal,
    RedditActivity,
    XAccount,
    XActivity,
)
from tests.api.clickhouse import COLLECTED_AT, query_client

ASSETS = pathlib.Path("/fixture/assets")
ICON = "https://pbs.twimg.com/profile_images/123/profile_normal.png"


async def test_domain_counts_use_integer_values() -> None:
    stats = ClickHouseCollectionStats(query_client([
        {"domain": "a.example", "count": "2"},
        {"domain": "b.example", "count": 1},
    ]), ASSETS)

    assert await stats.domain_counts() == {"a.example": 2, "b.example": 1}


async def test_x_activity_lists_configured_accounts_with_profiles() -> None:
    stats = ClickHouseCollectionStats(query_client(
        [{"handle": "watcherguru", "count": 2},
         {"handle": "otherauthor", "count": 1}],
        [{
            "domain": "x.com", "handle": "watcherguru",
            "profile": json.dumps({
                "core": {"screen_name": "WatcherGuru", "name": "Watcher.Guru"},
                "avatar": {"image_url": ICON},
            }),
        }],
        [{"x_home_count": "2"}],
    ), ASSETS)
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


async def test_reddit_activity_covers_each_configured_community() -> None:
    updated = COLLECTED_AT - dt.timedelta(hours=1)
    stats = ClickHouseCollectionStats(query_client(
        [{"community": "localllama", "count": "2"}], [],
        [{
            "community": "localllama", "total_posts": "120",
            "source": "arctic_shift", "updated_at": updated,
            "scraped_at": COLLECTED_AT,
        }],
        [{"community": "localllama", "name": "News", "count": 2},
         {"community": "python", "name": "News", "count": 1}],
    ), ASSETS)
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
        flairs=(Flair("News", 3),),
    )


async def test_reddit_activity_without_communities_queries_nothing() -> None:
    stats = ClickHouseCollectionStats(query_client(), ASSETS)

    assert await stats.reddit_activity({}) == RedditActivity((), ())
