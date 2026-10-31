"""CollectionStats read from collected posts in ClickHouse.

SQL lives here and in the reader helpers this reuses, so the queries are
today's. Rows never leave the module: every method returns domain values.
"""

from __future__ import annotations

import asyncio
import pathlib
from collections.abc import Mapping

from dank.api.sources.model import (
    Community,
    Flair,
    PostTotal,
    RedditActivity,
    XAccount,
    XActivity,
    flair_totals,
)
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_accounts import source_accounts
from dank.web.reader_blogs import external_blog_count
from dank.web.reader_communities import community_icons, community_totals
from dank.web.reader_flairs import reddit_flairs
from dank.web.reader_query import REDDIT_COMMUNITY_SQL
from dank.web.reader_x_home import HOME_CONDITION

DOMAIN_COUNTS_SQL = (
    "SELECT domain, count() AS count FROM posts FINAL GROUP BY domain"
)
COMMUNITY_COUNTS_SQL = (
    f"SELECT {REDDIT_COMMUNITY_SQL} AS community, count() AS count "
    "FROM posts FINAL WHERE domain = 'reddit.com' GROUP BY community"
)
FOR_YOU_COUNT_SQL = (
    "SELECT uniqExact(post_id) AS x_home_count FROM raw_posts "
    "WHERE domain = %(domain)s AND " + HOME_CONDITION
)


class ClickHouseCollectionStats:
    def __init__(
        self, client: ClickHouseClient, assets_dir: pathlib.Path,
    ) -> None:
        self._client = client
        self._assets_dir = assets_dir

    async def domain_counts(self) -> dict[str, int]:
        return await self._counts(DOMAIN_COUNTS_SQL, "domain")

    async def external_blog_count(self) -> int:
        return await external_blog_count(self._client)

    async def x_activity(
        self, domain: str, accounts: tuple[str, ...],
    ) -> XActivity:
        found, home = await asyncio.gather(
            source_accounts(self._client, domain, accounts),
            self._client.fetch_json(FOR_YOU_COUNT_SQL, {"domain": domain}),
        )

        return XActivity(
            accounts=tuple(XAccount(**account) for account in found),
            for_you_count=(
                int(home.rows[0]["x_home_count"]) if home.rows else 0
            ),
        )

    async def reddit_activity(
        self, communities: Mapping[str, str],
    ) -> RedditActivity:
        if not communities:
            return RedditActivity(communities=(), flairs=())

        keys = list(communities)
        counts, icons, totals, flair_rows = await asyncio.gather(
            self._counts(COMMUNITY_COUNTS_SQL, "community"),
            community_icons(self._client, keys, self._assets_dir),
            community_totals(self._client, keys),
            reddit_flairs(self._client),
        )
        flairs = {
            community: tuple(Flair(**flair) for flair in labels)
            for community, labels in flair_rows.items()
        }
        total = {key: PostTotal(**value) for key, value in totals.items()}

        return RedditActivity(
            communities=tuple(
                Community(
                    name=name,
                    count=counts.get(key, 0),
                    icon_url=icons.get(key),
                    flairs=flairs.get(key, ()),
                    post_total=total.get(key),
                )
                for key, name in sorted(communities.items())
            ),
            # Every collected community counts, configured or not.
            flairs=flair_totals(
                flair for labels in flairs.values() for flair in labels
            ),
        )

    async def _counts(self, query: str, key: str) -> dict[str, int]:
        result = await self._client.fetch_json(query)

        return {str(row[key]): int(row["count"]) for row in result.rows}
