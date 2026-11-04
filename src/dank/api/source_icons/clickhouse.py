"""CollectedDomains read from collected posts in ClickHouse."""

from dank.storage.clickhouse import ClickHouseClient

HAS_POSTS_SQL = (
    "SELECT domain FROM posts FINAL WHERE domain = %(domain)s LIMIT 1"
)


class ClickHouseCollectedDomains:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def has_posts(self, domain: str) -> bool:
        result = await self._client.fetch_json(
            HAS_POSTS_SQL, {"domain": domain},
        )

        return bool(result.rows)
