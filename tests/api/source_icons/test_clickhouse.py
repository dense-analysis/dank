"""Map query results to source icon visibility without opening a database."""

from dank.api.source_icons.clickhouse import ClickHouseCollectedDomains
from tests.api.clickhouse import query_client


async def test_a_domain_counts_once_it_has_a_post() -> None:
    collected = ClickHouseCollectedDomains(
        query_client([{"domain": "old.example"}], []),
    )

    assert await collected.has_posts("old.example")
    assert not await collected.has_posts("stranger.example")
