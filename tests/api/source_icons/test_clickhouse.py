"""ClickHouseCollectedDomains against a real, throwaway database."""

import pytest

from dank.api.source_icons.clickhouse import ClickHouseCollectedDomains
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import insert, post

pytestmark = pytest.mark.clickhouse


async def test_a_domain_counts_once_it_has_a_post(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [post("old.example", "1")])
    collected = ClickHouseCollectedDomains(clickhouse)

    assert await collected.has_posts("old.example")
    assert not await collected.has_posts("stranger.example")
