"""Both servers answer GET /api/reader/source-icon alike."""

import hashlib
import json
from collections.abc import AsyncIterator

import pytest

from dank.config import ClickHouseSettings, Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from tests.api.clickhouse import insert, post
from tests.api.parity import Fetch, both_servers_whole

pytestmark = pytest.mark.clickhouse

PATH = "/api/reader/source-icon"
PNG = b"\x89PNG cached icon"


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
) -> AsyncIterator[tuple[Fetch, Fetch]]:
    await insert(clickhouse, "posts", [post("old.example", "1")])
    configured = settings._replace(
        clickhouse=clickhouse_settings,
        sources=(SourceConfig("politics.example", ()),),
    )
    # A fresh file on disk, so that neither server goes to the Internet.
    icons = assets_path(configured) / "source-icons"
    icons.mkdir(parents=True)

    for domain in ("politics.example", "old.example"):
        name = hashlib.sha256(domain.encode()).hexdigest()
        (icons / f"{name}.png").write_bytes(PNG)

    async with both_servers_whole(configured) as both:
        yield both


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("domain", [
    "politics.example", "old.example", "%20Politics.EXAMPLE%20",
])
async def test_a_cached_icon_matches(
    servers: tuple[Fetch, Fetch], method: str, domain: str,
) -> None:
    old, new = servers
    target = f"{PATH}?domain={domain}"

    expected = await old(target, method=method)
    actual = await new(target, method=method)

    assert actual.status == expected.status == 200
    assert actual.body == expected.body
    assert actual.headers["content-type"] == expected.headers["content-type"]
    assert actual.headers["cache-control"] == "public, max-age=86400"
    assert expected.headers["cache-control"] == "public, max-age=86400"


@pytest.mark.parametrize("query", [
    "",
    "?domain=",
    "?domain=reddit.com",
    "?domain=127.0.0.1",
    "?domain=stranger.example",
])
async def test_having_no_icon_matches(
    servers: tuple[Fetch, Fetch], query: str,
) -> None:
    old, new = servers

    expected = await old(PATH + query)
    actual = await new(PATH + query)

    assert actual.status == expected.status == 404
    assert actual.headers["cache-control"] == "no-store"
    assert expected.headers["cache-control"] == "no-store"
    assert json.loads(actual.body)["error"] == (
        json.loads(expected.body)["error"]
    )
