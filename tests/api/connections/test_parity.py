"""Both servers answer the connection routes with the same JSON."""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import pytest

from dank.config import ClickHouseSettings, Settings
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import COLLECTED_AT, insert, post
from tests.api.parity import Get, both_servers

pytestmark = pytest.mark.clickhouse

STAMP = COLLECTED_AT - dt.timedelta(hours=1)
ROOT_URL = "https://www.reddit.com/r/kernel/comments/1/"
SIBLING_URL = "https://www.reddit.com/r/kernel/comments/2/"
TARGET = "https://target.test/article"
ALIAS = "https://target.test/alias"
VIDEO = "https://www.youtube.com/watch?v=NnV_cWeoo5Q"
UNCOLLECTED = "https://nowhere.test/page"


def evidence(
    post_id: str, url: str, target: str, kind: str, reference_id: str = "",
) -> dict[str, Any]:
    return {
        "domain": "reddit.com", "post_id": post_id, "url": url,
        "source": "reddit", "title": f"Post {post_id}", "target_url": target,
        "kind": kind, "reference_id": reference_id, "reference_url": url,
        "author": "Reader", "matched_url": target, "excerpt": "Evidence",
        "observed_at": STAMP,
    }


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
) -> AsyncIterator[tuple[Get, Get]]:
    await insert(clickhouse, "posts", [
        post("reddit.com", "1", url=ROOT_URL, source="reddit"),
        post("reddit.com", "2", url=SIBLING_URL, source="reddit"),
        post(
            "target.test", "article", url=TARGET, source="linked_article",
            html="<p>Saved</p>", title="Original",
        ),
    ])
    await insert(clickhouse, "post_urls", [
        {"url": url, "domain": "target.test", "post_id": "article",
         "observed_at": STAMP}
        for url in (TARGET, ALIAS)
    ] + [{
        "url": ROOT_URL, "domain": "reddit.com", "post_id": "1",
        "observed_at": STAMP,
    }])
    await insert(clickhouse, "post_reference_evidence", [
        evidence("1", ROOT_URL, TARGET, "post"),
        evidence("1", ROOT_URL, VIDEO, "comment", "c1"),
        evidence("1", ROOT_URL, UNCOLLECTED, "comment", "c2"),
        evidence("2", SIBLING_URL, ALIAS, "comment", "c3"),
        evidence("2", SIBLING_URL, ROOT_URL, "comment", "c4"),
    ])
    await insert(clickhouse, "youtube_video_metadata", [{
        "video_id": "NnV_cWeoo5Q", "observed_at": STAMP,
        "payload": '{"title": "A video"}',
    }])
    configured = settings._replace(clickhouse=clickhouse_settings)

    async with both_servers(configured) as pair:
        yield pair


def lookup(**fields: str) -> str:
    return "/api/reader/connection?" + urlencode({
        "domain": "reddit.com", "id": "1", **fields,
    })


@pytest.mark.parametrize("path", [
    "/api/reader/connections?domain=reddit.com&id=1",
    "/api/reader/connections?domain=+reddit.com+&id=1",
    "/api/reader/connections?domain=reddit.com&id=1&source=Reddit",
    "/api/reader/connections?domain=reddit.com&id=1&source=www.reddit.com"
    "&community=Kernel",
    "/api/reader/connections?domain=reddit.com&id=1&source=x",
    "/api/reader/connections?domain=reddit.com&id=2",
    "/api/reader/connections?domain=target.test&id=article",
    lookup(target=TARGET, kind="post"),
    lookup(target=TARGET + "#fragment", kind="post"),
    lookup(target=VIDEO, kind="comment", reference_id="c1"),
    lookup(target=UNCOLLECTED, kind="comment", reference_id="c2"),
    lookup(
        target=UNCOLLECTED, kind="comment", reference_id="c2",
        observed_at=STAMP.isoformat(),
    ),
])
async def test_both_servers_return_the_same_json(
    servers: tuple[Get, Get], path: str,
) -> None:
    old, new = servers
    expected, actual = await old(path), await new(path)

    assert expected[0] == 200, path
    assert actual == expected


async def test_rows_cover_each_direction_and_target(
    servers: tuple[Get, Get],
) -> None:
    _, new = servers
    _, body = await new("/api/reader/connections?domain=reddit.com&id=1")

    assert body["incoming"]
    assert body["related"]
    assert {
        item["target"]["post"] is None for item in body["outgoing"]
    } == {True, False}
    assert any(item["target"]["youtube_item_id"] for item in body["outgoing"])


@pytest.mark.parametrize(("path", "status"), [
    ("/api/reader/connections?domain=gone&id=gone", 404),
    (lookup(target=TARGET, kind="comment", reference_id="absent"), 404),
    ("/api/reader/connections", 400),
    ("/api/reader/connections?domain=a", 400),
    ("/api/reader/connections?id=b", 400),
    ("/api/reader/connections?domain=a&id=b&unknown=x", 400),
    ("/api/reader/connections?domain=a&domain=b&id=c", 400),
    ("/api/reader/connections?domain=a&id=b&limit=100000", 400),
    ("/api/reader/connections?domain=a&id=b&source=unknown", 400),
    ("/api/reader/connections?domain=a&id=b&community=kernel", 400),
    ("/api/reader/connections?domain=a&id=b&source=x&community=kernel", 400),
    ("/api/reader/connections?domain=a&id=b&source=reddit&community=b/c", 400),
    ("/api/reader/connections?domain=a&id=b&source=reddit&source=x", 400),
    ("/api/reader/connections?domain=+&id=b", 400),
    ("/api/reader/connections?domain=" + "a" * 2001 + "&id=b", 400),
    ("/api/reader/connection", 400),
    (lookup(target="javascript:alert(1)", kind="post"), 400),
    (lookup(target=TARGET, kind="quote"), 400),
    (lookup(target=TARGET, kind="comment"), 400),
    (lookup(target=TARGET, kind="post", reference_id="c1"), 400),
    (lookup(target=TARGET, kind="post", observed_at="not a date"), 400),
    (lookup(target=TARGET, kind="post", source="reddit"), 400),
    (lookup(target=TARGET, kind="post") + "&kind=comment", 400),
])
async def test_failures_match(
    servers: tuple[Get, Get], path: str, status: int,
) -> None:
    old, new = servers
    (old_status, old_body), (new_status, new_body) = (
        await old(path), await new(path),
    )

    assert (old_status, new_status) == (status, status), path

    if new_body["code"] in {"invalid_input", "not_found"}:
        assert new_body["error"] == old_body["error"]
