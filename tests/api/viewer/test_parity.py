"""Both servers render the legacy viewer identically, over real rows."""

import datetime as dt
import html
import pathlib
import re
from collections.abc import AsyncIterator

import pytest

from dank.config import ClickHouseSettings, Settings
from dank.embeddings import get_embedding_model
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from tests.api.clickhouse import COLLECTED_AT, asset, insert, post
from tests.api.parity import Fetch, both_servers_whole

pytestmark = pytest.mark.clickhouse

VECTOR = (1.0, 0.0)
PAGE_HEADERS = ("content-type", "cache-control", "pragma", "expires")


def at(hours: int) -> dt.datetime:
    return COLLECTED_AT - dt.timedelta(hours=hours)


def pager_link(page: bytes, direction: str) -> str:
    pattern = rf'class="pager-{direction}" href="([^"]+)"'
    found = re.search(pattern, page.decode())

    assert found is not None

    return html.unescape(found[1])


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
    tmp_path: pathlib.Path,
) -> AsyncIterator[tuple[Fetch, Fetch]]:
    configured = settings._replace(clickhouse=clickhouse_settings)
    assets = assets_path(configured)
    assets.mkdir(parents=True)
    (assets / "a b.png").write_bytes(b"image")
    (assets / "clip.mp4").write_bytes(b"video")
    (tmp_path / "outside.png").write_bytes(b"image")
    await insert(clickhouse, "posts", [
        post(
            "a.example", "3",
            created_at=at(0),
            author="Ada Lovelace",
            title="A & B",
            html='<p onclick="x()">First <b>post</b></p><script>x()</script>',
            embedding=VECTOR,
        ),
        post("a.example", "2", created_at=at(1), author="Grace", title=""),
        post(
            "b.example", "1",
            created_at=at(48),
            author="ada",
            html="<p>" + "A long summary. " * 40 + "</p>",
            embedding=VECTOR,
        ),
    ])
    await insert(clickhouse, "assets", [
        asset("a.example", "3", "https://cdn/1", assets / "a b.png"),
        asset("a.example", "3", "https://cdn/2", assets / "clip.mp4", "video"),
        asset("a.example", "2", "https://cdn/3", tmp_path / "outside.png"),
    ])
    # Semantic search reads this rather than loading the model in a test.
    await insert(clickhouse, "web_embedding_cache", [{
        "model_name": get_embedding_model().model_name,
        "search_text": "space",
        "embedding": list(VECTOR),
        "created_at": dt.datetime.now(dt.UTC),
    }])

    async with both_servers_whole(configured) as both:
        yield both


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("target", [
    "/",
    "/?limit=1",
    "/?limit=many&days_back=-1&cursor_direction=sideways&unknown=1",
    "/?limit=999",
    "/?domain=a.example",
    "/?account=ada",
    "/?days_back=9999",
    "/?q=space",
    "/?q=space&domain=b.example",
    "/post?domain=a.example&post_id=3",
    "/post?domain=a.example&post_id=2",
    "/post?domain=b.example&post_id=1&unknown=1",
])
async def test_pages_match(
    servers: tuple[Fetch, Fetch], method: str, target: str,
) -> None:
    old, new = servers

    expected = await old(target, method=method)
    actual = await new(target, method=method)

    assert actual.status == expected.status == 200
    assert actual.body == expected.body

    for name in PAGE_HEADERS:
        assert actual.headers[name] == expected.headers[name]


async def test_paging_through_with_either_servers_links_matches(
    servers: tuple[Fetch, Fetch],
) -> None:
    old, new = servers
    target = "/?limit=1"
    page = b""

    # Three posts, one to a page: two steps forward lands on the last one.
    for _ in range(2):
        page = (await new(target)).body

        assert page == (await old(target)).body

        target = pager_link(page, "next")

    last = (await new(target)).body

    assert last == (await old(target)).body
    assert b"pager-next" not in last

    back = pager_link(last, "prev")

    assert (await new(back)).body == (await old(back)).body


@pytest.mark.parametrize(("target", "status", "text"), [
    ("/post", 400, "Missing post_id or domain"),
    ("/post?domain=a.example", 400, "Missing post_id or domain"),
    ("/post?post_id=3", 400, "Missing post_id or domain"),
    ("/post?domain=&post_id=3", 400, "Missing post_id or domain"),
    ("/post?domain=a.example&post_id=9", 404, "Post not found"),
])
async def test_plain_text_errors_match(
    servers: tuple[Fetch, Fetch], target: str, status: int, text: str,
) -> None:
    old, new = servers

    expected = await old(target)
    actual = await new(target)

    assert (actual.status, actual.body.decode()) == (status, text)
    assert (expected.status, expected.body.decode()) == (status, text)
    assert actual.headers["content-type"] == expected.headers["content-type"]
