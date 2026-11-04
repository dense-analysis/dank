"""Both servers answer for files alike, with their differences named.

The aiohttp server's reader build is fixed to the checkout, so the reader's
routes are compared against a real build by hand instead; see DEN-50.
"""

import email.utils
import os
import pathlib
from collections.abc import AsyncIterator

import pytest

from dank.config import ClickHouseSettings, Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import assets_path
from tests.api.parity import Exchange, Fetch, both_servers_whole

pytestmark = pytest.mark.clickhouse

VIDEO = bytes(range(256)) * 4096
SIZE = len(VIDEO)
OTHER_FILES = (
    "a.webm", "a.mp3", "a.jpg", "a.png", "a.gif", "a.webp", "no-extension",
)
CLIP = "/assets/example.com/clip.mp4"
MODIFIED = 1_700_000_000
MODIFIED_HTTP = email.utils.formatdate(MODIFIED, usegmt=True)
EARLIER_HTTP = email.utils.formatdate(MODIFIED - 1, usegmt=True)
# Headers that must match on any file response. Validators' values are the
# servers' own, apart from the date; ETags are compared by what they do.
SAME_HEADERS = (
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "last-modified",
    "cache-control",
    "pragma",
    "expires",
)


def visible(response: Exchange) -> tuple[object, ...]:
    return (
        response.status,
        {name: response.headers.get(name) for name in SAME_HEADERS},
        response.body,
    )


@pytest.fixture
async def servers(
    clickhouse: ClickHouseClient,
    clickhouse_settings: ClickHouseSettings,
    settings: Settings,
    tmp_path: pathlib.Path,
) -> AsyncIterator[tuple[Fetch, Fetch]]:
    configured = settings._replace(clickhouse=clickhouse_settings)
    assets = assets_path(configured)
    (assets / "example.com").mkdir(parents=True)
    clip = assets / "example.com" / "clip.mp4"
    clip.write_bytes(VIDEO)
    os.utime(clip, (MODIFIED, MODIFIED))

    for name in OTHER_FILES:
        (assets / name).write_bytes(b"data")
        # Whole seconds, because aiohttp rounds a fraction up and Starlette
        # down. Each date still revalidates against its own server.
        os.utime(assets / name, (MODIFIED, MODIFIED))

    (tmp_path / "secret.txt").write_text("secret")
    (assets / "link.txt").symlink_to(tmp_path / "secret.txt")

    async with both_servers_whole(configured) as both:
        yield both


@pytest.mark.parametrize("headers", [
    {},
    {"range": "bytes=100-199"},
    {"range": "bytes=0-0"},
    {"range": "bytes=500000-"},
    {"range": "bytes=-10"},
    {"range": f"bytes={SIZE - 5}-{SIZE + 100}"},
    {"if-modified-since": EARLIER_HTTP},
    {"range": "bytes=0-9", "if-range": MODIFIED_HTTP},
    {"range": "bytes=0-9", "if-range": EARLIER_HTTP},
])
async def test_seeking_and_dates_match(
    servers: tuple[Fetch, Fetch], headers: dict[str, str],
) -> None:
    old, new = servers

    expected = visible(await old(CLIP, headers=headers))
    actual = visible(await new(CLIP, headers=headers))

    assert actual == expected


async def test_an_unchanged_date_revalidates_alike(
    servers: tuple[Fetch, Fetch],
) -> None:
    # Only the outcome: a 304 needs no more headers, and Starlette sends fewer.
    for fetch in servers:
        fresh = await fetch(CLIP, headers={"if-modified-since": MODIFIED_HTTP})

        assert (fresh.status, fresh.body) == (304, b"")


async def test_each_servers_etag_revalidates_alike(
    servers: tuple[Fetch, Fetch],
) -> None:
    for fetch in servers:
        etag = (await fetch(CLIP)).headers["etag"]
        fresh = await fetch(CLIP, headers={"if-none-match": etag})
        part = await fetch(
            CLIP, headers={"range": "bytes=0-9", "if-range": etag},
        )

        assert (fresh.status, fresh.body) == (304, b"")
        assert (part.status, part.body) == (206, VIDEO[:10])


async def test_a_range_past_the_end_matches(
    servers: tuple[Fetch, Fetch],
) -> None:
    old, new = servers
    headers = {"range": f"bytes={SIZE}-"}

    expected = await old(CLIP, headers=headers)
    actual = await new(CLIP, headers=headers)

    assert actual.status == expected.status == 416
    assert actual.headers["content-range"] == (
        expected.headers["content-range"]
    )


@pytest.mark.parametrize("headers", [{}, {"range": "bytes=0-9"}])
async def test_head_matches(
    servers: tuple[Fetch, Fetch], headers: dict[str, str],
) -> None:
    old, new = servers

    expected = visible(await old(CLIP, method="HEAD", headers=headers))
    actual = visible(await new(CLIP, method="HEAD", headers=headers))

    assert actual == expected


@pytest.mark.parametrize("name", OTHER_FILES)
async def test_content_types_match(
    servers: tuple[Fetch, Fetch], name: str,
) -> None:
    old, new = servers

    assert visible(await new(f"/assets/{name}")) == (
        visible(await old(f"/assets/{name}"))
    )


@pytest.mark.parametrize("path", [
    "/assets/missing.mp4",
    "/assets/%2e%2e/secret.txt",
    "/assets/link.txt",
    "/static/missing.css",
])
async def test_what_stays_hidden_matches(
    servers: tuple[Fetch, Fetch], path: str,
) -> None:
    old, new = servers

    expected = await old(path)
    actual = await new(path)

    assert actual.status == expected.status == 404
    assert b"secret" not in actual.body


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("headers", [{}, {"range": "bytes=0-3"}])
async def test_static_files_match(
    servers: tuple[Fetch, Fetch], method: str, headers: dict[str, str],
) -> None:
    old, new = servers
    path = "/static/app.css"

    expected = await old(path, method=method, headers=headers)
    actual = await new(path, method=method, headers=headers)

    assert actual.status == expected.status
    assert actual.body == expected.body

    for name in ("cache-control", "pragma", "expires", "content-length"):
        assert actual.headers[name] == expected.headers[name]

    # Starlette names the charset of a text file; aiohttp doesn't.
    assert actual.headers["content-type"] == "text/css; charset=utf-8"
    assert expected.headers["content-type"].startswith("text/css")


async def test_the_bare_reader_path_redirects_alike(
    servers: tuple[Fetch, Fetch],
) -> None:
    old, new = servers

    expected = await old("/reader")
    actual = await new("/reader")

    assert actual.status == expected.status == 302
    assert actual.headers["location"] == expected.headers["location"]


async def test_the_intended_differences(
    servers: tuple[Fetch, Fetch],
) -> None:
    old, new = servers
    several = {"range": "bytes=0-1,4-5"}
    malformed = {"range": "bytes=nonsense"}

    # A directory is hidden either way.
    assert (await old("/assets/example.com")).status == 403
    assert (await new("/assets/example.com")).status == 404
    # Starlette can serve several ranges; aiohttp refuses them.
    assert (await old(CLIP, headers=several)).status == 416
    assert (await new(CLIP, headers=several)).status == 206
    # A Range header that makes no sense, which browsers don't send.
    assert (await old(CLIP, headers=malformed)).status == 416
    assert (await new(CLIP, headers=malformed)).status == 400
    # Unknown pages are plain text on both, in different words.
    unknown = await new("/nope")

    assert (await old("/nope")).status == unknown.status == 404
    assert unknown.headers["content-type"].startswith("text/plain")
