from __future__ import annotations

import asyncio
import io
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from PIL import Image

from dank.config import SourceConfig, load_settings
from dank.storage.clickhouse import QueryResult
from dank.web.reader_source_icons import (
    ICON_CONCURRENCY,
    ICON_FRESH_SECONDS,
    SOURCE_ICON_CACHE,
    SourceIconCache,
    _as_png,  # pyright: ignore[reportPrivateUsage]
    _capture_icon,  # pyright: ignore[reportPrivateUsage]
    _fetch_bytes,  # pyright: ignore[reportPrivateUsage]
    _icon_candidates,  # pyright: ignore[reportPrivateUsage]
    _PublicResolver,  # pyright: ignore[reportPrivateUsage]
    handle_source_icon,
    source_icon_url,
)


def _png() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (128, 128), "orange").save(output, format="PNG")

    return output.getvalue()


@pytest.mark.parametrize("domain", [
    "reddit.com", "www.youtube.com", "x.com", "127.0.0.1", "localhost",
    "host:8080", "../github.blog", "github.blog/path", "bad..example",
])
def test_icon_route_rejects_platforms_and_malformed_domains(domain: str):
    assert source_icon_url(domain) is None


def test_icon_route_and_declared_candidates_are_bounded() -> None:
    assert source_icon_url("github.blog") == (
        "/api/reader/source-icon?domain=github.blog"
    )
    assert source_icon_url("news.ycombinator.com") is not None
    page = b'''
        <link rel="icon" href="data:image/png;base64,bogus">
        <link rel="icon" href="https://[broken/favicon.ico">
        <link rel="icon" href="https://127.0.0.1/icon.png">
        <link rel="icon" type="image/svg+xml" href="/vector">
        <link rel="icon" href="/vector.svg">
        <link rel="shortcut icon" href="/small.png">
        <link rel="icon" href="/small.png">
        <link rel="apple-touch-icon" href="https://cdn.example/icon.png">
        <link rel="icon" href="/another.png">
    '''
    assert _icon_candidates(page, "https://github.blog/") == [
        "https://github.blog/small.png", "https://cdn.example/icon.png",
        "https://github.blog/favicon.ico",
    ]


def test_downloaded_image_is_reencoded_and_resized() -> None:
    with Image.open(io.BytesIO(_as_png(_png()))) as image:
        assert image.format == "PNG"
        assert image.size == (64, 64)
        assert image.mode == "RGBA"

    with pytest.raises(OSError):
        _as_png(b"<html>Access denied</html>")


@pytest.mark.parametrize("address", [
    "127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "ff02::1",
])
async def test_resolver_blocks_private_addresses(address: str) -> None:
    with patch.object(aiohttp.ThreadedResolver, "resolve", new=AsyncMock(
        return_value=[{"host": address}],
    )):
        with pytest.raises(OSError, match="public addresses"):
            await _PublicResolver().resolve("blog.example")


async def test_redirects_cannot_escape_to_private_url() -> None:
    response = AsyncMock()
    response.status = 302
    response.headers = {"Location": "http://127.0.0.1/secret"}
    session = SimpleNamespace(get=Mock(return_value=response))
    response.__aenter__.return_value = response

    with pytest.raises(ValueError, match="Unsupported"):
        await _fetch_bytes(
            session, "https://blog.example/", 100,  # type: ignore[arg-type]
        )


async def test_capture_uses_favicon_fallback_after_invalid_declared_image():
    with patch("dank.web.reader_source_icons._fetch_bytes", new=AsyncMock(
        side_effect=[
            (b'<link rel="icon" href="/bad.png">', "https://blog.example/"),
            (b"not an image", "https://blog.example/bad.png"),
            (_png(), "https://blog.example/favicon.ico"),
        ],
    )) as fetch:
        result = await _capture_icon("blog.example")

    assert result and result.startswith(b"\x89PNG")
    assert [call.args[1] for call in fetch.call_args_list] == [
        "https://blog.example/", "https://blog.example/bad.png",
        "https://blog.example/favicon.ico",
    ]


async def test_cache_deduplicates_capture_and_survives_restart(tmp_path: Path):
    cache = SourceIconCache(tmp_path)
    image = _as_png(_png())

    with patch("dank.web.reader_source_icons._capture_icon", new=AsyncMock(
        return_value=image,
    )) as capture:
        assert await asyncio.gather(*(
            cache.get("github.blog") for _ in range(8)
        )) == [image] * 8
        assert await SourceIconCache(tmp_path).get("github.blog") == image

    capture.assert_awaited_once_with("github.blog")


async def test_failed_refresh_preserves_good_icon_and_backs_off(
    tmp_path: Path,
):
    cache = SourceIconCache(tmp_path)
    image = _as_png(_png())

    with patch("dank.web.reader_source_icons._capture_icon", new=AsyncMock(
        return_value=image,
    )):
        assert await cache.get("github.blog") == image

    path = next(cache.directory.glob("*.png"))
    os.utime(path, (1, 1))
    cache = SourceIconCache(tmp_path)
    assert path.stat().st_mtime < ICON_FRESH_SECONDS

    with patch("dank.web.reader_source_icons._capture_icon", new=AsyncMock(
        return_value=None,
    )) as capture:
        assert await cache.get("github.blog") == image
        assert await cache.get("github.blog") == image

    capture.assert_awaited_once()
    assert path.read_bytes() == image


async def test_capture_concurrency_is_bounded(tmp_path: Path) -> None:
    active = 0
    peak = 0

    async def capture(_domain: str) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        active -= 1

    cache = SourceIconCache(tmp_path)

    with patch("dank.web.reader_source_icons._capture_icon", capture):
        await asyncio.gather(*(
            cache.get(f"blog{index}.example") for index in range(20)
        ))

    assert peak == ICON_CONCURRENCY


@pytest.mark.parametrize("configured,stored,allowed", [
    (True, False, True), (False, True, True), (False, False, False),
])
async def test_icon_capture_requires_a_known_source(
    *, configured: bool, stored: bool, allowed: bool,
) -> None:
    settings = load_settings("config.example.toml")._replace(
        sources=(SourceConfig("github.blog", ()),) if configured else (),
    )
    cache = AsyncMock()
    cache.get.return_value = _as_png(_png())
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(
        [{"domain": "github.blog"}] if stored else [],
    )
    request = make_mocked_request(
        "GET", "/api/reader/source-icon?domain=github.blog", app={
            "state": SimpleNamespace(settings=settings),
            "clickhouse": client, SOURCE_ICON_CACHE: cache,
        },
    )

    if allowed:
        response = await handle_source_icon(request)
        assert response.content_type == "image/png"
        assert response.body == cache.get.return_value
        cache.get.assert_awaited_once_with("github.blog")
    else:
        with pytest.raises(web.HTTPNotFound):
            await handle_source_icon(request)

        cache.get.assert_not_awaited()


@pytest.mark.parametrize("truncate", [False, True])
async def test_download_limits_apply_to_streamed_response(*, truncate: bool):
    async def chunks():
        yield b"1234"
        yield b"5678"

    response = AsyncMock()
    response.status = 200
    response.url = "https://blog.example/"
    response.raise_for_status = Mock()
    response.content.iter_chunked = Mock(return_value=chunks())
    response.__aenter__.return_value = response
    session = SimpleNamespace(get=Mock(return_value=response))

    if truncate:
        data, _ = await _fetch_bytes(
            session, response.url, 5,  # type: ignore[arg-type]
            truncate=True,
        )
        assert data == b"12345"
    else:
        with pytest.raises(ValueError, match="size limit"):
            await _fetch_bytes(
                session, response.url, 5,  # type: ignore[arg-type]
            )


async def test_capture_timeout_has_backoff_and_no_partial_file(tmp_path: Path):
    cache = SourceIconCache(tmp_path)

    with patch("dank.web.reader_source_icons._capture_icon", new=AsyncMock(
        side_effect=TimeoutError,
    )) as capture:
        assert await cache.get("github.blog") is None
        assert await cache.get("github.blog") is None

    capture.assert_awaited_once()
    assert not cache.directory.exists()
