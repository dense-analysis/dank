import asyncio
import pathlib
from collections.abc import AsyncGenerator
from http import HTTPStatus

import aiohttp
import pytest

from dank.config import ScrapeSettings, SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics
from tests.http import http_reply, local_http_client

SIGNED_PATH = "/image.jpeg?crop=1%3A1%2Csmart&s=signature"


@pytest.fixture
async def client() -> AsyncGenerator[aiohttp.ClientSession]:
    def respond(path: str) -> bytes:
        if path == "/redirect":
            return http_reply(HTTPStatus.FOUND, Location=SIGNED_PATH)

        if path != SIGNED_PATH:
            return http_reply(HTTPStatus.FORBIDDEN)

        return http_reply(HTTPStatus.OK, b"image", **{
            "Content-Type": "image/jpeg",
        })

    async with local_http_client(respond) as session:
        yield session


async def test_default_client_reproduces_signature_failure(
    client: aiohttp.ClientSession,
) -> None:
    url = "http://external-preview.redd.it" + SIGNED_PATH

    async with client.get(url) as response:
        assert response.status == 403
        assert "crop=1:1,smart" in str(response.request_info.real_url)


@pytest.mark.parametrize(
    "host", ["external-preview.redd.it", "preview.redd.it"],
)
@pytest.mark.parametrize("redirect", [False, True])
@pytest.mark.parametrize("limited", [False, True])
async def test_signed_preview_download_preserves_query_bytes_and_redirects(
    client: aiohttp.ClientSession, memory_path: pathlib.Path, host: str,
    *, redirect: bool, limited: bool,
) -> None:
    url = f"http://{host}" + ("/redirect" if redirect else SIGNED_PATH)
    discovery = AssetDiscovery(
        "reddit", "reddit.com", "t3_one", url, "image", None,
    )
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    token = current_limiter.set(
        RequestLimiter(ScrapeSettings()) if limited else None,
    )

    try:
        result = await download_assets(
            [discovery], assets_dir=memory_path, http_client=client,
            max_asset_bytes=100, owners={discovery: stats},
        )
    finally:
        current_limiter.reset(token)

    assert result[0].url == url
    body = await asyncio.to_thread(
        pathlib.Path(result[0].local_path).read_bytes,
    )
    assert body == b"image"
    assert stats.counts["files_failed"] == 0
    assert stats.counts["files_downloaded"] == 1
    assert stats.counts["http_requests"] == (2 if redirect else 1)


async def test_genuine_preview_403_still_counts_as_failure(
    client: aiohttp.ClientSession, memory_path: pathlib.Path,
) -> None:
    discovery = AssetDiscovery(
        "reddit", "reddit.com", "t3_one",
        "http://external-preview.redd.it/image.jpeg?crop=wrong&s=invalid",
        "image", None,
    )
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=memory_path, http_client=client,
        max_asset_bytes=100, owners={discovery: stats},
    )
    assert result[0].local_path == ""
    assert stats.counts["files_failed"] == 1
    assert stats.counts["media_skipped"] == 0
