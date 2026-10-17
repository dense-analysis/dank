import asyncio
import pathlib
import sys
import tempfile
from collections.abc import AsyncGenerator

import aiohttp
import pytest
from aiohttp import web

from dank.config import ScrapeSettings, SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="HTTP fixture uses local Unix sockets",
)
SIGNED_PATH = "/image.jpeg?crop=1%3A1%2Csmart&s=signature"


@pytest.fixture
async def client() -> AsyncGenerator[aiohttp.ClientSession]:
    async def respond(request: web.Request) -> web.Response:
        if request.path == "/redirect":
            return web.Response(status=302, headers={"Location": SIGNED_PATH})

        if request.raw_path != SIGNED_PATH:
            return web.Response(status=403)

        return web.Response(body=b"image", content_type="image/jpeg")

    app = web.Application()
    app.router.add_get("/{path:.*}", respond)
    runner = web.AppRunner(app)
    await runner.setup()

    with tempfile.TemporaryDirectory(prefix="dank-media-", dir="/tmp") as path:
        try:
            socket_path = path + "/http.sock"
            await web.UnixSite(runner, socket_path).start()

            async with aiohttp.ClientSession(
                connector=aiohttp.UnixConnector(path=socket_path),
            ) as session:
                yield session
        finally:
            await runner.cleanup()


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
    client: aiohttp.ClientSession, tmp_path: pathlib.Path, host: str,
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
            [discovery], assets_dir=tmp_path, http_client=client,
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
    client: aiohttp.ClientSession, tmp_path: pathlib.Path,
) -> None:
    discovery = AssetDiscovery(
        "reddit", "reddit.com", "t3_one",
        "http://external-preview.redd.it/image.jpeg?crop=wrong&s=invalid",
        "image", None,
    )
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=client,
        max_asset_bytes=100, owners={discovery: stats},
    )
    assert result[0].local_path == ""
    assert stats.counts["files_failed"] == 1
    assert stats.counts["media_skipped"] == 0
