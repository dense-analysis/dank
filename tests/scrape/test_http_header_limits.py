from __future__ import annotations

import asyncio
import sys
import tempfile
from collections.abc import AsyncGenerator, Generator

import aiohttp
import pytest
from aiohttp import web

from dank.config import ScrapeSettings
from dank.scrape.http import RequestLimiter, current_limiter, http_response

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="HTTP fixture uses local Unix sockets",
)


@pytest.fixture
async def client() -> AsyncGenerator[aiohttp.ClientSession]:
    async def respond(request: web.Request) -> web.Response:
        size = int(request.match_info["size"])
        headers = {"X-Large-Header": "x" * size}

        if request.match_info["kind"] == "redirect":
            headers["Location"] = f"/article/{size}"

            return web.Response(status=302, headers=headers)

        return web.Response(text="Article content", headers=headers)

    app = web.Application()
    app.router.add_get("/{kind}/{size}", respond)
    runner = web.AppRunner(app)
    await runner.setup()

    # A short Unix socket path works on macOS and needs no network exception.
    with tempfile.TemporaryDirectory(prefix="dank-http-", dir="/tmp") as path:
        try:
            socket_path = path + "/http.sock"
            await web.UnixSite(runner, socket_path).start()

            async with aiohttp.ClientSession(
                connector=aiohttp.UnixConnector(path=socket_path),
            ) as session:
                yield session
        finally:
            await runner.cleanup()


@pytest.fixture(params=[False, True])
def limiter(request: pytest.FixtureRequest) -> Generator[None]:
    token = current_limiter.set(
        RequestLimiter(ScrapeSettings(
            http_concurrency=1, http_per_host=1,
        )) if request.param else None,
    )

    try:
        yield
    finally:
        current_limiter.reset(token)


async def test_reproduces_original_parser_limit(
    client: aiohttp.ClientSession,
) -> None:
    with pytest.raises(aiohttp.ClientResponseError, match="Header value"):
        async with client.get("http://fixture/article/8755"):
            pytest.fail("Default client accepted the oversized header")


@pytest.mark.usefixtures("limiter")
@pytest.mark.parametrize("kind", ["article", "redirect"])
@pytest.mark.parametrize("size", [8755, 16384])
async def test_large_headers_work_for_articles_and_redirects(
    client: aiohttp.ClientSession, kind: str, size: int,
) -> None:
    async with http_response(
        client, f"http://fixture/{kind}/{size}",
    ) as result:
        assert result.status == 200
        assert await result.text() == "Article content"
        assert result.url.path == f"/article/{size}"
        assert len(result.headers["X-Large-Header"]) == size


@pytest.mark.usefixtures("limiter")
@pytest.mark.parametrize("kind", ["article", "redirect"])
async def test_larger_fields_fail_and_release_http_capacity(
    client: aiohttp.ClientSession, kind: str,
) -> None:
    with pytest.raises(aiohttp.ClientResponseError, match="Header value"):
        async with http_response(client, f"http://fixture/{kind}/16385"):
            pytest.fail("Header limit was not enforced")

    async with (
        asyncio.timeout(1),
        http_response(client, "http://fixture/article/10") as result,
    ):
        assert result.status == 200
        assert await result.text() == "Article content"
