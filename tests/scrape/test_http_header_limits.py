from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from http import HTTPStatus

import aiohttp
import pytest

from dank.config import ScrapeSettings
from dank.scrape.http import RequestLimiter, current_limiter, http_response
from tests.http import http_reply, local_http_client


@pytest.fixture
async def client() -> AsyncGenerator[aiohttp.ClientSession]:
    def respond(path: str) -> bytes:
        _, kind, size_text = path.split("/")
        size = int(size_text)
        headers = {"X-Large-Header": "x" * size}

        if kind == "redirect":
            return http_reply(
                HTTPStatus.FOUND, Location=f"/article/{size}", **headers,
            )

        return http_reply(HTTPStatus.OK, b"Article content", **headers)

    async with local_http_client(respond) as session:
        yield session


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
