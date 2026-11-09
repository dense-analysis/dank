"""Both servers side by side, for the parity tests.

Each starts for real, with its own startup work, over one seeded throwaway
database. Deleted with the aiohttp server in DEN-51.
"""

from __future__ import annotations

import pathlib
import tempfile
from collections.abc import AsyncGenerator, Awaitable, Mapping
from contextlib import asynccontextmanager
from typing import Any, NamedTuple, Protocol

import aiohttp
from aiohttp import web
from yarl import URL

from dank.api.app import create_app as create_api_app
from dank.config import Settings
from dank.web.app import create_app as create_aiohttp_app
from tests.api.client import BASE_URL, serving


class Get(Protocol):
    """A status and JSON for a target: a GET, or a POST of `post` if given."""

    def __call__(
        self, target: str, *, post: Any = None,
    ) -> Awaitable[tuple[int, Any]]: ...


class Exchange(NamedTuple):
    """A whole response, for routes that answer with more than JSON."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class Fetch(Protocol):
    def __call__(
        self,
        target: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
    ) -> Awaitable[Exchange]: ...


@asynccontextmanager
async def aiohttp_session(
    settings: Settings,
) -> AsyncGenerator[aiohttp.ClientSession]:
    runner = web.AppRunner(create_aiohttp_app(settings))
    # Runs the aiohttp startup contexts against the throwaway database.
    await runner.setup()

    try:
        # A Unix socket, since tests may only dial ClickHouse over TCP.
        with tempfile.TemporaryDirectory() as directory:
            path = str(pathlib.Path(directory) / "web.sock")
            await web.UnixSite(runner, path).start()
            connector = aiohttp.UnixConnector(path=path)

            async with aiohttp.ClientSession(connector=connector) as session:
                yield session
    finally:
        await runner.cleanup()


@asynccontextmanager
async def aiohttp_server(settings: Settings) -> AsyncGenerator[Get]:
    async with aiohttp_session(settings) as session:

        async def get(target: str, *, post: Any = None) -> tuple[int, Any]:
            url = BASE_URL + target

            async with (
                session.get(url) if post is None
                else session.post(url, json=post)
            ) as response:
                return response.status, await response.json()

        yield get


@asynccontextmanager
async def fastapi_server(settings: Settings) -> AsyncGenerator[Get]:
    async with serving(create_api_app(settings)) as client:

        async def get(target: str, *, post: Any = None) -> tuple[int, Any]:
            response = await (
                client.get(target) if post is None
                else client.post(target, json=post)
            )

            return response.status_code, response.json()

        yield get


@asynccontextmanager
async def both_servers(settings: Settings) -> AsyncGenerator[tuple[Get, Get]]:
    """(aiohttp, FastAPI) clients over the same database."""
    async with (
        aiohttp_server(settings) as old,
        fastapi_server(settings) as new,
    ):
        yield old, new


@asynccontextmanager
async def both_servers_whole(
    settings: Settings,
) -> AsyncGenerator[tuple[Fetch, Fetch]]:
    """(aiohttp, FastAPI) fetches of whole responses over the same database."""
    async with (
        aiohttp_session(settings) as session,
        serving(create_api_app(settings)) as client,
    ):

        async def old(
            target: str,
            *,
            method: str = "GET",
            headers: Mapping[str, str] | None = None,
        ) -> Exchange:
            async with session.request(
                method,
                # As sent, so that an escaped traversal reaches the server.
                URL(BASE_URL + target, encoded=True),
                headers=headers,
                allow_redirects=False,
            ) as response:
                return Exchange(
                    response.status,
                    {
                        key.lower(): value
                        for key, value in response.headers.items()
                    },
                    await response.read(),
                )

        async def new(
            target: str,
            *,
            method: str = "GET",
            headers: Mapping[str, str] | None = None,
        ) -> Exchange:
            response = await client.request(method, target, headers=headers)

            return Exchange(
                response.status_code, response.headers, response.content,
            )

        yield old, new
