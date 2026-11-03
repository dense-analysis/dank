"""Both servers side by side, for the parity tests.

Each starts for real, with its own startup work, over one seeded throwaway
database. Deleted with the aiohttp server in DEN-51.
"""

from __future__ import annotations

import pathlib
import tempfile
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
from aiohttp import web

from dank.api.app import create_app as create_api_app
from dank.config import Settings
from dank.web.app import create_app as create_aiohttp_app
from tests.api.client import BASE_URL, serving

type Get = Callable[[str], Awaitable[tuple[int, Any]]]


@asynccontextmanager
async def aiohttp_server(settings: Settings) -> AsyncGenerator[Get]:
    runner = web.AppRunner(create_aiohttp_app(settings, page_size=50))
    # Runs the aiohttp startup contexts against the throwaway database.
    await runner.setup()

    try:
        # A Unix socket, since tests may only dial ClickHouse over TCP.
        with tempfile.TemporaryDirectory() as directory:
            path = str(pathlib.Path(directory) / "web.sock")
            await web.UnixSite(runner, path).start()
            connector = aiohttp.UnixConnector(path=path)

            async with aiohttp.ClientSession(connector=connector) as session:

                async def get(target: str) -> tuple[int, Any]:
                    async with session.get(BASE_URL + target) as response:
                        return response.status, await response.json()

                yield get
    finally:
        await runner.cleanup()


@asynccontextmanager
async def fastapi_server(settings: Settings) -> AsyncGenerator[Get]:
    async with serving(create_api_app(settings, page_size=50)) as client:

        async def get(target: str) -> tuple[int, Any]:
            response = await client.get(target)

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
