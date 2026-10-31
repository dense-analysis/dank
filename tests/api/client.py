"""HTTP clients for the FastAPI app, with and without its lifespan."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

BASE_URL = "http://dank.test"


def http_client(
    app: ASGIApp, *, raise_app_exceptions: bool = True,
) -> httpx.AsyncClient:
    # httpx's own ASGI transport, not Starlette's TestClient, which warns
    # when used with httpx. Neither runs the lifespan.
    transport = httpx.ASGITransport(
        app=app, raise_app_exceptions=raise_app_exceptions,
    )

    return httpx.AsyncClient(transport=transport, base_url=BASE_URL)


@asynccontextmanager
async def serving(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient]:
    """A client for an app whose lifespan has run, as under uvicorn."""
    async with app.router.lifespan_context(app) as state:

        async def with_state(
            scope: Scope, receive: Receive, send: Send,
        ) -> None:
            # uvicorn gives each request a shallow copy of lifespan state.
            await app({**scope, "state": dict(state or {})}, receive, send)

        async with http_client(with_state) as client:
            yield client
