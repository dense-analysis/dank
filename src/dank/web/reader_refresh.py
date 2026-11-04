"""aiohttp routes for refresh jobs, until DEN-51 removes them.

The queue, scope rules and workers live in dank.api.refresh.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

from aiohttp import web

from dank.api.refresh.clickhouse import ClickHouseRefreshTargets
from dank.api.refresh.manager import RefreshManager, running_refresh
from dank.api.refresh.service import RefreshService
from dank.api.refresh.worker import ProcessWorker
from dank.errors import TooManyRequests

REFRESH_MANAGER = web.AppKey("reader_refresh_manager", RefreshManager)


def add_refresh_routes(app: web.Application) -> None:
    app.cleanup_ctx.append(refresh_context)
    app.router.add_get("/api/reader/refresh", handle_refresh)
    app.router.add_post("/api/reader/refresh", handle_refresh)


async def refresh_context(app: web.Application) -> AsyncIterator[None]:
    async with running_refresh(
        app["state"].settings, ProcessWorker(),
    ) as manager:
        app[REFRESH_MANAGER] = manager
        yield


async def handle_refresh(request: web.Request) -> web.Response:
    manager = request.app[REFRESH_MANAGER]

    if request.method == "GET":
        return web.json_response({
            "jobs": [job._asdict() for job in reversed(manager.jobs())],
        })

    origin = request.headers.get("Origin")

    # The local HTTPS proxy checks the browser's https:// origin itself,
    # then rewrites it to http:// for this server, so compare with the
    # scheme this server sees rather than X-Forwarded-Proto.
    if request.headers.get("Sec-Fetch-Site") == "cross-site" or (
        origin is not None and origin != f"{request.scheme}://{request.host}"
    ):
        raise web.HTTPForbidden(reason="Same-origin request required")

    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType(reason="JSON request required")

    service = RefreshService(
        request.app["state"].settings,
        ClickHouseRefreshTargets(request.app["clickhouse"]),
        manager,
    )

    try:
        raw_body = await request.json()

        if not isinstance(raw_body, dict):
            raise ValueError("A refresh scope is required")

        body = cast(dict[str, Any], raw_body)

        if set(body) != {"scope"}:
            raise ValueError("A refresh scope is required")

        job = await service.start(body["scope"])
    except TooManyRequests as error:
        raise web.HTTPTooManyRequests(reason=str(error)) from error
    except (ValueError, TypeError) as error:
        raise web.HTTPBadRequest(reason=str(error)) from error

    return web.json_response({"job": job._asdict()}, status=202)
