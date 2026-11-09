"""JSON and static routes for the independent reader frontend."""

from __future__ import annotations

import logging
import pathlib
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from dank.api.files.model import client_route, reader_location
from dank.api.source_icons.model import source_icon_url
from dank.api.sources.model import source_kind
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    AppState,
    _fetch_post,  # pyright: ignore[reportPrivateUsage]
    _parse_post_row,  # pyright: ignore[reportPrivateUsage]
    _search_embedding,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import source_accounts
from dank.web.reader_blogs import external_blog_count
from dank.web.reader_communities import community_icons, community_totals
from dank.web.reader_connections import handle_connection, handle_connections
from dank.web.reader_flairs import reddit_flairs
from dank.web.reader_posts import post_payloads
from dank.web.reader_query import (
    REDDIT_COMMUNITY_SQL,
    build_query,
    encode_cursor,
    parse_filters,
)
from dank.web.reader_refresh import add_refresh_routes
from dank.web.reader_x_authors import handle_x_authors
from dank.web.reader_x_home import HOME_CONDITION, encode_home_cursor
from dank.web.reader_youtube import add_youtube_routes
from dank.web.sources import current_sources

logger = logging.getLogger(__name__)
READER_DIST = pathlib.Path(__file__).resolve().parents[3] / "reader" / "dist"


def add_reader_routes(app: web.Application) -> None:
    app.middlewares.append(reader_errors)
    add_youtube_routes(app)
    add_refresh_routes(app)
    app.router.add_get("/api/reader/connections", handle_connections)
    app.router.add_get("/api/reader/connection", handle_connection)
    app.router.add_get("/api/reader/sources", handle_sources)
    app.router.add_get("/api/reader/categories", handle_categories)
    app.router.add_get("/api/reader/x/authors", handle_x_authors)
    app.router.add_get("/api/reader/posts", handle_posts)
    app.router.add_get("/api/reader/post", handle_post)
    app.router.add_get("/reader", handle_old_reader_address)
    app.router.add_get("/reader/{path:.*}", handle_old_reader_address)


def add_reader_app_route(app: web.Application) -> None:
    """Serves the built reader from /. Add it last: it matches every path."""
    app.router.add_get("/{path:.*}", handle_reader)


@web.middleware
async def reader_errors(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    if not request.path.startswith("/api/reader/"):
        return await handler(request)

    try:
        response = await handler(request)
    except web.HTTPException as error:
        response = web.json_response(
            {"error": error.reason}, status=error.status,
        )
    except Exception:
        logger.exception("Reader request failed")
        response = web.json_response(
            {"error": "The collection is unavailable. Please try again."},
            status=503,
        )

    if request.path != "/api/reader/source-icon" or response.status != 200:
        response.headers["Cache-Control"] = "no-store"

    return response


async def handle_categories(request: web.Request) -> web.Response:
    state: AppState = request.app["state"]

    return web.json_response({
        "categories": [
            category.name for category in state.settings.categories.definitions
        ],
    })


async def handle_sources(request: web.Request) -> web.Response:
    state: AppState = request.app["state"]
    client: ClickHouseClient = request.app["clickhouse"]
    result = await client.fetch_json(
        "SELECT domain, count() AS count FROM posts FINAL GROUP BY domain",
    )
    counts = {str(row["domain"]): int(row["count"]) for row in result.rows}
    configured = await current_sources(request)
    tags: dict[str, set[str]] = {}

    for source in configured:
        tags.setdefault(source.domain, set()).update(source.tags)

    sources: list[dict[str, Any]] = [
        {"domain": domain,
         "name": "Hacker News" if domain == "news.ycombinator.com" else domain,
         "kind": source_kind(domain),
         "icon_url": source_icon_url(domain),
         "configured": domain in tags,
         "tags": sorted(tags.get(domain, [])),
         "count": counts.get(domain, 0)}
        for domain in sorted(counts.keys() | tags.keys())
    ]
    names = (
        urlsplit(url).path.split("/")[2]
        for source in configured if source.domain == "reddit.com"
        for url in source.feed_urls
    )
    communities = {name.lower(): name for name in names}

    if communities:
        metadata = await _reddit_source_metadata(
            client, communities, state.assets_dir,
        )

        for source in sources:
            if source["domain"] == "reddit.com":
                source.update(kind="reddit", **metadata)

    for source in sources:
        if source["kind"] == "x":
            accounts = tuple(
                account for setting in configured
                if setting.domain == source["domain"]
                for account in setting.accounts
            )
            source["accounts"] = await source_accounts(
                client, source["domain"], accounts,
            )
            home = await client.fetch_json(
                "SELECT uniqExact(post_id) AS x_home_count FROM raw_posts "
                "WHERE domain = %(domain)s AND " + HOME_CONDITION,
                {"domain": source["domain"]},
            )
            source["for_you_count"] = (
                int(home.rows[0]["x_home_count"]) if home.rows else 0
            )

    return web.json_response({
        "sources": sources,
        "total_posts": sum(counts.values()),
        "blog_external_count": await external_blog_count(client),
        "in_database": state.settings.sources_in_database,
    })


async def _reddit_source_metadata(
    client: ClickHouseClient,
    communities: dict[str, str],
    assets_dir: pathlib.Path,
) -> dict[str, Any]:
    result = await client.fetch_json(
        f"SELECT {REDDIT_COMMUNITY_SQL} AS community, count() AS count "
        "FROM posts FINAL WHERE domain = 'reddit.com' GROUP BY community",
    )
    counts = {
        str(row["community"]): int(row["count"]) for row in result.rows
    }
    icons = await community_icons(client, list(communities), assets_dir)
    totals = await community_totals(client, list(communities))
    flairs = await reddit_flairs(client)
    global_flairs: Counter[str] = Counter()

    for labels in flairs.values():
        for flair in labels:
            global_flairs[flair["name"]] += flair["count"]

    return {
        "communities": [
            {"name": name, "count": counts.get(key, 0),
             "icon_url": icons.get(key), "flairs": flairs.get(key, []),
             "post_total": totals.get(key)}
            for key, name in sorted(communities.items())
        ],
        "flairs": [
            {"name": name, "count": count}
            for name, count in sorted(global_flairs.items())
        ],
    }


async def handle_posts(request: web.Request) -> web.Response:
    try:
        filters = parse_filters(request.query)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    state: AppState = request.app["state"]
    client: ClickHouseClient = request.app["clickhouse"]
    embedding = None

    if filters.q and filters.mode in {"meaning", "combined"}:
        embedding = await _search_embedding(client, search_text=filters.q)

    query, params = build_query(
        filters, await current_sources(request), embedding,
    )
    result = await client.fetch_json(query, params)
    posts = [_parse_post_row(row) for row in result.rows[:filters.limit]]
    has_more = len(result.rows) > filters.limit
    cursor = None

    if has_more and filters.sort != "relevance":
        last = posts[-1]
        if filters.x_feed:
            cursor = encode_home_cursor(
                filters, result.rows[filters.limit - 1],
                params["x_home_snapshot"],
            )
        else:
            cursor = encode_cursor(
                filters, last.created_at, last.domain, last.post_id,
            )

    return web.json_response({
        "posts": await post_payloads(
            client, posts, state.assets_dir, filters.q,
        ),
        "next_cursor": cursor,
        "limited": has_more and filters.sort == "relevance",
    })


async def handle_post(request: web.Request) -> web.Response:
    domain = request.query.get("domain", "").strip()
    post_id = request.query.get("id", "").strip()

    if not domain or not post_id:
        return web.json_response(
            {"error": "domain and id are required"}, status=400,
        )

    client: ClickHouseClient = request.app["clickhouse"]
    state: AppState = request.app["state"]
    post = await _fetch_post(client, domain=domain, post_id=post_id)

    if post is None:
        return web.json_response({"error": "Article not found"}, status=404)

    posts = await post_payloads(
        client, [post], state.assets_dir, include_discussion=True,
    )

    return web.json_response(posts[0])


def _reader_file(path: str) -> pathlib.Path | None:
    root = READER_DIST.resolve()
    candidate = (root / (path or "index.html")).resolve()

    if not candidate.is_relative_to(root):
        return None

    if candidate.is_file():
        return candidate

    # Only the app's own routes fall back to it; missing bundles 404.
    if client_route(path) and (root / "index.html").is_file():
        return root / "index.html"

    return None


async def handle_old_reader_address(request: web.Request) -> web.Response:
    # Relative, because behind the HTTPS proxy this server sees plain HTTP.
    raise web.HTTPFound(reader_location(
        request.match_info.get("path", ""), request.rel_url.raw_query_string,
    ))


async def handle_reader(request: web.Request) -> web.StreamResponse:
    path = _reader_file(request.match_info.get("path", ""))

    if path is None:
        raise web.HTTPNotFound(text="Reader build or file not found.")

    response = web.FileResponse(path)

    if path.name == "index.html":
        response.headers["Cache-Control"] = "no-store"

    return response
