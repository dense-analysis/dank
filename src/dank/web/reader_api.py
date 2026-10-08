"""JSON and static routes for the independent reader frontend."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from collections.abc import Awaitable, Callable
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from dank.html_utils import remove_page_noise
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    AppState,
    AssetRow,
    PostRow,
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _fetch_post,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
    _parse_post_row,  # pyright: ignore[reportPrivateUsage]
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _search_embedding,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_query import build_query, encode_cursor, parse_filters

logger = logging.getLogger(__name__)
READER_DIST = pathlib.Path(__file__).resolve().parents[3] / "reader" / "dist"


def add_reader_routes(app: web.Application) -> None:
    app.middlewares.append(reader_errors)
    app.router.add_get("/api/reader/sources", handle_sources)
    app.router.add_get("/api/reader/posts", handle_posts)
    app.router.add_get("/api/reader/post", handle_post)
    app.router.add_get("/reader", handle_reader)
    app.router.add_get("/reader/{path:.*}", handle_reader)


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

    response.headers["Cache-Control"] = "no-store"

    return response


async def handle_sources(request: web.Request) -> web.Response:
    state: AppState = request.app["state"]
    client: ClickHouseClient = request.app["clickhouse"]
    result = await client.fetch_json(
        "SELECT domain, count() AS count FROM posts FINAL GROUP BY domain",
    )
    counts = {str(row["domain"]): int(row["count"]) for row in result.rows}
    tags: dict[str, set[str]] = {}

    for source in state.settings.sources:
        tags.setdefault(source.domain, set()).update(source.tags)

    sources = [
        {"domain": domain, "name": domain,
         "tags": sorted(tags.get(domain, [])),
         "count": counts.get(domain, 0)}
        for domain in sorted(counts.keys() | tags.keys())
    ]

    return web.json_response({
        "sources": sources,
        "total_posts": sum(counts.values()),
    })


async def handle_posts(request: web.Request) -> web.Response:
    try:
        filters = parse_filters(request.query)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    state: AppState = request.app["state"]
    client: ClickHouseClient = request.app["clickhouse"]
    embedding = None

    if filters.q and filters.mode == "meaning":
        embedding = await _search_embedding(client, search_text=filters.q)

    query, params = build_query(filters, state.settings.sources, embedding)
    result = await client.fetch_json(query, params)
    posts = [_parse_post_row(row) for row in result.rows[:filters.limit]]
    has_more = len(result.rows) > filters.limit
    cursor = None

    if has_more and filters.sort != "relevance":
        last = posts[-1]
        cursor = encode_cursor(
            filters, last.created_at, last.domain, last.post_id,
        )

    return web.json_response({
        "posts": await _post_payloads(client, posts, state.assets_dir),
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

    posts = await _post_payloads(client, [post], state.assets_dir)

    return web.json_response(posts[0])


async def _post_payloads(
    client: ClickHouseClient, posts: list[PostRow], assets_dir: pathlib.Path,
) -> list[dict[str, Any]]:
    if not posts:
        return []

    result = await client.fetch_json(
        "SELECT domain, post_id, url, local_path, content_type, size_bytes "
        "FROM assets FINAL WHERE (domain, post_id) IN %(post_keys)s",
        {"post_keys": [(post.domain, post.post_id) for post in posts]},
    )
    assets: dict[tuple[str, str], list[AssetRow]] = {}

    for row in result.rows:
        key = (str(row["domain"]), str(row["post_id"]))
        assets.setdefault(key, []).append(_parse_asset_row(row))

    return [
        post_payload(
            post, assets.get((post.domain, post.post_id), []), assets_dir,
        )
        for post in posts
    ]


def post_payload(
    post: PostRow, assets: list[AssetRow], assets_dir: pathlib.Path,
) -> dict[str, Any]:
    source_url = _http_url(post.url) or ""
    body = _sanitize_html(remove_page_noise(post.html, base_url=source_url))
    media: list[dict[str, str]] = []

    for asset in assets:
        view = _asset_view(asset, assets_dir=assets_dir)

        if view and view["kind"] in {"image", "audio", "video"}:
            media.append({
                "url": view["url"], "content_type": view["content_type"],
            })

    thumbnail = next((
        item["url"] for item in media
        if item["content_type"].startswith("image/")
        and "svg" not in item["content_type"]
    ), None)

    if thumbnail is None:
        parser = _ThumbnailParser()
        parser.feed(body)
        thumbnail = parser.url

    created_at = post.created_at

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.UTC)

    return {
        "id": post.post_id, "domain": post.domain, "url": source_url,
        "author": post.author, "title": post.title,
        "excerpt": _summarize_html(body), "html": body,
        "created_at": created_at.isoformat(), "source": post.source,
        "thumbnail": thumbnail, "media": media,
    }


def _http_url(value: str) -> str | None:
    try:
        parsed = urlsplit(value)

        if parsed.scheme.lower() in {"http", "https"} and parsed.hostname:
            return value
    except ValueError:
        pass

    return None


class _ThumbnailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.url: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "img" or self.url:
            return

        value = dict(attrs).get("src") or ""

        if not _http_url(value):
            return

        path = urlsplit(value).path.lower()

        if not path.endswith(".svg") and "logo" not in path:
            self.url = _http_url(value)


def _reader_file(path: str) -> pathlib.Path | None:
    root = READER_DIST.resolve()
    candidate = (root / (path or "index.html")).resolve()

    if not candidate.is_relative_to(root):
        return None

    if candidate.is_file():
        return candidate

    # Only extensionless routes may fall back to the SPA; missing assets 404.
    if not pathlib.Path(path).suffix and (root / "index.html").is_file():
        return root / "index.html"

    return None


async def handle_reader(request: web.Request) -> web.StreamResponse:
    if request.path == "/reader":
        raise web.HTTPFound("/reader/")

    path = _reader_file(request.match_info.get("path", ""))

    if path is None:
        raise web.HTTPNotFound(text="Reader build or file not found.")

    response = web.FileResponse(path)

    if path.name == "index.html":
        response.headers["Cache-Control"] = "no-store"

    return response
