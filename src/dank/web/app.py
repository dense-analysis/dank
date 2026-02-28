from __future__ import annotations

import asyncio
import datetime
import pathlib
import re
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Literal, NamedTuple
from urllib.parse import quote, urlencode

import bleach
from aiohttp import web
from jinja2 import Environment, FileSystemLoader, select_autoescape

from dank.config import Settings
from dank.embeddings import get_embedding_model
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
SEARCH_TITLE_WEIGHT = 0.65
"""The percentage of the weighting strength of title content."""
SEARCH_HTML_WEIGHT = 0.35
"""The percentage of the weighting strength of HTML content."""
SEARCH_MAXIMUM_DISTANCE = 1.0
"""Maximum cosine distance before exclusion. (bounded [0.0, 2.0])"""
SEARCH_FRESHNESS_WEIGHT = 0.3
"""
The weighting of freshness. (bounded [0.0, 2.0])

We apply: freshness_weight * exp(-days_ago / half_life_days)

"freshness" pulls content nearer if content matches weaker, but it is newer.
"""
SEARCH_FRESHNESS_HALF_LIFE_DAYS = 21.0
"""
The number of days of content age to measure freshness over.

We apply: freshness_weight * exp(-days_ago / half_life_days)

"freshness" pulls content nearer if content matches weaker, but it is newer.
"""
SEARCH_EMBEDDING_WEIGHT = 0.65
"""The contribution weight for semantic vector similarity."""
SEARCH_FULL_TEXT_WEIGHT = 0.35
"""The contribution weight for tokenized full-text matching."""
SEARCH_FULL_TEXT_TITLE_WEIGHT = 0.55
"""The contribution of title token matches to full-text score."""
SEARCH_FULL_TEXT_HTML_WEIGHT = 0.45
"""The contribution of HTML/body token matches to full-text score."""
SEARCH_TERM_PATTERN = re.compile(r"[0-9A-Za-z_]+")
"""Pattern used to split query text into search terms."""
SEARCH_MIN_TERM_LENGTH = 2
"""Minimum token length to include in full-text scoring."""

# Assert configuration of search parameters fit in bounds.
assert 0 <= SEARCH_TITLE_WEIGHT <= 1
assert 0 <= SEARCH_HTML_WEIGHT <= 1
assert 0 <= SEARCH_MAXIMUM_DISTANCE <= 2
assert 0 <= SEARCH_FRESHNESS_WEIGHT <= 2
assert SEARCH_FRESHNESS_HALF_LIFE_DAYS > 0
assert 0 <= SEARCH_EMBEDDING_WEIGHT <= 1
assert 0 <= SEARCH_FULL_TEXT_WEIGHT <= 1
assert 0 <= SEARCH_FULL_TEXT_TITLE_WEIGHT <= 1
assert 0 <= SEARCH_FULL_TEXT_HTML_WEIGHT <= 1
assert SEARCH_MIN_TERM_LENGTH > 0
assert abs((SEARCH_TITLE_WEIGHT + SEARCH_HTML_WEIGHT) - 1) < 1e-9
assert abs((SEARCH_EMBEDDING_WEIGHT + SEARCH_FULL_TEXT_WEIGHT) - 1) < 1e-9
assert (
    abs(
        (SEARCH_FULL_TEXT_TITLE_WEIGHT + SEARCH_FULL_TEXT_HTML_WEIGHT)
        - 1,
    )
    < 1e-9
)

DEFAULT_DAYS_BACK = 0
MAX_DAYS_BACK = 365

type CursorDirection = Literal["next", "prev"]

ALLOWED_TAGS = [
    "a",
    "blockquote",
    "br",
    "code",
    "del",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "strong",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
]
ALLOWED_ATTRIBUTES = {
    "a": ["href", "title", "rel"],
    "img": ["src", "alt", "title"],
    "code": ["class"],
    "pre": ["class"],
}


class PostRow(NamedTuple):
    domain: str
    post_id: str
    url: str
    author: str
    title: str
    html: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    source: str


class AssetRow(NamedTuple):
    post_id: str
    url: str
    local_path: str
    content_type: str
    size_bytes: int


class AppState(NamedTuple):
    settings: Settings
    page_size: int
    static_dir: pathlib.Path
    assets_dir: pathlib.Path
    templates: Environment


def create_app(settings: Settings, *, page_size: int) -> web.Application:
    app = web.Application()
    static_dir = _static_dir()
    assets_dir = _assets_dir(settings)
    templates = _template_environment()
    app["state"] = AppState(
        settings=settings,
        page_size=page_size,
        static_dir=static_dir,
        assets_dir=assets_dir,
        templates=templates,
    )
    app.cleanup_ctx.append(_clickhouse_context)
    app.middlewares.append(_no_cache_middleware)
    app.router.add_get("/", handle_index)
    app.router.add_get("/post", handle_post_detail)

    if static_dir.exists():
        app.router.add_static("/static/", static_dir, show_index=False)

    if assets_dir.exists():
        app.router.add_static("/assets/", assets_dir, show_index=False)

    return app


@web.middleware
async def _no_cache_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    response = await handler(request)
    content_type = response.headers.get("Content-Type", "").lower()

    if (
        request.path.startswith("/static/")
        or content_type.startswith("text/html")
    ):
        response.headers["Cache-Control"] = (
            "no-store, no-cache, must-revalidate, max-age=0"
        )
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    return response


async def _clickhouse_context(app: web.Application):
    state = app["state"]
    client = ClickHouseClient(state.settings.clickhouse)
    await client.__aenter__()
    app["clickhouse"] = client

    try:
        yield
    finally:
        await client.__aexit__(None, None, None)


async def handle_index(request: web.Request) -> web.Response:
    client = request.app["clickhouse"]
    state = request.app["state"]
    limit = _parse_limit(request.query.get("limit"), state.page_size)
    search_text = _parse_search_text(request.query.get("q"))
    domain_filter = _parse_filter_text(request.query.get("domain"))
    account_filter = _parse_filter_text(request.query.get("account"))
    days_back = _parse_days_back(request.query.get("days_back"))
    min_created_at = _minimum_created_at(days_back)
    cursor_created_at = _parse_cursor_datetime(
        request.query.get("cursor_created_at"),
    )
    cursor_post_id = request.query.get("cursor_post_id")
    cursor_direction = _parse_cursor_direction(
        request.query.get("cursor_direction"),
    )
    has_previous_page = False
    has_next_page = False


    if search_text:
        posts = await _search_posts(
            client,
            search_text=search_text,
            limit=limit,
            domain_filter=domain_filter,
            account_filter=account_filter,
            min_created_at=min_created_at,
        )
    else:
        posts = await _fetch_posts(
            client,
            limit=limit,
            cursor_created_at=cursor_created_at,
            cursor_post_id=cursor_post_id,
            cursor_direction=cursor_direction,
            domain_filter=domain_filter,
            account_filter=account_filter,
            min_created_at=min_created_at,
        )

        if posts:
            has_previous_page, has_next_page = await asyncio.gather(
                _has_newer_posts(
                    client,
                    cursor_created_at=posts[0].created_at,
                    cursor_post_id=posts[0].post_id,
                    domain_filter=domain_filter,
                    account_filter=account_filter,
                    min_created_at=min_created_at,
                ),
                _has_older_posts(
                    client,
                    cursor_created_at=posts[-1].created_at,
                    cursor_post_id=posts[-1].post_id,
                    domain_filter=domain_filter,
                    account_filter=account_filter,
                    min_created_at=min_created_at,
                ),
            )

    assets = await _fetch_assets(client, [post.post_id for post in posts])
    body = _render_index(
        state.templates,
        posts,
        assets,
        assets_dir=state.assets_dir,
        limit=limit,
        search_text=search_text,
        domain_filter=domain_filter,
        account_filter=account_filter,
        days_back=days_back,
        has_previous_page=has_previous_page,
        has_next_page=has_next_page,
    )

    return web.Response(text=body, content_type="text/html")


async def handle_post_detail(request: web.Request) -> web.Response:
    client = request.app["clickhouse"]
    state = request.app["state"]
    post_id = request.query.get("post_id")
    domain = request.query.get("domain")

    if not post_id or not domain:
        return web.Response(text="Missing post_id or domain", status=400)

    post = await _fetch_post(client, domain=domain, post_id=post_id)

    if post is None:
        return web.Response(text="Post not found", status=404)

    assets = await _fetch_assets(client, [post.post_id])
    body = _render_post_detail(
        state.templates,
        post,
        assets.get(post.post_id, []),
        assets_dir=state.assets_dir,
    )

    return web.Response(text=body, content_type="text/html")


async def _fetch_posts(
    clickhouse_client: ClickHouseClient,
    *,
    limit: int,
    cursor_created_at: datetime.datetime | None,
    cursor_post_id: str | None,
    cursor_direction: CursorDirection,
    domain_filter: str,
    account_filter: str,
    min_created_at: datetime.datetime | None,
) -> list[PostRow]:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source FROM posts FINAL"
    )
    params: dict[str, Any] = {"limit": int(limit)}
    conditions, params = _post_filter_conditions(
        params,
        domain_filter=domain_filter,
        account_filter=account_filter,
        min_created_at=min_created_at,
    )

    if cursor_created_at is not None and cursor_post_id:
        if cursor_direction == "prev":
            conditions.append(
                "(created_at > %(cursor_created_at)s "
                "OR (created_at = %(cursor_created_at)s "
                "AND post_id > %(cursor_post_id)s))",
            )
            query += _where_clause(conditions)
            query += " ORDER BY created_at ASC, post_id ASC "
            query += "LIMIT %(limit)s"
            params["cursor_created_at"] = cursor_created_at
            params["cursor_post_id"] = cursor_post_id
            result = await clickhouse_client.fetch_json(query, params)
            posts = [_parse_post_row(row) for row in result.rows]
            posts.reverse()

            return posts

        conditions.append(
            "(created_at < %(cursor_created_at)s "
            "OR (created_at = %(cursor_created_at)s "
            "AND post_id < %(cursor_post_id)s))",
        )
        params["cursor_created_at"] = cursor_created_at
        params["cursor_post_id"] = cursor_post_id

    query += _where_clause(conditions)
    query += " ORDER BY created_at DESC, post_id DESC "
    query += "LIMIT %(limit)s"

    result = await clickhouse_client.fetch_json(query, params)

    return [_parse_post_row(row) for row in result.rows]


async def _has_newer_posts(
    clickhouse_client: ClickHouseClient,
    *,
    cursor_created_at: datetime.datetime,
    cursor_post_id: str,
    domain_filter: str,
    account_filter: str,
    min_created_at: datetime.datetime | None,
) -> bool:
    query = "SELECT post_id FROM posts FINAL"
    params: dict[str, Any] = {
        "cursor_created_at": cursor_created_at,
        "cursor_post_id": cursor_post_id,
    }
    conditions, params = _post_filter_conditions(
        params,
        domain_filter=domain_filter,
        account_filter=account_filter,
        min_created_at=min_created_at,
    )
    conditions.append(
        "(created_at > %(cursor_created_at)s "
        "OR (created_at = %(cursor_created_at)s "
        "AND post_id > %(cursor_post_id)s))",
    )
    query += _where_clause(conditions)
    query += " ORDER BY created_at ASC, post_id ASC LIMIT 1"
    result = await clickhouse_client.fetch_json(
        query,
        params,
    )

    return bool(result.rows)


async def _has_older_posts(
    clickhouse_client: ClickHouseClient,
    *,
    cursor_created_at: datetime.datetime,
    cursor_post_id: str,
    domain_filter: str,
    account_filter: str,
    min_created_at: datetime.datetime | None,
) -> bool:
    query = "SELECT post_id FROM posts FINAL"
    params: dict[str, Any] = {
        "cursor_created_at": cursor_created_at,
        "cursor_post_id": cursor_post_id,
    }
    conditions, params = _post_filter_conditions(
        params,
        domain_filter=domain_filter,
        account_filter=account_filter,
        min_created_at=min_created_at,
    )
    conditions.append(
        "(created_at < %(cursor_created_at)s "
        "OR (created_at = %(cursor_created_at)s "
        "AND post_id < %(cursor_post_id)s))",
    )
    query += _where_clause(conditions)
    query += " ORDER BY created_at DESC, post_id DESC LIMIT 1"
    result = await clickhouse_client.fetch_json(
        query,
        params,
    )

    return bool(result.rows)


async def _search_posts(
    clickhouse_client: ClickHouseClient,
    *,
    search_text: str,
    limit: int,
    domain_filter: str,
    account_filter: str,
    min_created_at: datetime.datetime | None,
) -> list[PostRow]:
    embedding = await _search_embedding(
        clickhouse_client,
        search_text=search_text,
    )
    search_terms = _search_terms(search_text)
    search_term_count = len(search_terms)

    if embedding is None:
        return []

    params: dict[str, Any] = {
        "embedding": list(embedding),
        "search_terms": list(search_terms),
        "search_term_count": search_term_count,
        "search_embedding_weight": SEARCH_EMBEDDING_WEIGHT,
        "search_full_text_weight": SEARCH_FULL_TEXT_WEIGHT,
        "search_full_text_title_weight": SEARCH_FULL_TEXT_TITLE_WEIGHT,
        "search_full_text_html_weight": SEARCH_FULL_TEXT_HTML_WEIGHT,
        "title_weight": SEARCH_TITLE_WEIGHT,
        "html_weight": SEARCH_HTML_WEIGHT,
        "maximum_distance": SEARCH_MAXIMUM_DISTANCE,
        "freshness_weight": SEARCH_FRESHNESS_WEIGHT,
        "freshness_half_life_days": SEARCH_FRESHNESS_HALF_LIFE_DAYS,
        "limit": int(limit),
    }
    conditions, params = _post_filter_conditions(
        params,
        domain_filter=domain_filter,
        account_filter=account_filter,
        min_created_at=min_created_at,
    )
    where_clause = _where_clause(
        [
            "length(title_embedding) > 0",
            "length(html_embedding) > 0",
            *conditions,
        ],
    )

    query = rf"""
        SELECT
            *,
            (
                (
                    embedding_similarity * %(search_embedding_weight)s
                    + full_text_score * %(search_full_text_weight)s
                ) + (
                    %(freshness_weight)s
                    * exp(-age_days / %(freshness_half_life_days)s)
                )
            ) AS score
        FROM (
            SELECT
                *,
                (
                    cosineDistance(title_embedding, %(embedding)s)
                        * %(title_weight)s
                    + cosineDistance(html_embedding, %(embedding)s)
                        * %(html_weight)s
                ) AS embedding_distance,
                (
                    greatest(
                        1 - least(embedding_distance, 2.0) / 2.0,
                        0.0
                    )
                ) AS embedding_similarity,
                (
                    if(
                        %(search_term_count)s = 0,
                        0.0,
                        toFloat64(
                            arrayCount(
                                term -> (
                                    positionCaseInsensitive(title, term) > 0
                                ),
                                %(search_terms)s
                            )
                        ) / %(search_term_count)s
                    )
                ) AS title_text_score,
                (
                    if(
                        %(search_term_count)s = 0,
                        0.0,
                        toFloat64(
                            arrayCount(
                                term -> (
                                    positionCaseInsensitive(html, term) > 0
                                ),
                                %(search_terms)s
                            )
                        ) / %(search_term_count)s
                    )
                ) AS html_text_score,
                (
                    title_text_score * %(search_full_text_title_weight)s
                    + html_text_score * %(search_full_text_html_weight)s
                ) AS full_text_score,
                (
                    greatest(
                        dateDiff('second', created_at, now64(3)),
                        0
                    ) / 86400.0
                ) AS age_days
            FROM posts FINAL
            {where_clause}
        )
        WHERE (
            embedding_distance <= %(maximum_distance)s
            OR full_text_score > 0
        )
        ORDER BY score DESC
        LIMIT %(limit)s
    """
    result = await clickhouse_client.fetch_json(query, params)

    return [_parse_post_row(row) for row in result.rows]


async def _search_embedding(
    clickhouse_client: ClickHouseClient,
    *,
    search_text: str,
) -> tuple[float, ...] | None:
    embedder = get_embedding_model()
    model_name = embedder.model_name
    cached_embedding = await _load_cached_embedding(
        clickhouse_client,
        model_name=model_name,
        search_text=search_text,
    )

    if cached_embedding is not None:
        return cached_embedding

    embeddings = await asyncio.to_thread(embedder.embed_texts, [search_text])

    if not embeddings:
        return None

    embedding = embeddings[0]
    await _store_cached_embedding(
        clickhouse_client,
        model_name=model_name,
        search_text=search_text,
        embedding=embedding,
    )

    return embedding


async def _load_cached_embedding(
    clickhouse_client: ClickHouseClient,
    *,
    model_name: str,
    search_text: str,
) -> tuple[float, ...] | None:
    query = (
        "SELECT embedding FROM web_embedding_cache FINAL "
        "WHERE model_name = %(model_name)s "
        "AND search_text = %(search_text)s "
        "ORDER BY created_at DESC "
        "LIMIT 1"
    )
    result = await clickhouse_client.fetch_json(
        query,
        {
            "model_name": model_name,
            "search_text": search_text,
        },
    )

    # Convert the embedding from a list of numbers to a tuple of floats.
    return (
        tuple(float(item) for item in result.rows[0]["embedding"])
        if result.rows else
        None
    )


async def _store_cached_embedding(
    clickhouse_client: ClickHouseClient,
    *,
    model_name: str,
    search_text: str,
    embedding: tuple[float, ...],
) -> None:
    row = {
        "model_name": model_name,
        "search_text": search_text,
        "embedding": [float(value) for value in embedding],
        "created_at": datetime.datetime.now(datetime.UTC),
    }

    await clickhouse_client.insert_rows("web_embedding_cache", [row])


async def _fetch_post(
    clickhouse_client: ClickHouseClient,
    *,
    domain: str,
    post_id: str,
) -> PostRow | None:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source FROM posts FINAL "
        "WHERE domain = %(domain)s "
        "AND post_id = %(post_id)s "
    )
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "post_id": post_id},
    )

    if not result.rows:
        return None

    return _parse_post_row(result.rows[0])


async def _fetch_assets(
    clickhouse_client: ClickHouseClient,
    post_ids: Iterable[str],
) -> dict[str, list[AssetRow]]:
    ids = [post_id for post_id in post_ids if post_id]

    if not ids:
        return {}

    query = (
        "SELECT post_id, url, local_path, content_type, size_bytes "
        "FROM assets FINAL WHERE post_id IN %(post_ids)s "
    )
    result = await clickhouse_client.fetch_json(query, {"post_ids": ids})
    assets: dict[str, list[AssetRow]] = {}

    for row in result.rows:
        asset = _parse_asset_row(row)
        assets.setdefault(asset.post_id, []).append(asset)

    return assets


def _parse_post_row(row: dict[str, Any]) -> PostRow:
    created_at = parse_datetime(row.get("created_at"))
    updated_at = parse_datetime(row.get("updated_at"))

    if created_at is None:
        created_at = datetime.datetime.now(datetime.UTC)

    if updated_at is None:
        updated_at = created_at

    return PostRow(
        domain=str(row.get("domain", "")),
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        author=str(row.get("author", "")),
        title=str(row.get("title", "")),
        html=str(row.get("html", "")),
        created_at=created_at,
        updated_at=updated_at,
        source=str(row.get("source", "")),
    )


def _parse_asset_row(row: dict[str, Any]) -> AssetRow:
    size = row.get("size_bytes")
    if not isinstance(size, int):
        size = 0

    return AssetRow(
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        local_path=str(row.get("local_path", "")),
        content_type=str(row.get("content_type", "")),
        size_bytes=size,
    )


def _render_index(
    templates: Environment,
    posts: list[PostRow],
    assets: dict[str, list[AssetRow]],
    *,
    assets_dir: pathlib.Path,
    limit: int,
    search_text: str,
    domain_filter: str,
    account_filter: str,
    days_back: int,
    has_previous_page: bool,
    has_next_page: bool,
) -> str:
    previous_link = ""
    next_link = ""

    if posts and not search_text and has_previous_page:
        cursor_post = posts[0]
        cursor_created_at = _cursor_datetime(cursor_post.created_at)
        params = _list_query_params(
            limit=limit,
            domain_filter=domain_filter,
            account_filter=account_filter,
            days_back=days_back,
        )
        params.update(
            {
                "cursor_created_at": cursor_created_at,
                "cursor_post_id": cursor_post.post_id,
                "cursor_direction": "prev",
            },
        )
        previous_link = "/?" + urlencode(params)

    if posts and not search_text and has_next_page:
        cursor_post = posts[-1]
        cursor_created_at = _cursor_datetime(cursor_post.created_at)
        params = _list_query_params(
            limit=limit,
            domain_filter=domain_filter,
            account_filter=account_filter,
            days_back=days_back,
        )
        params.update(
            {
                "cursor_created_at": cursor_created_at,
                "cursor_post_id": cursor_post.post_id,
                "cursor_direction": "next",
            },
        )
        next_link = "/?" + urlencode(params)

    rendered_posts = [
        _post_view(post, assets.get(post.post_id, []), assets_dir=assets_dir)
        for post in posts
    ]

    return _render_template(
        templates,
        "index.html",
        title="DANK Posts",
        posts=rendered_posts,
        search_text=search_text,
        domain_filter=domain_filter,
        account_filter=account_filter,
        days_back=days_back,
        days_back_label=_days_back_label(days_back),
        limit=limit,
        previous_link=previous_link,
        next_link=next_link,
    )


def _render_post_detail(
    templates: Environment,
    post: PostRow,
    assets: list[AssetRow],
    *,
    assets_dir: pathlib.Path,
) -> str:
    return _render_template(
        templates,
        "post_detail.html",
        title=post.title or post.url,
        post={
            "title": post.title or post.url,
            "author": post.author,
            "created_at": _format_display_datetime(post.created_at),
            "source_link": post.url,
            "body_html": _sanitize_html(post.html),
        },
        assets=[
            asset
            for asset in (
                _asset_view(item, assets_dir=assets_dir)
                for item in assets
            )
            if asset is not None
        ],
    )


def _render_template(
    templates: Environment,
    template_name: str,
    **context: Any,
) -> str:
    template = templates.get_template(template_name)

    return template.render(**context)


def _post_view(
    post: PostRow,
    assets: list[AssetRow],
    *,
    assets_dir: pathlib.Path,
) -> dict[str, Any]:
    return {
        "title": post.title or post.url,
        "author": post.author,
        "domain": post.domain,
        "created_at": _format_display_datetime(post.created_at),
        "summary": _summarize_html(post.html),
        "detail_link": "/post?" + urlencode(
            {"domain": post.domain, "post_id": post.post_id},
        ),
        "source_link": post.url,
        "assets": [
            asset
            for asset in (
                _asset_view(item, assets_dir=assets_dir)
                for item in assets
            )
            if asset is not None
        ],
    }


def _asset_view(
    asset: AssetRow,
    *,
    assets_dir: pathlib.Path,
) -> dict[str, str] | None:
    local_url = _asset_local_url(asset, assets_dir)

    if not local_url:
        return None

    kind = "file"

    if _is_image_asset(asset, local_url):
        kind = "image"
    elif _is_audio_asset(asset, local_url):
        kind = "audio"
    elif _is_video_asset(asset, local_url):
        kind = "video"

    return {
        "kind": kind,
        "url": local_url,
        "content_type": asset.content_type,
    }


def _sanitize_html(raw_html: str) -> str:
    cleaned = bleach.clean(
        raw_html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True,
    )

    return bleach.linkify(
        cleaned,
        parse_email=False,
        skip_tags={"code", "pre"},
    )


def _summarize_html(raw_html: str, *, limit: int = 280) -> str:
    text = bleach.clean(raw_html, tags=[], attributes={}, strip=True)
    text = " ".join(text.split())

    if len(text) <= limit:
        return text

    trimmed = text[:limit].rsplit(" ", 1)[0]

    return f"{trimmed}..."


def _parse_limit(value: str | None, default: int) -> int:
    if value is None:
        return default

    try:
        parsed = int(value)
    except ValueError:
        return default

    if parsed <= 0:
        return default

    return min(parsed, MAX_PAGE_SIZE)


def _parse_days_back(value: str | None) -> int:
    if value is None:
        return DEFAULT_DAYS_BACK

    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_DAYS_BACK

    if parsed < 0:
        return DEFAULT_DAYS_BACK

    return min(parsed, MAX_DAYS_BACK)


def _minimum_created_at(days_back: int) -> datetime.datetime | None:
    if days_back <= 0:
        return None

    return datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        days=days_back,
    )


def _days_back_label(days_back: int) -> str:
    if days_back <= 0:
        return "∞"

    return str(days_back)


def _list_query_params(
    *,
    limit: int,
    domain_filter: str,
    account_filter: str,
    days_back: int,
) -> dict[str, str]:
    params: dict[str, str] = {
        "limit": str(limit),
        "days_back": str(days_back),
    }

    if domain_filter:
        params["domain"] = domain_filter

    if account_filter:
        params["account"] = account_filter

    return params


def _where_clause(conditions: list[str]) -> str:
    if not conditions:
        return ""

    return " WHERE " + " AND ".join(conditions)


def _post_filter_conditions(
    params: dict[str, Any],
    *,
    domain_filter: str,
    account_filter: str,
    min_created_at: datetime.datetime | None,
) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []

    if domain_filter:
        conditions.append("domain = %(domain_filter)s")
        params["domain_filter"] = domain_filter

    if account_filter:
        conditions.append(
            "positionCaseInsensitive(author, %(account_filter)s) > 0",
        )
        params["account_filter"] = account_filter

    if min_created_at is not None:
        conditions.append("created_at >= %(min_created_at)s")
        params["min_created_at"] = min_created_at

    return conditions, params


def _parse_cursor_datetime(value: str | None) -> datetime.datetime | None:
    if not value:
        return None

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)

    return parsed


def _cursor_datetime(value: datetime.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)

    return value.isoformat()


def _format_display_datetime(value: datetime.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC)

    return value.strftime("%Y-%m-%d %H:%M UTC")


def _assets_dir(settings: Settings) -> pathlib.Path:
    return (settings.data_dir / "assets").expanduser().resolve()


def _template_environment() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(_template_dir())),
        autoescape=select_autoescape(("html", "xml")),
    )


def _template_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent / "templates"


def _static_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[3] / "static"


def _asset_local_url(asset: AssetRow, assets_dir: pathlib.Path) -> str | None:
    if not asset.local_path:
        return None

    path = pathlib.Path(asset.local_path).expanduser()

    try:
        resolved = path.resolve()
    except FileNotFoundError:
        return None

    if not resolved.is_file():
        return None

    try:
        relative = resolved.relative_to(assets_dir)
    except ValueError:
        return None

    return "/assets/" + quote(relative.as_posix(), safe="/")


def _is_image_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("image/"):
        return True

    return _has_extension(url, (".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _is_audio_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("audio/"):
        return True

    return _has_extension(url, (".mp3", ".wav", ".ogg", ".m4a"))


def _is_video_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("video/"):
        return True

    return _has_extension(url, (".mp4", ".webm"))


def _has_extension(value: str, extensions: tuple[str, ...]) -> bool:
    trimmed = value.split("?", 1)[0].lower()

    return trimmed.endswith(extensions)


def _search_terms(value: str) -> tuple[str, ...]:
    seen: set[str] = set()
    terms: list[str] = []

    for raw_term in SEARCH_TERM_PATTERN.findall(value.lower()):
        if len(raw_term) < SEARCH_MIN_TERM_LENGTH:
            continue

        if raw_term in seen:
            continue

        seen.add(raw_term)
        terms.append(raw_term)

    return tuple(terms)


def _parse_search_text(value: str | None) -> str:
    if value is None:
        return ""

    return value.strip()


def _parse_filter_text(value: str | None) -> str:
    if value is None:
        return ""

    return value.strip()


def _parse_cursor_direction(value: str | None) -> CursorDirection:
    if value == "prev":
        return "prev"

    return "next"
