from __future__ import annotations

import datetime
import html
import pathlib
import textwrap
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, NamedTuple
from urllib.parse import quote, urlencode

import bleach
from aiohttp import web

from dank.config import Settings
from dank.storage.clickhouse import (
    ClickHouseClient,
    format_datetime,
    parse_datetime,
)

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080

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


def create_app(settings: Settings, *, page_size: int) -> web.Application:
    app = web.Application()
    static_dir = _static_dir()
    assets_dir = _assets_dir(settings)
    app["state"] = AppState(
        settings=settings,
        page_size=page_size,
        static_dir=static_dir,
        assets_dir=assets_dir,
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

    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store"

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
    cursor_created_at = _parse_cursor_datetime(
        request.query.get("cursor_created_at"),
    )
    cursor_post_id = request.query.get("cursor_post_id")

    posts = await _fetch_posts(
        client,
        limit=limit,
        cursor_created_at=cursor_created_at,
        cursor_post_id=cursor_post_id,
    )
    assets = await _fetch_assets(client, [post.post_id for post in posts])
    body = _render_index(
        posts,
        assets,
        assets_dir=state.assets_dir,
        limit=limit,
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
        post,
        assets.get(post.post_id, []),
        assets_dir=state.assets_dir,
    )

    return web.Response(text=body, content_type="text/html")


async def _fetch_posts(
    client: ClickHouseClient,
    *,
    limit: int,
    cursor_created_at: datetime.datetime | None,
    cursor_post_id: str | None,
) -> list[PostRow]:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source FROM posts FINAL"
    )

    if cursor_created_at is not None and cursor_post_id:
        query += (
            " WHERE created_at <= "
            f"{_format_datetime_literal(cursor_created_at)} "
            "AND post_id < "
            f"{quote_literal(cursor_post_id)}"
        )

    query += " ORDER BY created_at DESC, post_id DESC "
    query += f"LIMIT {int(limit)}"

    result = await client.fetch_json(query)

    return [_parse_post_row(row) for row in result.rows]


async def _fetch_post(
    client: ClickHouseClient,
    *,
    domain: str,
    post_id: str,
) -> PostRow | None:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source FROM posts FINAL "
        f"WHERE domain = {quote_literal(domain)} "
        f"AND post_id = {quote_literal(post_id)} "
    )
    result = await client.fetch_json(query)

    if not result.rows:
        return None

    return _parse_post_row(result.rows[0])


async def _fetch_assets(
    client: ClickHouseClient,
    post_ids: Iterable[str],
) -> dict[str, list[AssetRow]]:
    ids = [post_id for post_id in post_ids if post_id]

    if not ids:
        return {}

    literal_ids = ", ".join(quote_literal(post_id) for post_id in ids)
    query = (
        "SELECT post_id, url, local_path, content_type, size_bytes "
        f"FROM assets FINAL WHERE post_id IN ({literal_ids}) "
    )
    result = await client.fetch_json(query)
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
    posts: list[PostRow],
    assets: dict[str, list[AssetRow]],
    *,
    assets_dir: pathlib.Path,
    limit: int,
) -> str:
    title = "DANK Posts"
    items: list[str] = []

    for post in posts:
        summary = _summarize_html(post.html)
        item_assets = assets.get(post.post_id, [])
        items.append(
            _render_post_card(
                post,
                summary=summary,
                assets=item_assets,
                assets_dir=assets_dir,
            ),
        )

    next_link = ""

    if posts:
        cursor_post = posts[-1]
        cursor_created_at = _cursor_datetime(cursor_post.created_at)
        query = urlencode(
            {
                "limit": str(limit),
                "cursor_created_at": cursor_created_at,
                "cursor_post_id": cursor_post.post_id,
            },
        )
        next_link = (
            '<a class="pager-next" href="/?'
            + html.escape(query)
            + '">Next</a>'
        )

    body = "\n".join(items) or "<p>No posts yet.</p>"

    return _render_page(
        title,
        "\n".join(
            [
                '<section class="post-list">',
                body,
                "</section>",
                f'<div class="pager">{next_link}</div>',
            ],
        ),
    )


def _render_post_card(
    post: PostRow,
    *,
    summary: str,
    assets: list[AssetRow],
    assets_dir: pathlib.Path,
) -> str:
    title = html.escape(post.title or post.url)
    author = html.escape(post.author)
    domain = html.escape(post.domain)
    created_at = html.escape(_format_display_datetime(post.created_at))
    summary_text = html.escape(summary)
    detail_link = html.escape(
        "/post?" + urlencode({"domain": post.domain, "post_id": post.post_id}),
    )
    source_link = html.escape(post.url)
    assets_html = _render_assets(assets, assets_dir=assets_dir)

    return "\n".join(
        [
            '<article class="post-card">',
            '<header class="post-header">',
            f'<h2 class="post-title"><a href="{detail_link}">',
            f"{title}</a></h2>",
            '<div class="post-meta">',
            f'<span class="meta-item">{domain}</span>',
            f'<span class="meta-item">{created_at}</span>',
            f'<span class="meta-item">{author}</span>',
            f'<a class="meta-link" href="{source_link}" '
            'target="_blank" rel="noreferrer">'
            f'Source<span class="icon">↗</span></a>',
            "</div>",
            "</header>",
            f'<p class="post-summary">{summary_text}</p>',
            assets_html,
            "</article>",
        ],
    )


def _render_post_detail(
    post: PostRow,
    assets: list[AssetRow],
    *,
    assets_dir: pathlib.Path,
) -> str:
    title = html.escape(post.title or post.url)
    author = html.escape(post.author)
    created_at = html.escape(_format_display_datetime(post.created_at))
    source_link = html.escape(post.url)
    content = _sanitize_html(post.html)

    return _render_page(
        title,
        "\n".join(
            [
                '<article class="post-detail">',
                '<header class="post-header">',
                f'<h1 class="post-title">{title}</h1>',
                '<div class="post-meta">',
                f'<span class="meta-item">{created_at}</span>',
                f'<span class="meta-item">{author}</span>',
                f'<a class="meta-link" href="{source_link}" '
                'target="_blank" rel="noreferrer">'
                f'Source <span class="icon">↗</span></a>',
                "</div>",
                "</header>",
                f'<div class="post-body">{content}</div>',
                _render_assets(assets, assets_dir=assets_dir),
                '<div class="pager">',
                '<a class="pager-next" href="/">Back</a>',
                "</div>",
                "</article>",
            ],
        ),
    )


def _render_assets(
    assets: list[AssetRow],
    *,
    assets_dir: pathlib.Path,
) -> str:
    items: list[str] = []

    for asset in assets:
        local_url = _asset_local_url(asset, assets_dir)

        if not local_url:
            continue

        render_url = html.escape(local_url)
        content_type = asset.content_type
        type_attr = (
            f' type="{html.escape(content_type)}"' if content_type else ""
        )
        media = ""

        if _is_image_asset(asset, local_url):
            media = (
                '<a class="asset-link" '
                f'href="{render_url}">'
                f'<img src="{render_url}" alt="" />'
                "</a>"
            )
        elif _is_audio_asset(asset, local_url):
            media = (
                "<audio controls>"
                f'<source src="{render_url}"{type_attr} />'
                "</audio>"
            )
        elif _is_video_asset(asset, local_url):
            media = (
                "<video controls>"
                f'<source src="{render_url}"{type_attr} />'
                "</video>"
            )
        else:
            media = (
                f'<a class="asset-link" href="{render_url}">Local asset</a>'
            )

        items.append(f'<li class="asset-item">{media}</li>')

    if not items:
        return ""

    return "\n".join(
        [
            '<section class="assets">',
            "<h3>Assets</h3>",
            '<ul class="asset-list">',
            "\n".join(items),
            "</ul>",
            "</section>",
        ],
    )


def _render_page(title: str, body: str) -> str:
    return textwrap.dedent(f"""
    <!doctype html>
    <html lang="en">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>{html.escape(title)}</title>
            <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
            <link rel="stylesheet" href="/static/app.css" />
        </head>
        <body>
            <main class="page">
                <header class="page-header"><h1>DANK</h1></header>
                {body}
            </main>
        </body>
    </html>
    """).lstrip()  # noqa


def _sanitize_html(raw_html: str) -> str:
    return bleach.clean(
        raw_html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True,
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


def _format_datetime_literal(value: datetime.datetime) -> str:
    formatted = format_datetime(value)

    if formatted is None:
        formatted = format_datetime(datetime.datetime.now(datetime.UTC))

    return f"toDateTime64('{formatted}', 3, 'UTC')"


def _format_display_datetime(value: datetime.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC)

    return value.strftime("%Y-%m-%d %H:%M UTC")


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _assets_dir(settings: Settings) -> pathlib.Path:
    return settings.assets_dir.expanduser().resolve()


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
