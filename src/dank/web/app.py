# Other modules import these private helpers until DEN-51 removes this file.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import datetime
import pathlib
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, NamedTuple, cast
from urllib.parse import quote

import bleach
from aiohttp import web

from dank.api.posts.embeddings import CachedSearchEmbeddings
from dank.api.posts.model import (
    AssetRow as AssetRow,
    PostRow as PostRow,
)
from dank.api.viewer.model import (
    MAX_PAGE_SIZE as MAX_PAGE_SIZE,
    index_filters,
)
from dank.config import Settings
from dank.embeddings import get_embedding_model
from dank.errors import InvalidInput, NotFound
from dank.html_utils import html_text, remove_page_noise
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

if TYPE_CHECKING:
    from dank.api.viewer.pages import ViewerPages
    from dank.api.viewer.service import ViewerService

DEFAULT_PAGE_SIZE = 50
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080

ALLOWED_TAGS = [
    "a",
    "blockquote",
    "br",
    "caption",
    "code",
    "col",
    "colgroup",
    "del",
    "em",
    "figcaption",
    "figure",
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
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
]
ALLOWED_ATTRIBUTES = {
    "a": ["href", "title", "rel"],
    "img": ["src", "alt", "title"],
    "code": ["class", "data-lang", "data-language"],
    "pre": ["class", "data-lang", "data-language"],
    "td": ["colspan", "rowspan"],
    "th": ["colspan", "rowspan", "scope", "abbr"],
    "col": ["span"],
    "colgroup": ["span"],
}


class AppState(NamedTuple):
    settings: Settings
    page_size: int
    static_dir: pathlib.Path
    assets_dir: pathlib.Path


def create_app(settings: Settings, *, page_size: int) -> web.Application:
    from dank.web.reader_api import add_reader_routes
    from dank.web.reader_source_icons import add_source_icon_routes

    app = web.Application()
    static_dir = _static_dir()
    assets_dir = assets_path(settings)
    # Keep assets discovered after the first startup immediately readable.
    assets_dir.mkdir(parents=True, exist_ok=True)
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
    add_reader_routes(app)
    add_source_icon_routes(app)

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
    from dank.web.startup import prepare_postgres

    state = app["state"]
    # First, so a bad schema stops startup before any long backfills.
    await prepare_postgres(state.settings.postgres)
    client = ClickHouseClient(state.settings.clickhouse)
    await client.__aenter__()
    app["clickhouse"] = client

    try:
        from dank.web.startup import prepare_clickhouse

        await prepare_clickhouse(client)
        yield
    finally:
        await client.__aexit__(None, None, None)


def _viewer(request: web.Request) -> tuple[ViewerService, ViewerPages]:
    # Imported here because the viewer's adapters use this module's helpers.
    from dank.api.viewer.clickhouse import ClickHouseViewerStore
    from dank.api.viewer.pages import ViewerPages
    from dank.api.viewer.service import ViewerService

    store = ClickHouseViewerStore(
        request.app["clickhouse"],
        CachedSearchEmbeddings(request.app["clickhouse"], get_embedding_model),
    )
    service = ViewerService(store, lambda: datetime.datetime.now(datetime.UTC))

    return service, ViewerPages(request.app["state"].assets_dir)


async def handle_index(request: web.Request) -> web.Response:
    service, pages = _viewer(request)
    filters = index_filters(
        request.query, page_size=request.app["state"].page_size,
    )
    page = await service.index(filters)

    return web.Response(
        text=pages.index(filters, page), content_type="text/html",
    )


async def handle_post_detail(request: web.Request) -> web.Response:
    service, pages = _viewer(request)

    try:
        page = await service.detail(
            request.query.get("domain"), request.query.get("post_id"),
        )
    except InvalidInput as error:
        return web.Response(text=str(error), status=400)
    except NotFound as error:
        return web.Response(text=str(error), status=404)

    return web.Response(text=pages.post(page), content_type="text/html")


async def _search_embedding(
    clickhouse_client: ClickHouseClient,
    *,
    search_text: str,
) -> tuple[float, ...] | None:
    return await CachedSearchEmbeddings(
        clickhouse_client, get_embedding_model,
    ).embed(search_text)


async def _fetch_post(
    clickhouse_client: ClickHouseClient,
    *,
    domain: str,
    post_id: str,
) -> PostRow | None:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source, categories FROM posts FINAL "
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
        categories=tuple(
            (str(cast(dict[str, Any], item)["name"]),
             float(cast(dict[str, Any], item)["confidence"]),
             float(cast(dict[str, Any], item)["similarity"]))
            if isinstance(item, dict)
            else (str(item[0]), float(item[1]), float(item[2]))
            for item in cast(list[Any], row.get("categories", []))
        ),
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
    return bleach.clean(
        remove_page_noise(raw_html),
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True,
    )


def _summarize_html(raw_html: str, *, limit: int = 280) -> str:
    text = html_text(raw_html)
    text = " ".join(text.split())

    if len(text) <= limit:
        return text

    trimmed = text[:limit].rsplit(" ", 1)[0]

    return f"{trimmed}..."


def assets_path(settings: Settings) -> pathlib.Path:
    return (settings.data_dir / "assets").expanduser().resolve()


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
