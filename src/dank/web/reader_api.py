"""JSON and static routes for the independent reader frontend."""

from __future__ import annotations

import datetime as dt
import html
import json
import logging
import pathlib
from collections import Counter
from collections.abc import Awaitable, Callable
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import urlsplit

from aiohttp import web

from dank.html_utils import PAGE_CHALLENGE_PATTERN, remove_page_noise
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
from dank.web.reader_accounts import (
    source_accounts,
    source_kind,
)
from dank.web.reader_blogs import external_blog_count
from dank.web.reader_communities import community_icons, community_totals
from dank.web.reader_connections import handle_connection, handle_connections
from dank.web.reader_excerpt import search_excerpt
from dank.web.reader_flairs import reddit_flairs
from dank.web.reader_hacker_news import add_hacker_news_metadata
from dank.web.reader_links import add_post_links
from dank.web.reader_provenance import add_collection_info
from dank.web.reader_query import (
    REDDIT_COMMUNITY_SQL,
    build_query,
    encode_cursor,
    parse_filters,
)
from dank.web.reader_reddit import (
    comment_capture,
    images,
    videos,
    without_video_posters,
)
from dank.web.reader_reddit_history import preserve_reddit_archives
from dank.web.reader_refresh import add_refresh_routes
from dank.web.reader_source_icons import source_icon_url
from dank.web.reader_x import post_x_content
from dank.web.reader_youtube import add_youtube_routes
from dank.web.reader_youtube_feed import add_youtube_post_metadata

logger = logging.getLogger(__name__)
READER_DIST = pathlib.Path(__file__).resolve().parents[3] / "reader" / "dist"


def add_reader_routes(app: web.Application) -> None:
    app.middlewares.append(reader_errors)
    add_youtube_routes(app)
    add_refresh_routes(app)
    app.router.add_get("/api/reader/connections", handle_connections)
    app.router.add_get("/api/reader/connection", handle_connection)
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

    if request.path != "/api/reader/source-icon" or response.status != 200:
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
        for source in state.settings.sources if source.domain == "reddit.com"
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
            configured = tuple(
                account for setting in state.settings.sources
                if setting.domain == source["domain"]
                for account in setting.accounts
            )
            source["accounts"] = await source_accounts(
                client, source["domain"], configured,
            )

    return web.json_response({
        "sources": sources,
        "total_posts": sum(counts.values()),
        "blog_external_count": await external_blog_count(client),
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
        "posts": await _post_payloads(
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

    posts = await _post_payloads(
        client, [post], state.assets_dir, include_discussion=True,
    )

    return web.json_response(posts[0])


async def _post_payloads(
    client: ClickHouseClient, posts: list[PostRow], assets_dir: pathlib.Path,
    query: str = "",
    *,
    include_discussion: bool = False,
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

    incomplete = await _incomplete_page_keys(client, posts)

    payloads = [
        post_payload(
            post, assets.get((post.domain, post.post_id), []), assets_dir,
            query,
            collection_incomplete=(post.domain, post.post_id) in incomplete,
        )
        for post in posts
    ]
    x_content = await post_x_content(
        client, posts, assets, assets_dir,
        include_discussion=include_discussion,
    )

    for post in payloads:
        content = x_content.get((post["domain"], post["id"]))

        if content:
            post["x"] = content

    await _add_reddit_metadata(
        client, payloads, assets_dir, posts, assets,
        include_discussion=include_discussion,
    )

    await add_hacker_news_metadata(
        client, payloads, posts, include_discussion=include_discussion,
    )

    await add_youtube_post_metadata(client, payloads, posts)
    await add_post_links(client, payloads, query)
    await add_collection_info(client, payloads, posts)

    return payloads


async def _add_reddit_metadata(
    client: ClickHouseClient, payloads: list[dict[str, Any]],
    assets_dir: pathlib.Path,
    rows: list[PostRow],
    assets: dict[tuple[str, str], list[AssetRow]],
    *,
    include_discussion: bool,
) -> None:
    reddit = [post for post in payloads if post["domain"] == "reddit.com"]

    if not reddit:
        return

    # Match the processed article version: a pending scrape must not replace
    # its discussion with a newer, unprocessed capture.
    versions = [
        (post.post_id, round(post.updated_at.replace(
            tzinfo=post.updated_at.tzinfo or dt.UTC,
        ).timestamp() * 1000))
        for post in rows if post.domain == "reddit.com"
    ]
    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = 'reddit.com' "
        "AND (post_id, toUnixTimestamp64Milli(scraped_at)) "
        "IN %(reddit_versions)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        {"reddit_versions": versions},
    )
    snapshots = {
        str(row["post_id"]): _reddit_snapshot(row.get("payload"))
        for row in result.rows
    }
    snapshots = await preserve_reddit_archives(
        client, snapshots, versions, include_discussion=include_discussion,
    )

    for post in reddit:
        snapshot = snapshots.get(post["id"], {})
        path = urlsplit(str(post["url"])).path.split("/")
        community = path[2] if len(path) > 3 and path[1] == "r" else ""
        flair = snapshot.get("link_flair_text")
        post["reddit"] = {
            "community": community,
            "score": _reddit_count(snapshot.get("score")),
            "comments": _reddit_count(snapshot.get("num_comments"), minimum=0),
            "flair": flair.strip() if isinstance(flair, str) else None,
        }
        external_url = _http_url(str(snapshot.get("url") or ""))

        if (external_url and snapshot.get("is_self") is not True
                and external_url.rstrip("/") != str(post["url"]).rstrip("/")):
            post["reddit"]["external_url"] = external_url

        _add_reddit_archive(
            post, snapshot, assets.get(("reddit.com", post["id"]), []),
            assets_dir, include_discussion=include_discussion,
        )

    icons = await community_icons(
        client, [post["reddit"]["community"] for post in reddit], assets_dir,
    )

    for post in reddit:
        post["reddit"]["icon_url"] = icons.get(
            post["reddit"]["community"].lower(),
        )


def _add_reddit_archive(
    post: dict[str, Any], snapshot: dict[str, Any], assets: list[AssetRow],
    assets_dir: pathlib.Path, *, include_discussion: bool,
) -> None:
    local_urls: dict[str, str] = {}

    for asset in assets:
        view = _asset_view(asset, assets_dir=assets_dir)

        if view and view["kind"] in {"image", "video"}:
            # A previously downloaded HLS manifest is not an offline video.
            if urlsplit(view["url"]).path.lower().endswith(".m3u8"):
                continue

            local_urls[asset.url] = view["url"]

    media = videos(snapshot, local_urls)
    posters = {video["poster_url"] for video in media if video["poster_url"]}

    if not post["thumbnail"] and posters:
        post["thumbnail"] = next(iter(posters))

    post["reddit"]["video"] = media[0] if media else None
    post["reddit"]["videos"] = media
    image_media = images(snapshot, local_urls)

    if image_media:
        post["reddit"]["images"] = image_media

    if not include_discussion:
        return

    post["reddit"]["comment_capture"] = comment_capture(snapshot, post["url"])
    if media:
        posters.update(
            original for original, local in local_urls.items()
            if local in posters
        )
        post["html"] = without_video_posters(
            post["html"], posters,
        )


def _reddit_snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}

    try:
        payload = json.loads(value)
    except (ValueError, TypeError):
        return {}

    return cast(dict[str, Any], payload) if isinstance(payload, dict) else {}


def _reddit_count(
    value: object, *, minimum: int = -(2 ** 53 - 1),
) -> int | None:
    return (
        value if isinstance(value, int) and not isinstance(value, bool)
        and minimum <= value < 2 ** 53
        else None
    )


async def _incomplete_page_keys(
    client: ClickHouseClient, posts: list[PostRow],
) -> set[tuple[str, str]]:
    versions: list[tuple[str, str, int]] = []

    for post in posts:
        if post.source not in {"rss", "linked_article"}:
            continue

        timestamp = post.updated_at

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.UTC)

        # Plain datetime bindings discard ClickHouse's millisecond precision.
        versions.append((post.domain, post.post_id,
                         round(timestamp.timestamp() * 1000)))

    if not versions:
        return set()

    # Describe the displayed capture, even if a newer retry awaits processing.
    result = await client.fetch_json(
        "SELECT domain, post_id FROM raw_posts "
        "WHERE (domain, post_id, toUnixTimestamp64Milli(scraped_at)) "
        "IN %(post_versions)s "
        "AND source IN ('rss', 'linked_article') "
        "AND (JSONExtractString(payload, 'page_fetch_status') "
        "IN ('failed', 'unsupported') "
        "OR match(JSONExtractString(payload, 'page_html'), "
        "%(challenge_pattern)s))",
        {"post_versions": versions,
         "challenge_pattern": PAGE_CHALLENGE_PATTERN},
    )

    return {(str(row["domain"]), str(row["post_id"])) for row in result.rows}


def post_payload(
    post: PostRow, assets: list[AssetRow], assets_dir: pathlib.Path,
    query: str = "",
    *,
    collection_incomplete: bool = False,
) -> dict[str, Any]:
    source_url = _http_url(post.url) or ""
    body = _sanitize_html(remove_page_noise(post.html, base_url=source_url))
    media: list[dict[str, str]] = []
    local_urls: dict[str, str] = {}

    for asset in assets:
        view = _asset_view(asset, assets_dir=assets_dir)

        if view and view["kind"] in {"image", "audio", "video"}:
            local_urls[asset.url] = view["url"]
            media.append({
                "url": view["url"], "content_type": view["content_type"],
            })

    images = _ArticleImages(local_urls)
    images.feed(body)

    for original, replacement in images.replacements.items():
        body = body.replace(original, replacement)

    created_at = post.created_at

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.UTC)

    result: dict[str, Any] = {
        "id": post.post_id, "domain": post.domain, "url": source_url,
        "author": post.author, "title": post.title,
        "excerpt": search_excerpt(body, query) or _summarize_html(body),
        "html": body,
        "collection_incomplete": collection_incomplete,
        "created_at": created_at.isoformat(), "source": post.source,
        "thumbnail": images.thumbnail, "media": media,
    }


    if post.source == "linked_article":
        # Article conversion falls back to capture time for an unknown date.
        result["published_at"] = (
            None if post.created_at == post.updated_at
            else created_at.isoformat()
        )

    return result

def _http_url(value: str) -> str | None:
    try:
        parsed = urlsplit(value)

        if parsed.scheme.lower() in {"http", "https"} and parsed.hostname:
            return value
    except ValueError:
        pass

    return None


class _ArticleImages(HTMLParser):
    def __init__(self, local_urls: dict[str, str]) -> None:
        super().__init__()
        self.local_urls = local_urls
        self.replacements: dict[str, str] = {}
        self.thumbnail: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "img":
            return

        value = dict(attrs).get("src") or ""
        local_url = self.local_urls.get(value)

        if local_url:
            # Rewrite only image tags; preserve all other sanitized markup.
            rewritten = [
                (key, local_url if key == "src" else text)
                for key, text in attrs
            ]
            attributes = "".join(
                f' {key}="{html.escape(text, quote=True)}"'
                for key, text in rewritten if text is not None
            )
            self.replacements[self.get_starttag_text() or ""] = (
                f"<img{attributes}>"
            )

        if self.thumbnail is None and _is_lead_image(value):
            self.thumbnail = local_url or value


def _is_lead_image(value: str) -> bool:
    if not _http_url(value):
        return False

    url = urlsplit(value)
    path = url.path.lower()

    return not path.endswith(".svg") and not any(
        marker in path
        for marker in (
            "logo", "avatar", "profile", "author", "advert", "/ads/",
        )
    ) and "gravatar" not in url.netloc.lower()


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
