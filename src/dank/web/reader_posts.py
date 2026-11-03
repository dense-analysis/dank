"""The JSON-shaped projection of collected posts, for both servers."""

from __future__ import annotations

import datetime as dt
import html
import json
import pathlib
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import urlsplit

from dank.html_utils import PAGE_CHALLENGE_PATTERN, remove_page_noise
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    AssetRow,
    PostRow,
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_communities import community_icons
from dank.web.reader_excerpt import search_excerpt
from dank.web.reader_hacker_news import add_hacker_news_metadata
from dank.web.reader_links import add_post_links
from dank.web.reader_provenance import add_collection_info
from dank.web.reader_reddit import (
    comment_capture,
    images,
    videos,
    without_video_posters,
)
from dank.web.reader_reddit_history import preserve_reddit_archives
from dank.web.reader_x import post_x_content
from dank.web.reader_youtube_feed import add_youtube_post_metadata


async def post_payloads(
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
