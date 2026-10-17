"""Version-matched X content for cards and the opened post."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
from typing import Any, cast
from urllib.parse import urlsplit

from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    AssetRow,
    PostRow,
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import (
    X_ACCOUNT_PATTERN,
    X_DOMAINS,
    profile_identity,
)
from dank.web.reader_x_conversation import (
    MAX_CONVERSATION_POSTS,
    conversation_capture,
    identifier,
)
from dank.web.reader_x_history import preserve_x_archives
from dank.x_content import object_value, post_links, post_media


async def post_x_content(
    client: ClickHouseClient,
    posts: list[PostRow],
    assets: dict[tuple[str, str], list[AssetRow]],
    assets_dir: pathlib.Path,
    *, include_discussion: bool = False,
) -> dict[tuple[str, str], dict[str, Any]]:
    rows = [post for post in posts if post.domain in X_DOMAINS]

    if not rows:
        return {}

    versions = [
        (
            post.domain,
            post.post_id,
            round(
                post.updated_at.replace(
                    tzinfo=post.updated_at.tzinfo or dt.UTC,
                ).timestamp()
                * 1000,
            ),
        )
        for post in rows
    ]
    result = await client.fetch_json(
        "SELECT domain, post_id, payload FROM raw_posts "
        "WHERE (domain, post_id, toUnixTimestamp64Milli(scraped_at)) "
        "IN %(x_versions)s ORDER BY scraped_at DESC "
        "LIMIT 1 BY domain, post_id",
        {"x_versions": versions},
    )
    snapshots = {
        (str(row["domain"]), str(row["post_id"])): _snapshot(
            row.get("payload"),
        )
        for row in result.rows
    }

    archives = await preserve_x_archives(client, rows, snapshots)
    related_assets = await _conversation_assets(
        client, archives,
    ) if include_discussion else {}

    return {
        (post.domain, post.post_id): _content(
            post,
            snapshots.get((post.domain, post.post_id), {}),
            assets.get((post.domain, post.post_id), [])
            + related_assets.get(post.domain, []),
            assets_dir, archives.get((post.domain, post.post_id), {}),
            include_discussion=include_discussion,
        )
        for post in rows
    }


def _snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}

    try:
        return object_value(json.loads(value))
    except ValueError:
        return {}


def _content(
    post: PostRow,
    payload: dict[str, Any],
    assets: list[AssetRow],
    assets_dir: pathlib.Path,
    conversation_payload: dict[str, Any],
    *, include_discussion: bool,
) -> dict[str, Any]:
    match = re.match(X_ACCOUNT_PATTERN, post.url.lower())
    handle = match[1] if match and match[1] != "i" else ""
    profile = object_value(
        object_value(payload.get("core")).get("user_results"),
    )
    name, icon = profile_identity(profile.get("result"), handle)
    local_urls: dict[str, str] = {}

    for asset in assets:
        view = _asset_view(asset, assets_dir=assets_dir)

        if (
            view
            and view["kind"] in {"image", "video"}
            and not urlsplit(
                view["url"],
            )
            .path.lower()
            .endswith(".m3u8")
        ):
            local_urls[asset.url] = view["url"]

    replies = object_value(payload.get("legacy")).get("reply_count")
    reply_count = (
        replies
        if isinstance(replies, int)
        and not isinstance(replies, bool)
        and 0 <= replies < 2**53
        else None
    )
    captured_at = post.updated_at.replace(
        tzinfo=post.updated_at.tzinfo or dt.UTC,
    )

    conversation = conversation_capture(
        post, conversation_payload, local_urls,
        include_posts=include_discussion,
    )
    summary = {
        "replies_status": conversation["status"],
        "replies_captured_count": conversation["captured_count"],
    }

    if include_discussion:
        summary["conversation"] = conversation

    return {
        **summary, "handle": handle,
        "name": name,
        "icon_url": icon,
        "links": post_links(payload, local_urls),
        **({"media": post_media(payload, local_urls)} if payload else {}),
        "reply_count": reply_count,
        "captured_at": captured_at.isoformat() if payload else None,
    }


async def _conversation_assets(
    client: ClickHouseClient,
    snapshots: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, list[AssetRow]]:
    keys: set[tuple[str, str]] = set()

    for (domain, _), payload in snapshots.items():
        capture = object_value(payload.get("conversation_capture"))
        posts = capture.get("posts")

        if not isinstance(posts, list):
            continue

        for raw in cast(list[object], posts)[:MAX_CONVERSATION_POSTS]:
            post_id = identifier(object_value(raw).get("rest_id"))

            if post_id:
                keys.add((domain, post_id))

    if not keys:
        return {}

    result = await client.fetch_json(
        "SELECT domain, post_id, url, local_path, content_type, size_bytes "
        "FROM assets FINAL WHERE (domain, post_id) IN %(x_related_keys)s",
        {"x_related_keys": sorted(keys)},
    )
    assets: dict[str, list[AssetRow]] = {}

    for row in result.rows:
        key = (str(row["domain"]), str(row["post_id"]))

        if key in keys:
            assets.setdefault(key[0], []).append(_parse_asset_row(row))

    return assets
