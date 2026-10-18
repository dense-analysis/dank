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
    connect_saved_posts,
    conversation_capture,
    identifier,
)
from dank.web.reader_x_history import preserve_x_archives
from dank.web.reader_x_quotes import quote_target, quoted_post, saved_quotes
from dank.web.reader_x_saved import reply_metadata, saved_parents, saved_thread
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
    parents = await saved_parents(client, snapshots)
    saved: dict[tuple[str, str], tuple[list[dict[str, Any]], bool]] = {}

    if include_discussion:
        for post in rows:
            key = (post.domain, post.post_id)
            saved[key] = await saved_thread(
                client, post, snapshots.get(key, {}),
            )

    quote_nodes = {
        **_related_nodes(archives if include_discussion else {}, saved),
        **snapshots,
    }
    quote_keys = {
        (domain, target[0])
        for (domain, _), payload in quote_nodes.items()
        if (target := quote_target(payload))
    }
    quotes = await saved_quotes(client, quote_keys)
    related_assets = await _conversation_assets(
        client, archives if include_discussion else {}, saved, quote_keys,
    )

    return {
        (post.domain, post.post_id): _content(
            post,
            snapshots.get((post.domain, post.post_id), {}),
            assets.get((post.domain, post.post_id), [])
            + related_assets.get(post.domain, []),
            assets_dir, archives.get((post.domain, post.post_id), {}),
            include_discussion=include_discussion,
            parents=parents, quotes=quotes, quote_nodes=quote_nodes,
            saved=saved.get((post.domain, post.post_id), ([], False)),
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
    parents: dict[tuple[str, str], str],
    quotes: dict[tuple[str, str], dict[str, Any]],
    quote_nodes: dict[tuple[str, str], dict[str, Any]],
    saved: tuple[list[dict[str, Any]], bool],
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

    legacy = object_value(payload.get("legacy"))
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
        connect_saved_posts(
            conversation, post, payload, saved[0], local_urls,
            limited=saved[1],
        )
        for node in conversation["context"] + conversation["replies"]:
            node["x"].update(quoted_post(
                quote_nodes.get((post.domain, node["id"]), {}),
                post.domain, local_urls, quotes,
            ))

        summary["conversation"] = conversation

    relationship = reply_metadata(payload)
    reply = relationship.get("reply_to")

    if reply:
        reply["domain"] = post.domain
        parent_url = parents.get((post.domain, reply["id"]))

        if parent_url:
            reply.update(collected=True, url=parent_url)

    return {
        **summary, **relationship,
        **quoted_post(payload, post.domain, local_urls, quotes),
        "handle": handle,
        "name": name,
        "icon_url": icon,
        "links": post_links(payload, local_urls),
        **({"media": post_media(payload, local_urls)} if payload else {}),
        "reply_count": _count(legacy.get("reply_count")),
        "repost_count": _count(legacy.get("retweet_count")),
        "like_count": _count(legacy.get("favorite_count")),
        "view_count": _count(
            object_value(payload.get("views")).get("count"),
            allow_string=True,
        ),
        "captured_at": captured_at.isoformat() if payload else None,
    }


async def _conversation_assets(
    client: ClickHouseClient,
    snapshots: dict[tuple[str, str], dict[str, Any]],
    saved: dict[tuple[str, str], tuple[list[dict[str, Any]], bool]],
    quote_keys: set[tuple[str, str]],
) -> dict[str, list[AssetRow]]:
    keys = set(_related_nodes(snapshots, saved)) | quote_keys

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


def _related_nodes(
    snapshots: dict[tuple[str, str], dict[str, Any]],
    saved: dict[tuple[str, str], tuple[list[dict[str, Any]], bool]],
) -> dict[tuple[str, str], dict[str, Any]]:
    related: dict[tuple[str, str], dict[str, Any]] = {}

    for (domain, _), payload in snapshots.items():
        nodes = object_value(payload.get("conversation_capture")).get("posts")

        if isinstance(nodes, list):
            for raw in cast(list[object], nodes)[:MAX_CONVERSATION_POSTS]:
                node = object_value(raw)
                node_id = identifier(node.get("rest_id"))

                if node_id:
                    related.setdefault((domain, node_id), node)

    for (domain, _), (nodes, _) in saved.items():
        for node in nodes[:MAX_CONVERSATION_POSTS]:
            node_id = identifier(node.get("rest_id"))

            if node_id:
                related.setdefault((domain, node_id), node)

    return related


def _count(value: object, *, allow_string: bool = False) -> int | None:
    # X encodes view counts as decimal strings. Bound parsing to the length
    # of JavaScript's largest safe integer before returning a JSON number.
    if allow_string and isinstance(value, str):
        value = int(value) if re.fullmatch(r"[0-9]{1,16}", value) else None

    if isinstance(value, int) and not isinstance(value, bool):
        return value if 0 <= value < 2**53 else None

    return None
