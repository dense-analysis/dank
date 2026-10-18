"""Look up saved X relationships independently of discussion capture."""

from __future__ import annotations

import json
from typing import Any

from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import PostRow
from dank.web.reader_x_conversation import (
    MAX_CONVERSATION_POSTS,
    canonical_target,
    identifier,
)
from dank.x_content import object_value


def reply_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    legacy = object_value(payload.get("legacy"))
    parent = identifier(legacy.get("in_reply_to_status_id_str"))
    conversation = identifier(legacy.get("conversation_id_str"))
    result: dict[str, Any] = {}

    if conversation:
        result["conversation_id"] = conversation

    if parent and parent != payload.get("rest_id"):
        author = identifier(object_value(object_value(object_value(
            payload.get("core"),
        ).get("user_results")).get("result")).get("rest_id"))
        parent_author = identifier(legacy.get("in_reply_to_user_id_str"))
        result["reply_to"] = {
            "id": parent, "domain": "x.com",
            "url": f"https://x.com/i/status/{parent}",
            "is_self": author == parent_author if (
                author and parent_author
            ) else None,
            "collected": False,
        }

    return result


async def saved_parents(
    client: ClickHouseClient,
    snapshots: dict[tuple[str, str], dict[str, Any]],
) -> dict[tuple[str, str], str]:
    keys = {
        (domain, reply["id"])
        for (domain, _), payload in snapshots.items()
        if (reply := reply_metadata(payload).get("reply_to"))
    }

    if not keys:
        return {}

    result = await client.fetch_json(
        "SELECT domain, post_id, url FROM posts FINAL "
        "WHERE (domain, post_id) IN %(x_parent_keys)s",
        {"x_parent_keys": sorted(keys)},
    )

    return {
        (str(row["domain"]), str(row["post_id"])): str(row["url"])
        for row in result.rows
        if (str(row["domain"]), str(row["post_id"])) in keys
    }


async def saved_thread(
    client: ClickHouseClient, post: PostRow, payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    if not canonical_target(post) or payload.get("rest_id") != post.post_id:
        return [], False

    legacy = object_value(payload.get("legacy"))
    conversation = identifier(legacy.get("conversation_id_str"))

    if not conversation:
        return [], False

    parent = identifier(legacy.get("in_reply_to_status_id_str")) or ""
    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = %(x_thread_domain)s AND NOT is_reference "
        "AND (domain, post_id, toUnixTimestamp64Milli(scraped_at)) IN "
        "(SELECT domain, post_id, toUnixTimestamp64Milli(updated_at) "
        "FROM posts FINAL WHERE domain = %(x_thread_domain)s) "
        "AND JSONExtractString(payload, 'legacy', 'conversation_id_str') "
        "= %(x_thread_root)s AND post_id != %(x_thread_target)s "
        "ORDER BY (post_id IN %(x_thread_parents)s) DESC, "
        "post_created_at ASC, post_id ASC, scraped_at DESC "
        "LIMIT 1 BY post_id LIMIT %(x_thread_limit)s",
        {"x_thread_domain": post.domain, "x_thread_root": conversation,
         "x_thread_target": post.post_id,
         "x_thread_parents": [parent, conversation],
         "x_thread_limit": MAX_CONVERSATION_POSTS + 1},
    )
    nodes: list[dict[str, Any]] = []

    for row in result.rows[:MAX_CONVERSATION_POSTS]:
        try:
            node = object_value(json.loads(row.get("payload", "")))
        except (TypeError, ValueError):
            continue

        if node.get("rest_id") == row.get("post_id"):
            nodes.append(node)

    return nodes, len(result.rows) > MAX_CONVERSATION_POSTS
