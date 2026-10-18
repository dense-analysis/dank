"""Quoted-post cards prefer embedded snapshots over saved fallbacks."""

from __future__ import annotations

import json
from typing import Any

from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_x_conversation import identifier
from dank.web.reader_x_post import captured_post, post_handle
from dank.x_content import object_value


def quote_target(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    legacy = object_value(payload.get("legacy"))
    declared_id = identifier(legacy.get("quoted_status_id_str"))
    node = object_value(object_value(payload.get("quoted_status_result")).get(
        "result",
    ))

    # Visibility wrappers are not post bodies. Do not traverse other relations.
    for _ in range(3):
        if identifier(node.get("rest_id")):
            break

        node = object_value(node.get("tweet") or node.get("result"))

    node_id = identifier(node.get("rest_id"))
    quote_id = declared_id or node_id

    if not quote_id or quote_id == payload.get("rest_id"):
        return None

    if (
        node_id != quote_id or node.get("__typename", "Tweet") != "Tweet"
        or object_value(node.get("legacy")).get("id_str", quote_id) != quote_id
        or not post_handle(node)
    ):
        node = {}

    return quote_id, node


async def saved_quotes(
    client: ClickHouseClient, keys: set[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not keys:
        return {}

    result = await client.fetch_json(
        "SELECT domain, post_id, payload FROM raw_posts "
        "WHERE NOT is_reference AND "
        "(domain, post_id, toUnixTimestamp64Milli(scraped_at)) IN "
        "(SELECT domain, post_id, toUnixTimestamp64Milli(updated_at) "
        "FROM posts FINAL WHERE (domain, post_id) IN %(x_quote_keys)s) "
        "ORDER BY scraped_at DESC LIMIT 1 BY domain, post_id",
        {"x_quote_keys": sorted(keys)},
    )
    saved: dict[tuple[str, str], dict[str, Any]] = {}

    for row in result.rows:
        key = (str(row.get("domain")), str(row.get("post_id")))

        if key not in keys:
            continue

        try:
            node = object_value(json.loads(row.get("payload", "")))
        except (TypeError, ValueError):
            continue

        if (
            node.get("rest_id") == key[1] and post_handle(node)
            and node.get("__typename", "Tweet") == "Tweet"
            and object_value(node.get("legacy")).get(
                "id_str", key[1],
            ) == key[1]
        ):
            saved[key] = node

    return saved


def quoted_post(
    payload: dict[str, Any], domain: str, local_urls: dict[str, str],
    saved: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    target = quote_target(payload)

    if not target:
        return {}

    quote_id, embedded = target
    node = embedded or saved.get((domain, quote_id), {})
    quote: dict[str, Any] = {
        "id": quote_id, "domain": domain,
        "url": f"https://x.com/i/status/{quote_id}",
        "collected": (domain, quote_id) in saved,
    }

    if node:
        # Show one embedded level; deeper quotes open as separate posts.
        quote["post"] = captured_post(node, domain, local_urls)
        quote["url"] = quote["post"]["url"]

    return {"quote": quote}
