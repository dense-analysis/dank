"""Retain useful X archives without crossing the displayed post version."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.app import PostRow
from dank.web.reader_x_conversation import canonical_target, valid_capture
from dank.x_content import object_value


async def preserve_x_archives(
    client: ClickHouseClient, posts: list[PostRow],
    snapshots: dict[tuple[str, str], dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    candidates = {
        (post.domain, post.post_id): post for post in posts
        if canonical_target(post) and object_value(snapshots.get(
            (post.domain, post.post_id), {},
        ).get("conversation_capture")).get("status") not in (
            "captured", "partial",
        )
    }

    if not candidates:
        return snapshots

    clauses: list[str] = []
    params: dict[str, object] = {}

    for index, ((domain, post_id), post) in enumerate(candidates.items()):
        clauses.append(
            f"(domain = %(x_domain_{index})s "
            f"AND post_id = %(x_post_{index})s "
            f"AND toUnixTimestamp64Milli(scraped_at) < %(x_before_{index})s)",
        )
        params[f"x_domain_{index}"] = domain
        params[f"x_post_{index}"] = post_id
        params[f"x_before_{index}"] = round(post.updated_at.replace(
            tzinfo=post.updated_at.tzinfo or dt.UTC,
        ).timestamp() * 1000)

    result = await client.fetch_json(
        "SELECT domain, post_id, payload, scraped_at FROM raw_posts WHERE ("
        + " OR ".join(clauses) + ") AND JSONExtractString(payload, "
        "'conversation_capture', 'status') IN ('captured', 'partial') "
        "ORDER BY scraped_at DESC LIMIT 1 BY domain, post_id",
        params,
    )
    preserved = dict(snapshots)

    for row in result.rows:
        _preserve_row(row, candidates, preserved)

    return preserved


def _preserve_row(
    row: dict[str, Any], candidates: dict[tuple[str, str], PostRow],
    preserved: dict[tuple[str, str], dict[str, Any]],
) -> None:
    key = (str(row.get("domain")), str(row.get("post_id")))
    post = candidates.get(key)
    timestamp = parse_datetime(row.get("scraped_at"))

    if post is None or timestamp is None:
        return

    timestamp = timestamp.replace(tzinfo=timestamp.tzinfo or dt.UTC)
    version = post.updated_at.replace(tzinfo=post.updated_at.tzinfo or dt.UTC)

    if timestamp >= version:
        return

    try:
        old = object_value(json.loads(row.get("payload", "")))
    except (TypeError, ValueError):
        return

    capture = object_value(old.get("conversation_capture"))

    if capture.get("status") not in ("captured", "partial") or not (
        valid_capture(post, old, capture)
    ):
        return

    current = preserved.get(key, {})
    latest = object_value(current.get("conversation_capture"))
    latest_status = latest.get("status")
    status = latest_status if latest_status in ("failed", "disabled") else (
        "not_collected"
    )
    # Recover only the archive, preserving current body and identity.
    preserved[key] = {
        **current, "_conversation_target": old,
        "conversation_capture": {
            **capture, "latest_attempt_status": status,
            "latest_attempt_at": (
                latest.get("captured_at") or version.isoformat()
            ),
        },
    }
