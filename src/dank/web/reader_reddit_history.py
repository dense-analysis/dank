"""Preserve successful Reddit archives after unsuccessful later attempts."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast

from dank.storage.clickhouse import ClickHouseClient, parse_datetime


async def preserve_reddit_archives(
    client: ClickHouseClient,
    snapshots: dict[str, dict[str, Any]],
    versions: list[tuple[str, int]],
    *, include_discussion: bool,
) -> dict[str, dict[str, Any]]:
    """Return display projections, retaining each recovered capture's date."""
    preserved = {key: dict(value) for key, value in snapshots.items()}
    version_map = dict(versions)
    fields = ("detail_capture", "comment_capture") if include_discussion else (
        "detail_capture",
    )

    for field in fields:
        candidates = {
            key: version_map[key]
            for key, snapshot in snapshots.items()
            if key in version_map and isinstance(snapshot.get(field), dict)
            and snapshot[field].get("status")
            in ("failed", "disabled", "deferred")
        }

        if not candidates:
            continue

        previous = await _previous_captures(client, candidates, field)

        for key, (snapshot, timestamp) in previous.items():
            target = preserved[key]
            capture = snapshot.get(field)

            if not isinstance(capture, dict):
                continue

            capture = cast(dict[str, Any], capture)

            if capture.get("status") != "captured":
                continue

            if field == "detail_capture":
                # Counts, flair, title and failure status still describe the
                # displayed version; archived media comes from history.
                target["media"] = snapshot.get("media", [])
            else:
                target[field] = {
                    **capture,
                    "captured_at": capture.get("captured_at") or timestamp,
                    "latest_attempt_status": snapshots[key][field]["status"],
                }

    return preserved


async def _previous_captures(
    client: ClickHouseClient, candidates: dict[str, int], field: str,
) -> dict[str, tuple[dict[str, Any], str]]:
    clauses: list[str] = []
    params: dict[str, object] = {"capture_field": field}

    for index, (post_id, version) in enumerate(candidates.items()):
        clauses.append(
            f"(post_id = %(post_{index})s "
            f"AND toUnixTimestamp64Milli(scraped_at) < %(before_{index})s)",
        )
        params[f"post_{index}"] = post_id
        params[f"before_{index}"] = version

    result = await client.fetch_json(
        "SELECT post_id, payload, scraped_at FROM raw_posts "
        "WHERE domain = 'reddit.com' AND (" + " OR ".join(clauses) + ") "
        "AND JSONExtractString(payload, %(capture_field)s, 'status') "
        "= 'captured' ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        params,
    )
    captures: dict[str, tuple[dict[str, Any], str]] = {}

    for row in result.rows:
        post_id = str(row["post_id"])
        timestamp = parse_datetime(row.get("scraped_at"))

        if post_id not in candidates or timestamp is None:
            continue

        timestamp = timestamp.replace(tzinfo=timestamp.tzinfo or dt.UTC)

        if round(timestamp.timestamp() * 1000) >= candidates[post_id]:
            continue

        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            continue

        if isinstance(payload, dict):
            captures[post_id] = (
                cast(dict[str, Any], payload), timestamp.isoformat(),
            )

    return captures
