"""Version-bound Hacker News metadata and safe archived discussion views."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast
from urllib.parse import urlsplit

from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.app import (
    PostRow,
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
)

MAX_COMMENT_DEPTH = 32


def _object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _payload(value: object) -> dict[str, Any]:
    try:
        return _object(json.loads(value)) if isinstance(value, str) else {}
    except ValueError:
        return {}


def _number(value: object) -> int | None:
    # Homepage DOM counts are strings; JSON archives use integers.
    if (isinstance(value, str) and len(value) <= 16
            and value.isascii() and value.isdecimal()):
        value = int(value)

    return value if type(value) is int and 0 <= value < 2 ** 53 else None


def _timestamp(value: object) -> str | None:
    timestamp = _number(value)

    if timestamp is None:
        return None

    try:
        return dt.datetime.fromtimestamp(timestamp, dt.UTC).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def _http_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    try:
        parsed = urlsplit(value)
        _ = parsed.port

        if (parsed.scheme in {"http", "https"} and parsed.hostname
                and not parsed.username and not parsed.password):
            return value
    except ValueError:
        pass

    return None


async def add_hacker_news_metadata(
    client: ClickHouseClient, payloads: list[dict[str, Any]],
    rows: list[PostRow],
    *, include_discussion: bool,
) -> None:
    versions = [
        (post.post_id, round(post.updated_at.replace(
            tzinfo=post.updated_at.tzinfo or dt.UTC,
        ).timestamp() * 1000))
        for post in rows if post.domain == "news.ycombinator.com"
    ]

    if not versions:
        return

    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = 'news.ycombinator.com' "
        "AND (post_id, toUnixTimestamp64Milli(scraped_at)) "
        "IN %(hacker_news_versions)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        {"hacker_news_versions": versions},
    )
    snapshots = {
        str(row["post_id"]): _payload(row.get("payload"))
        for row in result.rows
    }

    if include_discussion:
        snapshots = await _preserve_comments(client, snapshots, dict(versions))

    for post in payloads:
        if post["domain"] != "news.ycombinator.com":
            continue

        snapshot = snapshots.get(post["id"], {})
        listing = _object(snapshot.get("hckrnews"))
        story = _object(snapshot.get("story"))
        rss = _object(snapshot.get("rss"))
        metadata = _object(snapshot.get("hacker_news"))
        external_url = _http_url(
            listing.get("link") or story.get("url") or rss.get("url"),
        )
        day = metadata.get("day")

        try:
            day = dt.date.fromisoformat(
                day if isinstance(day, str) else "",
            ).isoformat()
        except (ValueError, TypeError):
            day = str(post["created_at"])[:10]

        post["hacker_news"] = {
            "points": _number(listing.get("points", story.get("score"))),
            "comments": _number(
                listing.get("comments", story.get("descendants")),
            ),
            "discussion_url": post["url"],
            "external_url": (
                external_url if external_url != post["url"] else None
            ),
            "day": day, "rank": _number(metadata.get("rank")),
            "submitted_at": (
                _timestamp(story.get("time"))
                or _timestamp(listing.get("time"))
                or _timestamp(rss.get("time"))
            ),
        }

        if include_discussion:
            comments = _comments(snapshot.get("comments"), post["id"])
            capture = _object(snapshot.get("comment_capture"))
            status = capture.get("status")
            post["hacker_news"].update(
                comment_capture={
                    "status": status if isinstance(status, str) and status in {
                        "captured", "partial", "failed", "disabled",
                    } else "disabled",
                    "captured": len(comments),
                    "total": _number(capture.get("total")),
                    "reason": capture.get("reason") if isinstance(
                        capture.get("reason"), str,
                    ) else None,
                },
                comments_items=comments,
            )


async def _preserve_comments(
    client: ClickHouseClient, snapshots: dict[str, dict[str, Any]],
    versions: dict[str, int],
) -> dict[str, dict[str, Any]]:
    candidates = {
        key: versions[key] for key, snapshot in snapshots.items()
        if key in versions and _object(snapshot.get("comment_capture")).get(
            "status",
        ) not in ("captured", "partial")
    }

    if not candidates:
        return snapshots

    clauses: list[str] = []
    params: dict[str, object] = {}

    for index, (post_id, version) in enumerate(candidates.items()):
        clauses.append(
            f"(post_id = %(post_{index})s "
            f"AND toUnixTimestamp64Milli(scraped_at) < %(before_{index})s)",
        )
        params[f"post_{index}"] = post_id
        params[f"before_{index}"] = version

    result = await client.fetch_json(
        "SELECT post_id, payload, scraped_at FROM raw_posts "
        "WHERE domain = 'news.ycombinator.com' AND ("
        + " OR ".join(clauses) + ") "
        "AND JSONExtractString(payload, 'comment_capture', 'status') "
        "IN ('captured', 'partial') "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id", params,
    )
    preserved = dict(snapshots)

    for row in result.rows:
        key = str(row["post_id"])
        date = parse_datetime(row.get("scraped_at"))

        if key not in candidates or date is None:
            continue

        date = date.replace(tzinfo=date.tzinfo or dt.UTC)

        if round(date.timestamp() * 1000) >= candidates[key]:
            continue

        previous = _payload(row.get("payload"))
        capture = _object(previous.get("comment_capture"))

        if capture.get("status") not in ("captured", "partial"):
            continue

        reason = "Comments retained from an earlier detailed scrape."

        if isinstance(capture.get("reason"), str):
            reason += " " + capture["reason"]

        preserved[key] = {
            **snapshots[key], "comments": previous.get("comments", []),
            "comment_capture": {**capture, "reason": reason},
        }

    return preserved


def _comments(value: object, story_id: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    items: dict[str, dict[str, Any]] = {}

    for entry in cast(list[object], value):
        item = _object(entry)
        identifier = _number(item.get("id"))

        if identifier and str(identifier) != story_id:
            items.setdefault(str(identifier), item)

    children: dict[str, list[str]] = {}

    for identifier, item in items.items():
        parent = str(_number(item.get("parent")) or story_id)
        parent = parent if parent in items else story_id
        children.setdefault(parent, []).append(identifier)

    queue = [
        (identifier, 0) for identifier in reversed(children.get(story_id, []))
    ]
    result: list[dict[str, Any]] = []
    seen: set[str] = set()

    while queue:
        identifier, depth = queue.pop()

        if identifier in seen:
            continue

        seen.add(identifier)
        item = items[identifier]
        queue.extend(
            (child, min(depth + 1, MAX_COMMENT_DEPTH))
            for child in reversed(children.get(identifier, []))
        )
        body = item.get("text")

        if (item.get("deleted") or item.get("dead")
                or not isinstance(body, str)):
            continue

        author = item.get("by")
        result.append({
            "id": identifier, "parent_id": str(item.get("parent") or story_id),
            "author": author if isinstance(author, str) else "",
            "html": _sanitize_html(body),
            "created_at": _timestamp(item.get("time")),
            "depth": depth,
        })

    return result
