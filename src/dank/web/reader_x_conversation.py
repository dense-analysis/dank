"""Bounded, source-validated projections of captured X conversations."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, cast
from urllib.parse import urlsplit

from dank.storage.clickhouse import parse_datetime
from dank.web.app import (
    PostRow,
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import profile_identity
from dank.x_content import object_value, post_html, post_links, post_media

# Defensive reader limits, independent of a collector's configured budget.
MAX_CONVERSATION_POSTS = 200
MAX_CONVERSATION_DEPTH = 20
CAPTURE_STATUSES = {"captured", "partial", "failed", "disabled"}


def identifier(value: object) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(
        r"[0-9]{1,20}", value,
    ) else None


def canonical_target(post: PostRow) -> bool:
    try:
        url = urlsplit(post.url)

        return bool(
            url.scheme in {"https", "http"}
            and url.hostname in {"x.com", "twitter.com", "www.x.com",
                                 "www.twitter.com"}
            and url.username is None and url.password is None
            and url.port in {None, 443, 80}
            and re.fullmatch(
                r"/[A-Za-z0-9_]{1,15}/status/" + re.escape(post.post_id)
                + r"/?", url.path,
            ) and identifier(post.post_id),
        )
    except ValueError:
        return False


def conversation_capture(
    post: PostRow, payload: dict[str, Any], local_urls: dict[str, str],
    *, include_posts: bool,
) -> dict[str, Any]:
    capture = object_value(payload.get("conversation_capture"))
    target = object_value(payload.get("_conversation_target")) or payload
    raw_status = capture.get("status")
    status = raw_status if isinstance(raw_status, str) and (
        raw_status in CAPTURE_STATUSES
    ) else "not_collected"
    result: dict[str, Any] = {
        "status": status,
        "captured_at": _timestamp(capture.get("captured_at")),
        "has_more": capture.get("has_more") is True,
        "reason": capture.get("reason") if isinstance(
            capture.get("reason"), str,
        ) else None,
        "reported_replies": count(object_value(target.get("legacy")).get(
            "reply_count",
        )),
        "limit": min(count(capture.get("limit")) or 0, MAX_CONVERSATION_POSTS),
        "context": [], "replies": [], "captured_count": 0,
    }

    if capture.get("latest_attempt_status") in (
        "failed", "disabled", "not_collected",
    ):
        result["latest_attempt_status"] = capture["latest_attempt_status"]
        result["latest_attempt_at"] = _timestamp(
            capture.get("latest_attempt_at"),
        )

    if status in {"captured", "partial"}:
        _add_posts(
            result, post, target, capture, local_urls,
            include_posts=include_posts,
        )

    if not include_posts:
        result.pop("context")
        result.pop("replies")

    return result


def count(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(
        value, bool,
    ) and 0 <= value < 2**53 else None


def _timestamp(value: object) -> str | None:
    parsed = parse_datetime(value) if isinstance(value, str) else None

    return parsed.replace(tzinfo=parsed.tzinfo or dt.UTC).isoformat() if (
        parsed is not None
    ) else None


def valid_capture(
    post: PostRow, payload: dict[str, Any], capture: dict[str, Any],
) -> bool:
    legacy = object_value(payload.get("legacy"))
    conversation_id = identifier(capture.get("conversation_id"))
    raw_id = identifier(payload.get("rest_id"))
    raw_conversation = identifier(legacy.get("conversation_id_str"))

    return bool(
        canonical_target(post) and capture.get("target_id") == post.post_id
        and raw_id == post.post_id and conversation_id
        and raw_conversation == conversation_id
        and _timestamp(capture.get("captured_at")),
    )


def _partial(result: dict[str, Any], reason: str) -> None:
    result.update(status="partial", has_more=True)
    result["reason"] = result["reason"] or reason


def _add_posts(
    result: dict[str, Any], post: PostRow, payload: dict[str, Any],
    capture: dict[str, Any], local_urls: dict[str, str],
    *, include_posts: bool,
) -> None:
    if not valid_capture(post, payload, capture):
        result.update(
            status="failed", reason="invalid_capture", has_more=False,
        )

        return

    raw_posts = capture.get("posts")

    if not isinstance(raw_posts, list):
        result.update(
            status="failed", reason="invalid_capture", has_more=False,
        )

        return

    raw_posts = cast(list[object], raw_posts)
    nodes: dict[str, dict[str, Any]] = {}

    for raw in raw_posts[:MAX_CONVERSATION_POSTS]:
        node = _node(raw, str(capture["conversation_id"]))

        if node and node["rest_id"] != post.post_id:
            nodes.setdefault(node["rest_id"], node)

    if len(raw_posts) > MAX_CONVERSATION_POSTS:
        _partial(result, "limit")

    ancestors = _ancestors(post.post_id, payload, nodes, result)
    replies = _replies(post.post_id, str(capture["conversation_id"]), nodes,
                       ancestors, result)
    result["captured_count"] = len(replies)

    if include_posts:
        root_handle = _handle(payload)
        result["context"] = [
            _post(node, post, root_handle, local_urls, depth=0,
                  missing_parent=False)
            for node in reversed(ancestors)
        ]
        result["replies"] = [
            _post(node, post, root_handle, local_urls, depth=depth,
                  missing_parent=missing)
            for node, depth, missing in replies
        ]

    direct = sum(_parent(node) == post.post_id for node, _, _ in replies)

    if result["reported_replies"] is None:
        _partial(result, "missing_source")
    elif (
        direct < result["reported_replies"]
    ):
        _partial(result, "reported_replies")

    if result["has_more"] or result["status"] == "partial":
        _partial(result, "cursor")


def _node(raw: object, conversation_id: str) -> dict[str, Any]:
    node = object_value(raw)
    legacy = object_value(node.get("legacy"))
    node_id = identifier(node.get("rest_id"))

    if (
        not node_id or node.get("__typename", "Tweet") != "Tweet"
        or identifier(legacy.get("conversation_id_str")) != conversation_id
        or legacy.get("id_str", node_id) != node_id
        or node.get("promotedMetadata") or node.get("promoted_metadata")
        or not _handle(node)
    ):
        return {}

    return node


def _parent(node: dict[str, Any]) -> str | None:
    return identifier(object_value(node.get("legacy")).get(
        "in_reply_to_status_id_str",
    ))


def _ancestors(
    target: str, payload: dict[str, Any], nodes: dict[str, dict[str, Any]],
    result: dict[str, Any],
) -> list[dict[str, Any]]:
    parents: list[dict[str, Any]] = []
    seen = {target}
    parent = _parent(payload)

    while parent:
        if parent in seen or len(parents) >= MAX_CONVERSATION_DEPTH:
            _partial(result, "missing_context")
            break

        seen.add(parent)
        node = nodes.get(parent)

        if not node:
            _partial(result, "missing_context")
            break

        parents.append(node)
        parent = _parent(node)

    return parents


def _reply_path(
    node_id: str, target: str, nodes: dict[str, dict[str, Any]],
) -> tuple[list[str], str]:
    path: list[str] = []
    current = node_id

    while current != target:
        if current in path or len(path) >= MAX_CONVERSATION_DEPTH:
            return [], "cycle"

        node = nodes.get(current)

        if not node:
            return path, "missing"

        path.append(current)
        parent = _parent(node)

        if not parent:
            return [], "other"

        current = parent

    return path, "target"


def _replies(
    target: str, conversation_id: str, nodes: dict[str, dict[str, Any]],
    ancestors: list[dict[str, Any]], result: dict[str, Any],
) -> list[tuple[dict[str, Any], int, bool]]:
    context_ids = {str(node["rest_id"]) for node in ancestors}
    replies: dict[str, tuple[dict[str, Any], int, bool]] = {}

    for node_id in nodes:
        if node_id in context_ids:
            continue

        path, state = _reply_path(node_id, target, nodes)

        if state in {"missing", "cycle"}:
            _partial(result, "missing_context")

        if state != "target" and not (state == "missing" and (
            target == conversation_id
        )):
            continue

        for depth, related_id in enumerate(reversed(path)):
            replies.setdefault(related_id, (
                nodes[related_id], depth,
                state == "missing" and depth == 0,
            ))

    return _ordered_replies(replies)


def _ordered_replies(
    replies: dict[str, tuple[dict[str, Any], int, bool]],
) -> list[tuple[dict[str, Any], int, bool]]:
    children: dict[str | None, list[str]] = {}

    for node_id, (node, _, _) in replies.items():
        parent = _parent(node)
        children.setdefault(parent if parent in replies else None, []).append(
            node_id,
        )

    # Keep each branch contiguous even when a later page supplies its children.
    pending = list(reversed(children.get(None, [])))
    ordered: list[tuple[dict[str, Any], int, bool]] = []

    while pending:
        node_id = pending.pop()
        ordered.append(replies[node_id])
        pending.extend(reversed(children.get(node_id, [])))

    return ordered


def _handle(payload: dict[str, Any]) -> str:
    profile = object_value(object_value(object_value(
        payload.get("core"),
    ).get("user_results")).get("result"))
    core = object_value(profile.get("core"))
    identity = core if core.get("screen_name") else object_value(
        profile.get("legacy"),
    )
    value = identity.get("screen_name")

    return value.lower() if isinstance(value, str) and re.fullmatch(
        r"[A-Za-z0-9_]{1,15}", value,
    ) else ""


def _post(
    node: dict[str, Any], selected: PostRow, root_handle: str,
    local_urls: dict[str, str], *, depth: int, missing_parent: bool,
) -> dict[str, Any]:
    handle = _handle(node)
    profile = object_value(object_value(node.get("core")).get("user_results"))
    name, icon = profile_identity(profile.get("result"), handle)
    body = _sanitize_html(post_html(node))
    legacy = object_value(node.get("legacy"))
    node_id = str(node["rest_id"])

    return {
        "id": node_id, "domain": selected.domain,
        "url": f"https://x.com/{handle}/status/{node_id}",
        "author": handle, "title": "", "html": body,
        "excerpt": _summarize_html(body), "source": "x",
        "created_at": _created_at(legacy.get("created_at"), node_id),
        "thumbnail": None, "media": [], "parent_id": _parent(node),
        "depth": depth, "missing_parent": missing_parent,
        "is_author": bool(root_handle and handle == root_handle),
        "x": {
            "handle": handle, "name": name, "icon_url": icon,
            "links": post_links(node, local_urls),
            "media": post_media(node, local_urls),
            "reply_count": count(legacy.get("reply_count")),
        },
    }


def _created_at(value: object, node_id: str) -> str:
    if isinstance(value, str):
        try:
            return dt.datetime.strptime(
                value, "%a %b %d %H:%M:%S %z %Y",
            ).isoformat()
        except ValueError:
            parsed = _timestamp(value)

            if parsed:
                return parsed

    # X snowflakes encode their creation time even if legacy text is missing.
    # https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala#L22-L31
    milliseconds = (int(node_id) >> 22) + 1288834974657

    return dt.datetime.fromtimestamp(milliseconds / 1000, dt.UTC).isoformat()
