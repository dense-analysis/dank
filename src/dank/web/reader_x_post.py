"""Source-validated identity and content shared by X thread and quote cards."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from dank.storage.clickhouse import parse_datetime
from dank.web.app import (
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import profile_identity
from dank.x_content import object_value, post_html, post_links, post_media


def post_handle(payload: dict[str, Any]) -> str:
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


def captured_post(
    node: dict[str, Any], domain: str, local_urls: dict[str, str],
) -> dict[str, Any]:
    handle = post_handle(node)
    profile = object_value(object_value(node.get("core")).get("user_results"))
    name, icon = profile_identity(profile.get("result"), handle)
    body = _sanitize_html(post_html(node))
    legacy = object_value(node.get("legacy"))
    node_id = str(node["rest_id"])
    replies = legacy.get("reply_count")

    return {
        "id": node_id, "domain": domain,
        "url": f"https://x.com/{handle}/status/{node_id}",
        "author": handle, "title": "", "html": body,
        "excerpt": _summarize_html(body), "source": "x",
        "created_at": post_created_at(legacy.get("created_at"), node_id),
        "thumbnail": None, "media": [],
        "x": {
            "handle": handle, "name": name, "icon_url": icon,
            "links": post_links(node, local_urls),
            "media": post_media(node, local_urls),
            "reply_count": replies if isinstance(replies, int) and not
            isinstance(replies, bool) and 0 <= replies < 2**53 else None,
        },
    }


def post_created_at(value: object, node_id: str) -> str:
    if isinstance(value, str):
        try:
            return dt.datetime.strptime(
                value, "%a %b %d %H:%M:%S %z %Y",
            ).isoformat()
        except ValueError:
            parsed = parse_datetime(value)

            if parsed:
                return parsed.replace(
                    tzinfo=parsed.tzinfo or dt.UTC,
                ).isoformat()

    # X snowflakes encode their creation time even if legacy text is missing.
    # https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala#L22-L31
    milliseconds = (int(node_id) >> 22) + 1288834974657

    return dt.datetime.fromtimestamp(milliseconds / 1000, dt.UTC).isoformat()
