from __future__ import annotations

import datetime
import json
from typing import Any, cast

from dank.model import Post, RawPost


def convert_raw_x_post(row: RawPost) -> Post | None:
    try:
        payload = json.loads(row.payload)
    except ValueError:
        return None

    if not isinstance(payload, dict):
        return None

    payload = cast(dict[str, Any], payload)

    text = _extract_text(payload)
    title = text.splitlines()[0] if text else ""
    created_at = (
        row.post_created_at
        or _extract_created_at(payload)
        or datetime.datetime.now(datetime.UTC)
    )
    updated_at = row.scraped_at or created_at
    author = _extract_author(payload)
    return Post(
        domain=row.domain,
        post_id=row.post_id,
        url=row.url,
        created_at=created_at,
        updated_at=updated_at,
        author=author,
        title=title,
        html=text,
        source=row.source,
    )


def _as_dict(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return cast(dict[str, object], value)

    return None


def _get_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _extract_text(payload: dict[str, object]) -> str:
    legacy = _as_dict(payload.get("legacy"))
    if legacy is not None:
        full_text = _get_str(legacy.get("full_text"))
        if full_text:
            return full_text
    note_tweet = _as_dict(payload.get("note_tweet"))
    if note_tweet is not None:
        note_results = _as_dict(note_tweet.get("note_tweet_results"))
        if note_results is not None:
            result = _as_dict(note_results.get("result"))
            if result is not None:
                text = _get_str(result.get("text"))
                if text:
                    return text
    return ""


def _extract_created_at(
    payload: dict[str, object],
) -> datetime.datetime | None:
    legacy = _as_dict(payload.get("legacy"))
    if legacy is None:
        return None
    created_at = _get_str(legacy.get("created_at"))
    if not created_at:
        return None
    try:
        return datetime.datetime.strptime(
            created_at,
            "%a %b %d %H:%M:%S %z %Y",
        )
    except ValueError:
        return _parse_datetime(created_at)


def _parse_datetime(value: str) -> datetime.datetime | None:
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None


def _extract_author(payload: dict[str, object]) -> str:
    core = _as_dict(payload.get("core"))
    if core is None:
        return ""
    user_results = _as_dict(core.get("user_results"))
    if user_results is None:
        return ""
    result = _as_dict(user_results.get("result"))
    if result is None:
        return ""
    legacy = _as_dict(result.get("legacy"))
    if legacy is not None:
        handle = _get_str(legacy.get("screen_name"))
        if handle:
            return handle
    user_core = _as_dict(result.get("core"))
    if user_core is not None:
        handle = _get_str(user_core.get("screen_name"))
        if handle:
            return handle
    return ""
