from __future__ import annotations

import datetime
import html
import json
import re
from typing import Any, cast
from urllib.parse import urlsplit

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.model import Post, RawPost


def convert_raw_x_post(row: RawPost) -> Post | None:
    try:
        payload = json.loads(row.payload)
    except ValueError:
        return None

    if not isinstance(payload, dict):
        return None

    payload = cast(dict[str, Any], payload)

    text = _clean_post_text(payload, post_url=row.url)
    author = _extract_author(payload).strip()

    if not _has_substantive_content(text, author):
        return None

    title = _title_from_text(text)
    created_at = (
        row.post_created_at
        or _extract_created_at(payload)
        or datetime.datetime.now(datetime.UTC)
    )
    updated_at = row.scraped_at or created_at

    return Post(
        domain=row.domain,
        post_id=row.post_id,
        url=row.url,
        created_at=created_at,
        updated_at=updated_at,
        author=author,
        title=title,
        title_embedding=EMPTY_STRING_VECTOR,
        html=text,
        html_embedding=EMPTY_STRING_VECTOR,
        source=row.source,
    )


def _has_substantive_content(text: str, author: str) -> bool:
    return bool(text.strip()) or bool(author.strip())


def _title_from_text(text: str) -> str:
    first_line = text.splitlines()[0] if text else ""
    unescaped = html.unescape(first_line)
    without_urls = re.sub(r"https?://\S+", "", unescaped)

    return " ".join(without_urls.split())


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


def _clean_post_text(payload: dict[str, object], *, post_url: str) -> str:
    text = _extract_text(payload)

    if not text:
        return ""

    url_map = _extract_url_map(payload)
    text = _replace_tco_urls(text, url_map)
    removable_urls = _removable_trailing_urls(
        payload,
        url_map=url_map,
        post_url=post_url,
    )

    return _strip_trailing_urls(text, removable_urls)


def _strip_trailing_urls(text: str, removable: set[str]) -> str:
    if not text:
        return ""

    cleaned = text.rstrip()

    while True:
        match = re.search(r"\s+(https://[^\s]+)\s*$", cleaned)
        if match is None:
            break

        url = match.group(1)

        if url not in removable:
            break

        cleaned = cleaned[: match.start()].rstrip()

    return cleaned


def _replace_tco_urls(text: str, url_map: dict[str, str]) -> str:
    if not text:
        return ""

    return re.sub(
        r"https://t\.co/[A-Za-z0-9]+",
        lambda match: url_map.get(match.group(0), match.group(0)),
        text,
    )


def _removable_trailing_urls(
    payload: dict[str, object],
    *,
    url_map: dict[str, str],
    post_url: str,
) -> set[str]:
    removable = set(_extract_media_short_urls(payload))

    for short_url, expanded_url in url_map.items():
        if _is_same_post_url(expanded_url, post_url):
            removable.add(short_url)
            removable.add(expanded_url)

    return removable


def _extract_url_map(payload: dict[str, object]) -> dict[str, str]:
    url_map: dict[str, str] = {}

    for entry in _iter_entity_urls(payload):
        short_url = _get_str(entry.get("url"))
        expanded_url = _get_str(entry.get("expanded_url"))

        if short_url and expanded_url:
            url_map[short_url] = expanded_url

    return url_map


def _extract_media_short_urls(payload: dict[str, object]) -> set[str]:
    short_urls: set[str] = set()

    for media in _iter_entity_media(payload):
        short_url = _get_str(media.get("url"))

        if short_url:
            short_urls.add(short_url)

    return short_urls


def _iter_entity_urls(payload: dict[str, object]) -> list[dict[str, object]]:
    legacy = _as_dict(payload.get("legacy"))
    if legacy is None:
        return []

    entities = _as_dict(legacy.get("entities"))
    if entities is None:
        return []

    url_entries = _as_list(entities.get("urls"))
    if url_entries is None:
        return []

    return [entry for value in url_entries if (entry := _as_dict(value))]


def _iter_entity_media(payload: dict[str, object]) -> list[dict[str, object]]:
    legacy = _as_dict(payload.get("legacy"))
    if legacy is None:
        return []

    media_entries: list[dict[str, object]] = []
    entities = _as_dict(legacy.get("entities"))

    if entities is not None:
        media_entries.extend(_media_entries_from_container(entities))

    extended_entities = _as_dict(legacy.get("extended_entities"))

    if extended_entities is not None:
        media_entries.extend(_media_entries_from_container(extended_entities))

    return media_entries


def _media_entries_from_container(
    container: dict[str, object],
) -> list[dict[str, object]]:
    media = _as_list(container.get("media"))

    if media is None:
        return []

    return [entry for value in media if (entry := _as_dict(value))]


def _as_list(value: object) -> list[object] | None:
    if isinstance(value, list):
        return cast(list[object], value)

    return None


def _is_same_post_url(candidate_url: str, post_url: str) -> bool:
    candidate = urlsplit(candidate_url)
    post = urlsplit(post_url)

    candidate_host = candidate.hostname or ""
    post_host = post.hostname or ""

    candidate_domain = candidate_host.lower().removeprefix("www.")
    post_domain = post_host.lower().removeprefix("www.")

    if candidate_domain != post_domain:
        return False

    candidate_path = candidate.path.rstrip("/")
    post_path = post.path.rstrip("/")

    if not candidate_path or not post_path:
        return False

    return candidate_path == post_path or candidate_path.startswith(
        f"{post_path}/",
    )


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
