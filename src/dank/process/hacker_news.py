"""Prepare archived Hacker News stories without fetching linked articles."""

from __future__ import annotations

import html
import json
from typing import Any, cast
from urllib.parse import urlsplit

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.html_utils import remove_page_noise
from dank.model import Post, RawPost


def _object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def preserve_hacker_news_story(row: RawPost, previous: str) -> RawPost:
    """Retain a detailed story body when a later listing has no story data."""
    try:
        payload = _object(json.loads(row.payload))
        old = _object(json.loads(previous))
    except (TypeError, ValueError):
        return row

    if payload and not payload.get("story") and _object(old.get("story")):
        return row._replace(payload=json.dumps({
            **payload, "story": old["story"],
        }))

    return row


def convert_raw_hacker_news_post(row: RawPost) -> Post | None:
    try:
        payload = _object(json.loads(row.payload))
    except (TypeError, ValueError):
        return None

    listing = _object(payload.get("hckrnews"))
    story = _object(payload.get("story"))
    title = listing.get("link_text") or story.get("title")

    if not isinstance(title, str) or not title.strip():
        return None

    body = story.get("text")
    body = remove_page_noise(body) if isinstance(body, str) else ""
    link = listing.get("link") or story.get("url")

    if isinstance(link, str):
        try:
            parsed = urlsplit(link)

            if (parsed.scheme in {"http", "https"} and parsed.hostname
                    and not parsed.username and not parsed.password
                    and link != row.url):
                escaped = html.escape(link, quote=True)
                body += (
                    f'<p><a href="{escaped}" data-dank-hn-destination="1">'
                    f'{escaped}</a></p>'
                )
        except ValueError:
            pass

    author = story.get("by") or listing.get("submitter")

    return Post(
        domain=row.domain, post_id=row.post_id, url=row.url,
        created_at=row.post_created_at or row.scraped_at,
        updated_at=row.scraped_at,
        author=author if isinstance(author, str) else "",
        title=html.unescape(title).strip(),
        title_embedding=EMPTY_STRING_VECTOR, html=body,
        html_embedding=EMPTY_STRING_VECTOR, source="hacker_news",
    )
