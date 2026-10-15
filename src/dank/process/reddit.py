from __future__ import annotations

import datetime
import html
import json
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import urlsplit

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.html_utils import remove_page_noise
from dank.model import Post, RawPost


def convert_raw_reddit_post(row: RawPost) -> Post | None:
    """Prepare a captured community post without fetching its comments."""
    try:
        payload = json.loads(row.payload)
    except ValueError:
        return None

    if not isinstance(payload, dict):
        return None

    payload = cast(dict[str, Any], payload)
    title = _text(payload.get("title")).strip()

    if not title:
        return None

    created_at = (
        row.post_created_at
        or _created_at(payload.get("created_utc"))
        or row.scraped_at
    )

    # The canonical permalink also keeps the community on the existing model.
    return Post(
        domain=row.domain,
        post_id=row.post_id,
        url=row.url,
        created_at=created_at,
        updated_at=row.scraped_at,
        author=_text(payload.get("author")).strip(),
        title=title,
        title_embedding=EMPTY_STRING_VECTOR,
        html=_body_html(payload, row.url),
        html_embedding=EMPTY_STRING_VECTOR,
        source=row.source,
    )


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _created_at(value: object) -> datetime.datetime | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None

    try:
        return datetime.datetime.fromtimestamp(value, datetime.UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _body_html(payload: dict[str, Any], permalink: str) -> str:
    body = _text(payload.get("selftext_html")).strip()

    # Reddit JSON may escape the entire HTML fragment; DOM captures do not.
    if body.startswith("&lt;"):
        body = html.unescape(body)

    if body:
        body = remove_page_noise(body, base_url=permalink)
    else:
        text = _text(payload.get("selftext")).strip()
        paragraphs = [
            html.escape(paragraph).replace("\n", "<br>")
            for paragraph in text.split("\n\n") if paragraph.strip()
        ]
        body = "\n".join(f"<p>{paragraph}</p>" for paragraph in paragraphs)

    parts = [body] if body else []
    image_parser = _BodyImages()
    image_parser.feed(body)
    parts.extend(_media_html(payload.get("media"), image_parser.urls))
    external_url = _http_url(payload.get("url"))

    if (external_url and payload.get("is_self") is not True
            and external_url.rstrip("/") != permalink.rstrip("/")):
        escaped_url = html.escape(external_url, quote=True)
        parts.append(f'<p><a href="{escaped_url}">{escaped_url}</a></p>')

    return "\n".join(parts)


def _http_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    value = html.unescape(value).strip()

    try:
        parsed = urlsplit(value)
        _ = parsed.port

        if parsed.scheme in {"http", "https"} and parsed.hostname:
            return value
    except ValueError:
        pass

    return None


def _media_html(value: object, seen: set[str]) -> list[str]:
    if not isinstance(value, list):
        return []

    parts: list[str] = []

    for item in cast(list[object], value):
        if not isinstance(item, dict):
            continue

        media = cast(dict[str, object], item)
        url = _http_url(media.get("url"))

        if not url or url in seen or media.get("asset_type") != "photo":
            continue

        seen.add(url)
        escaped_url = html.escape(url, quote=True)
        parts.append(f'<p><img src="{escaped_url}" alt=""></p>')

    return parts


class _BodyImages(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: set[str] = set()

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "img":
            url = _http_url(dict(attrs).get("src"))

            if url:
                self.urls.add(url)
