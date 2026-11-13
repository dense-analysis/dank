"""Collected article HTML made safe to show, and short summaries of it."""

from __future__ import annotations

import bleach

from dank.html_utils import html_text, remove_page_noise

ALLOWED_TAGS = [
    "a",
    "blockquote",
    "br",
    "caption",
    "code",
    "col",
    "colgroup",
    "del",
    "em",
    "figcaption",
    "figure",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "strong",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
]
ALLOWED_ATTRIBUTES = {
    "a": ["href", "title", "rel"],
    "img": ["src", "alt", "title"],
    "code": ["class", "data-lang", "data-language"],
    "pre": ["class", "data-lang", "data-language"],
    "td": ["colspan", "rowspan"],
    "th": ["colspan", "rowspan", "scope", "abbr"],
    "col": ["span"],
    "colgroup": ["span"],
}


def sanitize_html(raw_html: str) -> str:
    return bleach.clean(
        remove_page_noise(raw_html),
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True,
    )


def summarize_html(raw_html: str, *, limit: int = 280) -> str:
    text = html_text(raw_html)
    text = " ".join(text.split())

    if len(text) <= limit:
        return text

    trimmed = text[:limit].rsplit(" ", 1)[0]

    return f"{trimmed}..."
