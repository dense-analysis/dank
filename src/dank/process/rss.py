from __future__ import annotations

import datetime
import json
import xml.etree.ElementTree as ElementTree
from email.utils import parsedate_to_datetime
from typing import Any, NamedTuple, cast

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.model import Post, RawPost
from dank.process.page import (
    extract_article_html,
    extract_page_metadata,
    extract_youtube_iframes,
    strip_html,
)

ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
RSS1_NAMESPACE = "http://purl.org/rss/1.0/"
DC_NAMESPACE = "http://purl.org/dc/elements/1.1/"
CONTENT_NAMESPACE = "http://purl.org/rss/1.0/modules/content/"
ATOM_NAMESPACES = {"atom": ATOM_NAMESPACE}
RSS_NAMESPACES = {
    "rss": RSS1_NAMESPACE,
    "dc": DC_NAMESPACE,
    "content": CONTENT_NAMESPACE,
}
RSS2_NAMESPACES = {
    "dc": DC_NAMESPACE,
    "content": CONTENT_NAMESPACE,
}


class _ParsedItem(NamedTuple):
    title: str
    text: str
    author: str
    created_at: datetime.datetime | None


def convert_raw_post(row: RawPost) -> Post | None:
    feed_xml, page_html = _split_payload(row.payload)
    root = _parse_xml_root(feed_xml)

    if root is None:
        return None

    match _strip_xml_namespace(root.tag).lower():
        case "entry":
            parsed = _parse_atom_entry(root)
        case "item":
            parsed = _parse_rss_item(root)
        case _:
            return None

    page_metadata = extract_page_metadata(page_html)
    title = parsed.title or page_metadata.title
    content_html = extract_article_html(page_html)

    if not content_html and page_html:
        content_html = page_html

    if not content_html:
        content_html = parsed.text

    if not content_html:
        content_html = extract_youtube_iframes(page_html)

    if not title:
        title_source = parsed.text or strip_html(content_html)
        if title_source:
            title = title_source.splitlines()[0].strip()

    created_at = (
        row.post_created_at
        or page_metadata.published_at
        or parsed.created_at
        or row.scraped_at
        or datetime.datetime.now(datetime.UTC)
    )
    updated_at = row.scraped_at or created_at
    author = parsed.author or page_metadata.author

    return Post(
        domain=row.domain,
        post_id=row.post_id,
        url=row.url,
        created_at=created_at,
        updated_at=updated_at,
        author=author,
        title=title,
        title_embedding=EMPTY_STRING_VECTOR,
        html=content_html,
        html_embedding=EMPTY_STRING_VECTOR,
        source=row.source,
    )


def _parse_xml_root(xml: str) -> ElementTree.Element | None:
    try:
        return ElementTree.fromstring(xml)
    except ElementTree.ParseError:
        return None


def _split_payload(payload: str) -> tuple[str, str]:
    if not payload:
        return "", ""

    try:
        parsed = json.loads(payload)
    except ValueError:
        return payload, ""

    if not isinstance(parsed, dict):
        return payload, ""

    parsed_dict = cast(dict[str, Any], parsed)
    feed_xml = parsed_dict.get("feed_xml")
    page_html = parsed_dict.get("page_html")

    if isinstance(feed_xml, str):
        return feed_xml, page_html if isinstance(page_html, str) else ""

    return payload, ""


def _strip_xml_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag


def _parse_atom_entry(entry: ElementTree.Element) -> _ParsedItem:
    title = _text(entry, "atom:title", ATOM_NAMESPACES) or ""
    text = _first_text(
        entry,
        ["atom:content", "atom:summary"],
        ATOM_NAMESPACES,
    )
    author = _first_text(
        entry,
        ["atom:author/atom:name", "atom:author/atom:email"],
        ATOM_NAMESPACES,
    )
    created_at = _parse_datetime(
        _first_text(
            entry,
            ["atom:published", "atom:updated"],
            ATOM_NAMESPACES,
        ),
    )

    return _ParsedItem(
        title=title,
        text=text or "",
        author=author or "",
        created_at=created_at,
    )


def _parse_rss_item(item: ElementTree.Element) -> _ParsedItem:
    title = _rss_text(item, "title")
    text = _first_text(
        item,
        ["content:encoded", "description", "summary"],
        RSS2_NAMESPACES,
        fallback_namespace=RSS_NAMESPACES,
    )
    author = _first_text(
        item,
        ["author", "dc:creator"],
        RSS2_NAMESPACES,
        fallback_namespace=RSS_NAMESPACES,
    )
    created_at = _parse_datetime(
        _first_text(
            item,
            ["pubDate", "dc:date"],
            RSS2_NAMESPACES,
            fallback_namespace=RSS_NAMESPACES,
        ),
    )

    return _ParsedItem(
        title=title or "",
        text=text or "",
        author=author or "",
        created_at=created_at,
    )


def _rss_text(item: ElementTree.Element, tag: str) -> str | None:
    return _first_text(
        item,
        [tag],
        RSS2_NAMESPACES,
        fallback_namespace=RSS_NAMESPACES,
    )


def _first_text(
    node: ElementTree.Element,
    paths: list[str],
    namespaces: dict[str, str],
    *,
    fallback_namespace: dict[str, str] | None = None,
) -> str | None:
    for path in paths:
        text = _text(node, path, namespaces)

        if text:
            return text

        if fallback_namespace:
            namespaced = path

            if ":" not in path:
                namespaced = f"rss:{path}"

            text = _text(node, namespaced, fallback_namespace)

            if text:
                return text

    return None


def _text(
    node: ElementTree.Element,
    path: str,
    namespaces: dict[str, str],
) -> str | None:
    child = node.find(path, namespaces)

    if child is None or child.text is None:
        return None

    return child.text.strip()


def _parse_datetime(value: str | None) -> datetime.datetime | None:
    if value is None:
        return None

    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        try:
            return parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None

    return None
