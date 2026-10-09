from __future__ import annotations

import datetime
import json
import xml.etree.ElementTree as ElementTree
from email.utils import parsedate_to_datetime
from typing import Any, NamedTuple, cast

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.html_utils import html_base_url, remove_page_noise
from dank.model import Post, RawPost
from dank.process.feed_html import recover_feed_html
from dank.process.page import (
    extract_article_html,
    extract_page_metadata,
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


class _RSSPayload(NamedTuple):
    feed_xml: str
    page_html: str
    page_final_url: str = ""


class _ParsedItem(NamedTuple):
    title: str
    text: str
    author: str
    created_at: datetime.datetime | None
    full_content: bool = False


class _PageDerivedValues(NamedTuple):
    title: str
    author: str
    published_at: datetime.datetime | None
    content_html: str


def convert_raw_post(row: RawPost) -> Post | None:
    payload = _split_payload(row.payload)
    root = _parse_xml_root(payload.feed_xml)

    if root is None:
        return None

    match _strip_xml_namespace(root.tag).lower():
        case "entry":
            parsed = _parse_atom_entry(root)
        case "item":
            parsed = _parse_rss_item(root)
        case _:
            return None

    page_derived = _derive_page_values(
        payload.page_html,
        page_url=payload.page_final_url or row.url,
        title=parsed.title,
        author=parsed.author,
        published_at=row.post_created_at or parsed.created_at,
        content_html=parsed.text,
        full_content=parsed.full_content,
    )
    title = page_derived.title
    author = page_derived.author
    published_at = page_derived.published_at
    content_html = page_derived.content_html

    if not title:
        title_source = parsed.text or strip_html(content_html)
        if title_source:
            title = title_source.splitlines()[0].strip()

    created_at = (
        published_at
        or row.scraped_at
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
        html=content_html,
        html_embedding=EMPTY_STRING_VECTOR,
        source=row.source,
    )


def _derive_page_values(
    page_html: str,
    *,
    page_url: str,
    title: str,
    author: str,
    published_at: datetime.datetime | None,
    content_html: str,
    full_content: bool = False,
) -> _PageDerivedValues:
    # Parse heavyweight HTML only when RSS fields are missing information.
    if page_html and (not title or not author or published_at is None):
        page_metadata = extract_page_metadata(page_html)

        if not title:
            title = page_metadata.title

        if not author:
            author = page_metadata.author

        if published_at is None:
            published_at = page_metadata.published_at

    base_url: str | None = None
    # Recover flat feed structure only when the page matches its entire text.
    if page_html and full_content and content_html:
        recovered = recover_feed_html(content_html, page_html)

        if recovered:
            content_html = recovered
            base_url = html_base_url(page_html, page_url)
    elif page_html:
        content_html = (
            extract_article_html(page_html) or page_html
        )
        base_url = html_base_url(page_html, page_url)

    content_html = remove_page_noise(content_html, base_url=base_url)

    return _PageDerivedValues(
        title=title,
        author=author,
        published_at=published_at,
        content_html=content_html,
    )


def recover_post_from_page(
    post: Post, previous_payload: str, previous_url: str,
) -> Post:
    """Reuse a saved page only when it matches the current post's full text."""
    payload = _split_payload(previous_payload)
    recovered = recover_feed_html(post.html, payload.page_html)

    if not recovered:
        return post

    base_url = html_base_url(
        payload.page_html, payload.page_final_url or previous_url,
    )

    return post._replace(html=remove_page_noise(recovered, base_url=base_url))


def _parse_xml_root(xml: str) -> ElementTree.Element | None:
    try:
        return ElementTree.fromstring(xml)
    except ElementTree.ParseError:
        return None


def _split_payload(payload: str) -> _RSSPayload:
    if not payload:
        return _RSSPayload("", "")

    try:
        parsed = json.loads(payload)
    except ValueError:
        return _RSSPayload(payload, "")

    if not isinstance(parsed, dict):
        return _RSSPayload(payload, "")

    parsed_dict = cast(dict[str, Any], parsed)
    feed_xml = parsed_dict.get("feed_xml")
    page_html = parsed_dict.get("page_html")
    page_final_url = parsed_dict.get("page_final_url")

    if isinstance(feed_xml, str):
        return _RSSPayload(
            feed_xml,
            page_html if isinstance(page_html, str) else "",
            page_final_url if isinstance(page_final_url, str) else "",
        )

    return _RSSPayload(payload, "")


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
        full_content=bool(_text(entry, "atom:content", ATOM_NAMESPACES)),
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
        full_content=bool(_text(item, "content:encoded", RSS2_NAMESPACES)),
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
