from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import xml.etree.ElementTree as ElementTree
from collections.abc import AsyncIterator, Iterable
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Literal, NamedTuple
from urllib.parse import urljoin

import aiohttp

from dank.model import RawPost
from dank.scrape.types import ScrapeBatch

logger = logging.getLogger(__name__)

RSS_SOURCE = "rss"
RSS_MIME_HINTS = ("rss", "atom", "xml", "rdf")
ATOM_FEED_TYPE = "atom"
RSS2_FEED_TYPE = "rss2"
RSS1_FEED_TYPE = "rss1"
type FeedType = Literal["atom", "rss2", "rss1"]

PAGE_DATE_KEYS = {
    "article:published_time",
    "article:published",
    "og:published_time",
    "published_time",
    "pubdate",
    "publishdate",
    "publish_date",
    "date",
    "datepublished",
    "date_published",
    "datecreated",
    "date_created",
    "dc.date",
    "dc.date.issued",
    "dc.date.created",
}
PAGE_ITEMPROP_KEYS = {
    "datepublished",
    "datecreated",
    "date",
}

ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
RSS1_NAMESPACE = "http://purl.org/rss/1.0/"
RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
ATOM_NAMESPACES = {"atom": ATOM_NAMESPACE}
RSS1_NAMESPACES = {
    "rss": RSS1_NAMESPACE,
    "rdf": RDF_NAMESPACE,
}


class FeedLink(NamedTuple):
    url: str
    feed_type: FeedType
    mime_type: str | None


class _HeadFeedLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self._in_head = False
        self.links: list[FeedLink] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "head":
            self._in_head = True

            return

        if tag != "link" or not self._in_head:
            return

        attr_map = {key.lower(): value for key, value in attrs}
        rel = (attr_map.get("rel") or "").lower()
        type_hint = (attr_map.get("type") or "").lower()
        href = attr_map.get("href")

        if not href:
            return

        if "alternate" not in rel and "feed" not in rel:
            return

        if type_hint and not any(hint in type_hint for hint in RSS_MIME_HINTS):
            return

        feed_type = _feed_type_from_mime(type_hint)
        self.links.append(FeedLink(href, feed_type, type_hint or None))

    def handle_endtag(self, tag: str) -> None:
        if tag == "head":
            self._in_head = False


class _PostDateParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.candidates: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attr_map = {
            key.lower(): value
            for key, value in attrs
            if key and value is not None
        }

        if tag == "meta":
            key = (
                attr_map.get("property")
                or attr_map.get("name")
                or attr_map.get("itemprop")
                or ""
            ).lower()
            if key in PAGE_DATE_KEYS:
                content = attr_map.get("content") or attr_map.get("value")
                if content:
                    self.candidates.append(content)
            return

        if tag == "time":
            datetime_value = attr_map.get("datetime") or attr_map.get(
                "content",
            )
            if datetime_value:
                self.candidates.append(datetime_value)
            return

        itemprop = (attr_map.get("itemprop") or "").lower()
        if itemprop in PAGE_ITEMPROP_KEYS:
            content = attr_map.get("content") or attr_map.get("datetime")
            if content:
                self.candidates.append(content)


async def scrape_site_rss(
    domain: str,
    *,
    batch_size: int = 50,
) -> AsyncIterator[ScrapeBatch]:
    root_url = f"https://{domain}"

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
    ) as client:
        html = await _fetch_text(client, root_url, accept=["text/html"])

        if not html:
            logger.warning("No HTML received for %s", root_url)

            return

        feed_links = discover_feed_links(html, root_url)

        if not feed_links:
            logger.warning("No RSS feeds found for %s", root_url)

            return

        selected = select_feed_link(feed_links)

        if selected is None:
            logger.warning("No RSS feeds found for %s", root_url)

            return

        xml = await _fetch_text(
            client,
            selected.url,
            accept=_accept_from_link(selected),
        )

        if not xml:
            logger.warning("No feed content returned for %s", selected.url)

            return

        posts = raw_posts_from_xml(
            xml,
            domain=domain,
            feed_url=selected.url,
            root_url=root_url,
        )

        if not posts:
            logger.warning("No posts parsed for %s", selected.url)

            return

        posts = await _attach_page_payloads(client, posts)

        pending: list[RawPost] = []

        for raw in posts:
            pending.append(raw)

            if len(pending) >= batch_size:
                yield ScrapeBatch(posts=pending, assets=[])
                pending = []

        if pending:
            yield ScrapeBatch(posts=pending, assets=[])


async def _fetch_text(
    client: aiohttp.ClientSession,
    url: str,
    *,
    accept: list[str],
) -> str | None:
    try:
        async with client.get(
            url,
            headers={"Accept": ", ".join(accept)},
        ) as response:
            response.raise_for_status()

            return await response.text()
    except Exception:
        logger.debug("Failed to fetch %s", url, exc_info=True)

        return None


def _feed_type_from_mime(type_hint: str | None) -> FeedType:
    match type_hint:
        case str() if "atom" in type_hint:
            return ATOM_FEED_TYPE
        case str() if "rdf" in type_hint:
            return RSS1_FEED_TYPE
        case _:
            return RSS2_FEED_TYPE


def select_feed_link(links: Iterable[FeedLink]) -> FeedLink | None:
    """Get the highest priority feed link."""
    order = [ATOM_FEED_TYPE, RSS2_FEED_TYPE, RSS1_FEED_TYPE]
    links = sorted(links, key=lambda link: order.index(link.feed_type))

    return links[0] if links else None


def _accept_from_link(link: FeedLink) -> list[str]:
    if link.mime_type:
        return [link.mime_type]

    match link.feed_type:
        case "atom":
            return [
                "application/atom+xml",
                "application/xml",
                "text/xml",
            ]
        case "rss1":
            return [
                "application/rdf+xml",
                "application/xml",
                "text/xml",
            ]
        case "rss2":
            return [
                "application/rss+xml",
                "application/xml",
                "text/xml",
            ]
        case _:
            return ["application/xml", "text/xml"]


async def _attach_page_payloads(
    client: aiohttp.ClientSession,
    posts: list[RawPost],
    *,
    concurrency: int = 4,
) -> list[RawPost]:
    if not posts:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def _attach(post: RawPost) -> RawPost:
        async with semaphore:
            page_html = await _fetch_text(
                client,
                post.url,
                accept=[
                    "text/html",
                    "application/xhtml+xml",
                    "application/xml",
                ],
            )

        created_at = post.post_created_at
        if created_at is None and page_html:
            created_at = _extract_post_created_at_from_html(page_html)

        payload = _compose_payload(post.payload, page_html or "")

        return post._replace(payload=payload, post_created_at=created_at)

    return await asyncio.gather(*(_attach(post) for post in posts))


def _compose_payload(feed_xml: str, page_html: str) -> str:
    return json.dumps(
        {"feed_xml": feed_xml, "page_html": page_html},
        separators=(",", ":"),
    )


def discover_feed_links(html: str, root_url: str) -> list[FeedLink]:
    parser = _HeadFeedLinkParser()
    parser.feed(html)
    links: list[FeedLink] = []
    seen: set[str] = set()

    for link in parser.links:
        absolute = urljoin(root_url, link.url)

        if absolute not in seen:
            seen.add(absolute)
            links.append(FeedLink(absolute, link.feed_type, link.mime_type))

    return links


def raw_posts_from_xml(
    xml: str,
    *,
    domain: str,
    feed_url: str,
    root_url: str,
) -> list[RawPost]:
    if not xml:
        return []

    root = _parse_xml_root(xml)

    if root is None:
        return []

    feed_type = _feed_type_from_root(root)

    match feed_type:
        case "atom":
            return _parse_atom_posts(
                root,
                domain=domain,
                feed_url=feed_url,
                root_url=root_url,
            )
        case "rss2":
            return _parse_rss2_posts(
                root,
                domain=domain,
                feed_url=feed_url,
                root_url=root_url,
            )
        case "rss1":
            return _parse_rss1_posts(
                root,
                domain=domain,
                feed_url=feed_url,
                root_url=root_url,
            )
        case _:
            return []


def _parse_xml_root(xml: str) -> ElementTree.Element | None:
    try:
        return ElementTree.fromstring(xml)
    except ElementTree.ParseError:
        logger.debug("Failed to parse feed XML", exc_info=True)

        return None


def _feed_type_from_root(root: ElementTree.Element) -> str | None:
    match _strip_xml_namespace(root.tag).lower():
        case "feed":
            return ATOM_FEED_TYPE
        case "rss":
            return RSS2_FEED_TYPE
        case "rdf":
            return RSS1_FEED_TYPE
        case _:
            return None


def _strip_xml_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag


def _parse_atom_posts(
    root: ElementTree.Element,
    *,
    domain: str,
    feed_url: str,
    root_url: str,
) -> list[RawPost]:
    return [
        _create_raw_post(
            payload_xml=ElementTree.tostring(entry, encoding="unicode"),
            link=_atom_entry_link(entry),
            created_at=_parse_datetime(
                _text(entry, "atom:published", ATOM_NAMESPACES)
                or _text(entry, "atom:updated", ATOM_NAMESPACES),
            ),
            domain=domain,
            feed_url=feed_url,
            root_url=root_url,
        )
        for entry in root.findall("atom:entry", ATOM_NAMESPACES)
    ]


def _atom_entry_link(entry: ElementTree.Element) -> str | None:
    links = entry.findall("atom:link", ATOM_NAMESPACES)
    # Prefer alternate links, then those with text/html types.
    links.sort(
        key=lambda link: (
            (link.get("rel") or "").lower() == "alternate",
            (link.get("type") or "").lower() == "text/html",
        ),
        reverse=True,
    )

    for link in links:
        if href := link.get("href"):
            return href

    return None


def _parse_rss2_posts(
    root: ElementTree.Element,
    *,
    domain: str,
    feed_url: str,
    root_url: str,
) -> list[RawPost]:
    if (channel := root.find("channel")) is not None:
        return [
            _create_raw_post(
                payload_xml=ElementTree.tostring(item, encoding="unicode"),
                link=_text(item, "link"),
                created_at=_parse_datetime(_text(item, "pubDate")),
                domain=domain,
                feed_url=feed_url,
                root_url=root_url,
            )
            for item in channel.findall("item")
        ]

    return []


def _parse_rss1_posts(
    root: ElementTree.Element,
    *,
    domain: str,
    feed_url: str,
    root_url: str,
) -> list[RawPost]:
    return [
        _create_raw_post(
            payload_xml=ElementTree.tostring(item, encoding="unicode"),
            link=_text(item, "rss:link", RSS1_NAMESPACES),
            created_at=None,
            domain=domain,
            feed_url=feed_url,
            root_url=root_url,
        )
        for item in root.findall("rss:item", RSS1_NAMESPACES)
    ]


def _create_raw_post(
    *,
    payload_xml: str,
    link: str | None,
    created_at: datetime.datetime | None,
    domain: str,
    feed_url: str,
    root_url: str,
) -> RawPost:
    url = urljoin(root_url, link) if link else root_url
    post_id = hashlib.sha256(url.encode()).hexdigest()

    return RawPost(
        domain=domain,
        post_id=post_id,
        url=url,
        post_created_at=created_at,
        scraped_at=datetime.datetime.now(datetime.UTC),
        source=RSS_SOURCE,
        request_url=feed_url,
        payload=payload_xml,
    )


def _text(
    node: ElementTree.Element,
    path: str,
    namespaces: dict[str, str] | None = None,
) -> str | None:
    child = node.find(path, namespaces or {})

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


def _extract_post_created_at_from_html(
    html: str,
) -> datetime.datetime | None:
    if not html:
        return None

    parser = _PostDateParser()
    parser.feed(html)

    for candidate in parser.candidates:
        parsed = _parse_date_candidate(candidate)

        if parsed is not None:
            return parsed

    return None


def _parse_date_candidate(value: str) -> datetime.datetime | None:
    if not value:
        return None

    trimmed = value.strip()

    if trimmed.endswith("Z"):
        trimmed = trimmed[:-1] + "+00:00"

    try:
        parsed = datetime.datetime.fromisoformat(trimmed)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(trimmed)
        except (TypeError, ValueError):
            return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)

    return parsed.astimezone(datetime.UTC)
