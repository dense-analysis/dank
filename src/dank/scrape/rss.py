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
from urllib.parse import urljoin, urlparse

import aiohttp

from dank.html_utils import is_youtube_url
from dank.model import AssetDiscovery, RawPost
from dank.scrape.types import ScrapeBatch

logger = logging.getLogger(__name__)

RSS_MIME_HINTS = ("rss", "atom", "xml", "rdf")
ATOM_FEED_TYPE = "atom"
RSS2_FEED_TYPE = "rss2"
RSS1_FEED_TYPE = "rss1"
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
RSS1_NAMESPACE = "http://purl.org/rss/1.0/"
RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
ATOM_NAMESPACES = {"atom": ATOM_NAMESPACE}
RSS1_NAMESPACES = {
    "rss": RSS1_NAMESPACE,
    "rdf": RDF_NAMESPACE,
}

FEED_ACCEPT = [
    "application/atom+xml",
    "application/rss+xml",
    "application/xml",
    "text/xml",
]
HTML_ACCEPT = ["text/html", "application/xhtml+xml", "application/xml"]
MEDIA_SRC_ATTRS = (
    "src",
    "data-src",
    "data-lazy-src",
    "data-original",
    "data-lazy",
)

type FeedType = Literal["atom", "rss2", "rss1"]


class PageDiscovery(NamedTuple):
    domain: str
    url: str
    created_at: datetime.datetime | None
    payload: str


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


class _ParsedEntry(NamedTuple):
    url: str
    created_at: datetime.datetime | None
    payload: str


class _PageAssetParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()

        self._base_url = base_url
        self._assets: dict[str, str] = {}
        self._media_stack: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        tag = tag.lower()
        attr_map = _attrs_to_map(attrs)

        if tag == "video":
            self._media_stack.append("video")
        elif tag == "audio":
            self._media_stack.append("audio")

        if tag == "img":
            _add_asset(
                self._assets,
                self._base_url,
                _extract_media_url(attr_map),
                "image",
            )

            return

        if tag == "video":
            _add_asset(
                self._assets,
                self._base_url,
                _extract_media_url(attr_map),
                "video",
            )
            _add_asset(
                self._assets,
                self._base_url,
                attr_map.get("poster"),
                "image",
            )

            return

        if tag == "audio":
            _add_asset(
                self._assets,
                self._base_url,
                _extract_media_url(attr_map),
                "audio",
            )

            return

        if tag == "source" and self._media_stack:
            _add_asset(
                self._assets,
                self._base_url,
                _extract_media_url(attr_map),
                self._media_stack[-1],
            )

            return

        if tag == "iframe":
            src = attr_map.get("src", "")
            asset_type = "youtube" if is_youtube_url(src) else "iframe"
            _add_asset(self._assets, self._base_url, src, asset_type)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if tag in {"video", "audio"} and self._media_stack:
            self._media_stack.pop()

    def assets(self) -> Iterable[tuple[str, str]]:
        return self._assets.items()


async def fetch_feed_links(
    domain: str,
    *,
    timeout_seconds: float = 30.0,
) -> list[FeedLink]:
    root_url = f"https://{domain}"

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout_seconds),
    ) as http_client:
        html = await _fetch_text(http_client, root_url, accept=["text/html"])

    if not html:
        logger.warning("No HTML received for %s", root_url)

        return []

    return discover_feed_links(html, root_url)


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


def _feed_type_from_mime(type_hint: str | None) -> FeedType:
    match type_hint:
        case str() if "atom" in type_hint:
            return ATOM_FEED_TYPE
        case str() if "rdf" in type_hint:
            return RSS1_FEED_TYPE
        case _:
            return RSS2_FEED_TYPE


def parse_feed_entries(
    xml: str,
    *,
    domain: str,
    root_url: str,
) -> list[PageDiscovery]:
    if not xml:
        return []

    root = _parse_xml_root(xml)

    if root is None:
        return []

    feed_type = _feed_type_from_root(root)

    entries: list[_ParsedEntry]
    match feed_type:
        case "atom":
            entries = _parse_atom_entries(root, root_url)
        case "rss2":
            entries = _parse_rss2_entries(root, root_url)
        case "rss1":
            entries = _parse_rss1_entries(root, root_url)
        case _:
            entries = []

    return [
        PageDiscovery(
            domain=domain,
            url=entry.url,
            created_at=entry.created_at,
            payload=entry.payload,
        )
        for entry in entries
    ]


def _parse_xml_root(xml: str) -> ElementTree.Element | None:
    try:
        return ElementTree.fromstring(xml)
    except ElementTree.ParseError:
        return None


def _feed_type_from_root(root: ElementTree.Element) -> FeedType | None:
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


def _parse_atom_entries(
    root: ElementTree.Element,
    root_url: str,
) -> list[_ParsedEntry]:
    entries: list[_ParsedEntry] = []

    for entry in root.findall("atom:entry", ATOM_NAMESPACES):
        payload_xml = ElementTree.tostring(entry, encoding="unicode")
        url = urljoin(root_url, _atom_entry_link(entry) or "")
        created_at = _parse_datetime(
            _text(entry, "atom:published", ATOM_NAMESPACES)
            or _text(entry, "atom:updated", ATOM_NAMESPACES),
        )
        entries.append(
            _ParsedEntry(
                url=url or root_url,
                created_at=created_at,
                payload=payload_xml,
            ),
        )

    return entries


def _atom_entry_link(entry: ElementTree.Element) -> str | None:
    links = entry.findall("atom:link", ATOM_NAMESPACES)
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


def _parse_rss2_entries(
    root: ElementTree.Element,
    root_url: str,
) -> list[_ParsedEntry]:
    channel = root.find("channel")

    if channel is None:
        return []

    entries: list[_ParsedEntry] = []

    for item in channel.findall("item"):
        payload_xml = ElementTree.tostring(item, encoding="unicode")
        url = urljoin(root_url, _text(item, "link") or "")
        created_at = _parse_datetime(_text(item, "pubDate"))
        entries.append(
            _ParsedEntry(
                url=url or root_url,
                created_at=created_at,
                payload=payload_xml,
            ),
        )

    return entries


def _parse_rss1_entries(
    root: ElementTree.Element,
    root_url: str,
) -> list[_ParsedEntry]:
    entries: list[_ParsedEntry] = []

    for item in root.findall("rss:item", RSS1_NAMESPACES):
        payload_xml = ElementTree.tostring(item, encoding="unicode")
        url = urljoin(root_url, _text(item, "rss:link", RSS1_NAMESPACES) or "")
        entries.append(
            _ParsedEntry(
                url=url or root_url,
                created_at=None,
                payload=payload_xml,
            ),
        )

    return entries


def _text(
    node: ElementTree.Element,
    path: str,
    namespaces: dict[str, str] | None = None,
) -> str | None:
    child = node.find(path, namespaces or {})

    if child is None or child.text is None:
        return None

    return child.text.strip()


async def scrape_feed_batches(
    http_client: aiohttp.ClientSession,
    *,
    domain: str,
    feed_urls: list[str],
    batch_size: int = 50,
    concurrency: int = 4,
) -> AsyncIterator[ScrapeBatch]:
    if not feed_urls:
        return

    root_url = f"https://{domain}"
    seen_urls: set[str] = set()

    for feed_url in feed_urls:
        feed_xml = await _fetch_text(http_client, feed_url, accept=FEED_ACCEPT)

        if not feed_xml:
            continue

        discoveries = parse_feed_entries(
            feed_xml,
            domain=domain,
            root_url=root_url,
        )
        unique_discoveries = dedupe_discoveries(discoveries, seen_urls)

        for page_chunk in chunked(unique_discoveries, batch_size):
            page_results = await _fetch_pages(
                http_client,
                page_chunk,
                concurrency=concurrency,
            )
            raw_posts: list[RawPost] = []
            asset_discoveries: list[AssetDiscovery] = []
            scraped_at = datetime.datetime.now(datetime.UTC)

            for discovery, page_html in page_results:
                if not page_html:
                    continue

                raw_post, assets = _build_raw_post(
                    discovery,
                    page_html,
                    request_url=feed_url,
                    scraped_at=scraped_at,
                )
                raw_posts.append(raw_post)
                asset_discoveries.extend(assets)

            if raw_posts or asset_discoveries:
                yield ScrapeBatch(posts=raw_posts, assets=asset_discoveries)


def _build_raw_post(
    discovery: PageDiscovery,
    page_html: str,
    *,
    request_url: str,
    scraped_at: datetime.datetime,
) -> tuple[RawPost, list[AssetDiscovery]]:
    post_id = _hash_post_id(discovery.url)
    raw_post = RawPost(
        domain=discovery.domain,
        post_id=post_id,
        url=discovery.url,
        post_created_at=discovery.created_at,
        scraped_at=scraped_at,
        source="rss",
        request_url=request_url,
        payload=_compose_payload(discovery.payload, page_html),
    )
    assets = _extract_page_assets(
        page_html,
        domain=discovery.domain,
        post_id=post_id,
        base_url=discovery.url,
    )

    return raw_post, assets


def dedupe_discoveries(
    discoveries: list[PageDiscovery],
    seen_urls: set[str],
) -> list[PageDiscovery]:
    unique: dict[str, PageDiscovery] = {}

    for discovery in discoveries:
        if discovery.url and discovery.url not in seen_urls:
            seen_urls.add(discovery.url)
            unique.setdefault(discovery.url, discovery)

    return list(unique.values())


def chunked(
    items: list[PageDiscovery],
    batch_size: int,
) -> Iterable[list[PageDiscovery]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def _compose_payload(feed_xml: str, page_html: str) -> str:
    return json.dumps(
        {"feed_xml": feed_xml, "page_html": page_html},
        separators=(",", ":"),
    )


def _hash_post_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _extract_page_assets(
    page_html: str,
    *,
    domain: str,
    post_id: str,
    base_url: str | None = None,
) -> list[AssetDiscovery]:
    if not page_html:
        return []

    parser = _PageAssetParser(base_url or f"https://{domain}")
    parser.feed(page_html)

    return [
        AssetDiscovery(
            source="rss",
            domain=domain,
            post_id=post_id,
            url=url,
            asset_type=asset_type,
        )
        for url, asset_type in parser.assets()
    ]


def _extract_media_url(attr_map: dict[str, str]) -> str:
    for key in MEDIA_SRC_ATTRS:
        value = attr_map.get(key)

        if value:
            return value

    srcset = attr_map.get("srcset") or attr_map.get("data-srcset")

    if srcset:
        first = srcset.split(",", 1)[0].strip()

        if first:
            return first.split(" ", 1)[0]

    return ""


def _add_asset(
    assets: dict[str, str],
    base_url: str,
    raw_url: str | None,
    asset_type: str,
) -> None:
    if not raw_url:
        return

    normalized = _normalize_asset_url(base_url, raw_url)

    if normalized is None:
        return

    existing = assets.get(normalized)

    if existing is None or _asset_priority(asset_type) > _asset_priority(
        existing,
    ):
        assets[normalized] = asset_type


def _normalize_asset_url(base_url: str, raw_url: str) -> str | None:
    trimmed = raw_url.strip()

    if not trimmed:
        return None

    if trimmed.startswith("data:") or trimmed.startswith("javascript:"):
        return None

    absolute = urljoin(base_url, trimmed)
    parsed = urlparse(absolute)

    if parsed.scheme not in {"http", "https"}:
        return None

    return absolute


def _asset_priority(asset_type: str) -> int:
    match asset_type:
        case "youtube":
            return 4
        case "video":
            return 3
        case "audio":
            return 3
        case "image":
            return 2
        case "iframe":
            return 1
        case _:
            return 0


def _attrs_to_map(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
    return {key.lower(): (value or "") for key, value in attrs if key}


async def _fetch_pages(
    http_client: aiohttp.ClientSession,
    discoveries: list[PageDiscovery],
    *,
    concurrency: int,
) -> list[tuple[PageDiscovery, str | None]]:
    if not discoveries:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch(
        discovery: PageDiscovery,
    ) -> tuple[PageDiscovery, str | None]:
        async with semaphore:
            html = await _fetch_text(
                http_client,
                discovery.url,
                accept=HTML_ACCEPT,
            )

        return discovery, html

    return await asyncio.gather(
        *(_fetch(discovery) for discovery in discoveries),
    )


async def _fetch_text(
    http_client: aiohttp.ClientSession,
    url: str,
    *,
    accept: list[str],
) -> str | None:
    if not url:
        return None

    try:
        async with http_client.get(
            url,
            headers={"Accept": ", ".join(accept)},
        ) as response:
            response.raise_for_status()

            return await response.text()
    except Exception:
        logger.debug("Failed to fetch %s", url, exc_info=True)

        return None


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
