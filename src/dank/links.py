"""URL identities and links between collected posts, without network access."""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import re
from html.parser import HTMLParser
from typing import NamedTuple
from urllib.parse import urljoin, urlsplit, urlunsplit

from dank.youtube import canonical_youtube_url


class LinkPost(NamedTuple):
    domain: str
    post_id: str
    url: str
    html: str
    source: str
    updated_at: dt.datetime


def normalized_url(value: str) -> str | None:
    """Normalize identity while preserving publisher path/query meaning."""
    try:
        parsed = urlsplit(html.unescape(value.strip()))

        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password
                or any(char.isspace() for char in value)):
            return None

        host = parsed.hostname.lower()
        port = parsed.port
        path = parsed.path or "/"
        query = parsed.query
        standard_port = port is None or (parsed.scheme, port) in {
            ("http", 80), ("https", 443),
        }

        if standard_port and host in {
            "x.com", "www.x.com", "twitter.com", "www.twitter.com",
        }:
            match = re.fullmatch(r"/[^/]+/status/(\d+)/?", path)

            if match:
                return f"https://x.com/i/status/{match[1]}"

        if standard_port and host in {
            "reddit.com", "www.reddit.com", "old.reddit.com",
            "new.reddit.com", "m.reddit.com",
        }:
            match = re.fullmatch(
                r"/(?:r/[^/]+/)?comments/([a-zA-Z0-9]+)(?:/[^/]+)?/?", path,
            )

            if match:
                return f"https://www.reddit.com/comments/{match[1].lower()}"

        if standard_port and (youtube := canonical_youtube_url(value)):
            return youtube

        netloc = f"[{host}]" if ":" in host else host

        if port is not None and (parsed.scheme, port) not in {
            ("http", 80), ("https", 443),
        }:
            netloc += f":{port}"

        return urlunsplit((parsed.scheme, netloc, path, query, ""))
    except ValueError:
        return None


def publisher_domain(value: str) -> str:
    return (urlsplit(value).hostname or "").removeprefix("www.")


def is_original_source(domain: str, url: str) -> bool:
    canonical = normalized_url(url)

    if canonical is None:
        return False

    host = publisher_domain(canonical)
    source = domain.lower().removeprefix("www.")

    if source == "twitter.com":
        source = "x.com"

    return source == host


def is_platform_url(value: str) -> bool:
    return publisher_domain(value) in {
        "x.com", "twitter.com", "youtube.com", "youtu.be", "reddit.com",
        "old.reddit.com", "news.ycombinator.com",
    }


class _Links(HTMLParser):
    def __init__(self, base: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base = base
        self.urls: list[str] = []
        self.primary: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "a":
            return

        href = dict(attrs).get("href")

        if not href:
            return

        try:
            url = normalized_url(urljoin(self.base, href))
        except ValueError:
            return

        if url:
            self.urls.append(url)

            if dict(attrs).get("data-dank-hn-destination") == "1":
                self.primary = url


def post_index_rows(
    post: LinkPost,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    own_url = normalized_url(post.url)
    identities: list[dict[str, object]] = [{
        "url": own_url, "domain": post.domain, "post_id": post.post_id,
        "observed_at": post.updated_at,
    }] if own_url else []
    parser = _Links(post.url)
    parser.feed(post.html)
    primary = parser.primary if post.source == "hacker_news" else None
    aggregated = (
        post.source == "rss" and own_url is not None
        and not is_original_source(post.domain, post.url)
    )

    if aggregated and own_url:
        primary = own_url
        parser.urls.insert(0, own_url)

    links: list[dict[str, object]] = [{
        "domain": post.domain, "post_id": post.post_id,
        "target_url": url, "kind": "primary" if url == primary else "link",
        "observed_at": post.updated_at,
        "content_hash": hashlib.sha256(post.html.encode()).hexdigest().upper(),
    } for url in dict.fromkeys(parser.urls) if url != own_url or aggregated]

    return identities, links
