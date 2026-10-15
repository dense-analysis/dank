from __future__ import annotations

import datetime
import json
import re
from collections.abc import Iterable
from html.parser import HTMLParser
from urllib.parse import urlsplit

from dank.model import RedditCommunityDiscovery
from dank.scrape.zendriver import NetworkResponse

from .html import AD_ATTRIBUTES, VOID_TAGS

HEADER_TAG = "shreddit-subreddit-header-buttons"
ICON_CLASS = "shreddit-subreddit-icon__icon"
ICON_IDS = frozenset({"subreddit-icon-img", "subreddit-icon-img-desktop"})
LISTING_PATH = re.compile(
    r"^/r/([A-Za-z0-9_]+)(?:/(?:best|hot|new|top|rising))?/?$",
)


class _CommunityParser(HTMLParser):
    """Read the community header, excluding post, ad and author imagery."""

    def __init__(self, community: str) -> None:
        super().__init__()
        self.community = community
        self.headers: list[tuple[str, str, str]] = []
        self.icons: list[tuple[str, str]] = []
        self.stack: list[tuple[str, bool, bool]] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = {key: value or "" for key, value in attrs}
        excluded = self._excluded(tag, attributes)
        in_icon = attributes.get("id") in ICON_IDS or bool(
            self.stack and self.stack[-1][2],
        )

        if not excluded:
            self._capture(tag, attributes, in_icon=in_icon)

        if tag not in VOID_TAGS:
            self.stack.append((tag, excluded, in_icon))

    def _excluded(self, tag: str, attributes: dict[str, str]) -> bool:
        return (
            bool(self.stack and self.stack[-1][1])
            or tag in {"shreddit-post", "shreddit-ad-post"}
            or attributes.get("data-promoted") == "true"
            or any(
                key in attributes and attributes[key] != "false"
                for key in AD_ATTRIBUTES
            )
        )

    def _capture(
        self,
        tag: str,
        attributes: dict[str, str],
        *,
        in_icon: bool,
    ) -> None:
        original = self.get_starttag_text() or ""

        if tag == HEADER_TAG:
            name = attributes.get("name", "")
            community_id = attributes.get("subreddit-id", "")

            if name.casefold() == self.community.casefold() and re.fullmatch(
                r"t5_[a-z0-9]+", community_id,
            ):
                self.headers.append((name, community_id, original))
        elif tag == "img" and (
            in_icon or ICON_CLASS in attributes.get("class", "").split()
        ):
            self.icons.append((attributes.get("src", ""), original))

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                self.stack = self.stack[:index]
                break


def _listing_matches(url: str, community: str) -> bool:
    try:
        parsed = urlsplit(url)
        match = LISTING_PATH.fullmatch(parsed.path)

        return (
            parsed.scheme == "https"
            and parsed.hostname in {"reddit.com", "www.reddit.com"}
            and parsed.username is None
            and parsed.password is None
            and parsed.port in {None, 443}
            and match is not None
            and match[1].casefold() == community.casefold()
        )
    except ValueError:
        return False


def _icon_matches(url: str, community_id: str) -> bool:
    try:
        parsed = urlsplit(url)

        return (
            parsed.scheme == "https"
            and parsed.hostname == "styles.redditmedia.com"
            and parsed.username is None
            and parsed.password is None
            and parsed.port in {None, 443}
            and parsed.path.startswith(
                f"/{community_id}/styles/communityIcon_",
            )
            and not parsed.fragment
        )
    except ValueError:
        return False


def _from_html(
    source: str,
    community: str,
    request_url: str,
) -> RedditCommunityDiscovery | None:
    if not _listing_matches(request_url, community):
        return None

    parser = _CommunityParser(community)
    parser.feed(source)
    parser.close()

    if not parser.headers:
        return None

    name, community_id, header = parser.headers[0]
    icon_url, icon_html = next(
        (
            (url, original)
            for url, original in parser.icons
            if _icon_matches(url, community_id)
        ),
        ("", ""),
    )

    return RedditCommunityDiscovery(
        community=name,
        icon_url=icon_url,
        scraped_at=datetime.datetime.now(datetime.UTC),
        request_url=request_url,
        payload=json.dumps(
            {"html": header + icon_html},
            separators=(",", ":"),
        ),
    )


def discover_community(
    responses: Iterable[NetworkResponse],
    community: str,
    *,
    dom_html: str = "",
    request_url: str = "",
) -> RedditCommunityDiscovery | None:
    """Capture header identity and a custom icon from a public listing."""
    candidates = [
        (response.body, response.url)
        for response in responses
        if 200 <= response.status < 300
        and response.resource_type == "Document"
    ]
    candidates.append((dom_html, request_url))
    fallback: RedditCommunityDiscovery | None = None

    for source, origin in candidates:
        discovery = _from_html(source, community, origin)

        if discovery is not None:
            if discovery.icon_url:
                return discovery

            fallback = discovery

    return fallback
