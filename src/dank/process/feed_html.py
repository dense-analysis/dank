"""Recover a full feed body's structure and adjacent source figures."""

from __future__ import annotations

import html
from html.parser import HTMLParser
from typing import NamedTuple
from urllib.parse import urljoin, urlsplit

from dank.html_utils import VOID_TAGS, html_text, remove_page_noise
from dank.process.page import extract_article_html

BODY_BLOCKS = {
    "p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol",
    "pre", "table",
}
MEDIA_BLOCKS = {"figure", "picture", "img"}


class _ExcerptLink(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__()
        self.page_url = page_url
        self.href: str | None = None
        self.parts: list[str] = []
        self.found = False

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "a":
            self.href = dict(attrs).get("href")
            self.parts.clear()

    def handle_data(self, data: str) -> None:
        if self.href is not None:
            self.parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a":
            return

        label = " ".join("".join(self.parts).split()).casefold()

        if label == "read full article" and self.href:
            self.found |= self._links_to_article(self.href)

        self.href = None

    def _links_to_article(self, href: str) -> bool:
        if href.startswith("#"):
            return False

        try:
            linked = urlsplit(urljoin(self.page_url, href))
            page = urlsplit(self.page_url)

            return (
                linked.scheme in {"http", "https"}
                and not linked.username and not linked.password
                and linked.hostname == page.hostname
                and linked.port == page.port
                and linked.path.rstrip("/") == page.path.rstrip("/")
                and linked.query == page.query
            )
        except ValueError:
            return False


def feed_is_excerpt(value: str, page_url: str) -> bool:
    """An explicit self-link can identify excerpts inside content:encoded."""
    parser = _ExcerptLink(page_url)
    parser.feed(value)

    return parser.found


class _BodyBlock(NamedTuple):
    html: str
    parent: tuple[int, ...]
    media: bool


def _usable_image(attrs: list[tuple[str, str | None]]) -> bool:
    values = dict(attrs)

    return bool(values.get("src")) and not (
        "hidden" in values or values.get("aria-hidden") == "true"
        or values.get("width") in {"0", "1"}
        or values.get("height") in {"0", "1"}
    )


class _BodyBlocks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.has_markup = False
        self.blocks: list[_BodyBlock] = []
        self._stack: list[str] = []
        self._parts: list[str] = []
        self._parents: list[tuple[str, int]] = []
        self._next_parent = 0
        self._kind = ""
        self._has_image = False

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        self.has_markup = True

        if not self._stack:
            if tag not in BODY_BLOCKS | MEDIA_BLOCKS:
                if tag not in VOID_TAGS:
                    self._parents.append((tag, self._next_parent))
                    self._next_parent += 1

                return

            self._kind = tag
            self._has_image = False

        self._parts.append(self.get_starttag_text() or "")
        self._has_image |= tag == "img" and _usable_image(attrs)

        if tag not in VOID_TAGS:
            self._stack.append(tag)
        elif not self._stack:
            self._finish_block()

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        self.has_markup = True

        if self._stack:
            self._parts.append(self.get_starttag_text() or "")
            self._has_image |= tag == "img" and _usable_image(attrs)
        elif tag == "img":
            self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if tag in VOID_TAGS:
            return

        if not self._stack:
            if self._parents and self._parents[-1][0] == tag:
                self._parents.pop()

            return

        # Incomplete or misnested blocks cannot safely supply structure.
        if self._stack[-1] != tag:
            self._stack.clear()
            self._parts.clear()

            return

        self._stack.pop()
        self._parts.append(f"</{tag}>")

        if not self._stack:
            self._finish_block()

    def _finish_block(self) -> None:
        self.blocks.append(_BodyBlock(
            "".join(self._parts),
            tuple(index for _, index in self._parents),
            self._kind in MEDIA_BLOCKS and self._has_image,
        ))
        self._parts.clear()

    def handle_data(self, data: str) -> None:
        if self._stack:
            self._parts.append(html.escape(data, quote=False))


def recover_feed_html(feed_html: str, page_html: str) -> str:
    """Return matching text and anchored media, or keep the original feed."""
    feed = _BodyBlocks()
    feed.feed(feed_html)

    # Existing feed markup may contain media or comment-specific formatting.
    if feed.has_markup:
        return ""

    remaining = " ".join(html_text(feed_html).split())

    if not remaining:
        return ""

    page = _BodyBlocks()
    page.feed(remove_page_noise(extract_article_html(page_html)))
    selected: list[str] = []
    pending_media: list[_BodyBlock] = []
    previous_parent: tuple[int, ...] | None = None
    matched_blocks = 0

    for block in page.blocks:
        text = " ".join(html_text(block.html).split())

        # Ignore sidebars and adverts; only take complete, ordered feed text.
        if text and (remaining == text or remaining.startswith(text + " ")):
            # Keep media only between matching sibling text blocks.
            if previous_parent == block.parent:
                selected.extend(
                    media.html for media in pending_media
                    if media.parent == block.parent
                )

            pending_media.clear()
            selected.append(block.html)
            previous_parent = block.parent
            matched_blocks += 1
            remaining = remaining[len(text):].lstrip()

            if not remaining:
                return "\n".join(selected) if matched_blocks > 1 else ""
        elif block.media and previous_parent is not None:
            pending_media.append(block)
        else:
            pending_media.clear()
            previous_parent = None

    return ""
