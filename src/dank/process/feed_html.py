"""Recover page structure without changing a full feed body's text."""

from __future__ import annotations

import html
from html.parser import HTMLParser

from dank.html_utils import html_text, remove_page_noise
from dank.process.page import VOID_TAGS, extract_article_html

BODY_BLOCKS = {
    "p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol",
    "pre", "table",
}


class _BodyBlocks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.has_markup = False
        self.blocks: list[str] = []
        self._stack: list[str] = []
        self._parts: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        self.has_markup = True

        if not self._stack and tag not in BODY_BLOCKS:
            return

        self._parts.append(self.get_starttag_text() or "")

        if tag not in VOID_TAGS:
            self._stack.append(tag)

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        self.has_markup = True

        if self._stack:
            self._parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag: str) -> None:
        if not self._stack or tag in VOID_TAGS:
            return

        # Incomplete or misnested blocks cannot safely supply structure.
        if self._stack[-1] != tag:
            self._stack.clear()
            self._parts.clear()

            return

        self._stack.pop()
        self._parts.append(f"</{tag}>")

        if not self._stack:
            self.blocks.append("".join(self._parts))
            self._parts.clear()

    def handle_data(self, data: str) -> None:
        if self._stack:
            self._parts.append(html.escape(data, quote=False))


def recover_feed_html(feed_html: str, page_html: str) -> str:
    """Return matching page blocks, or empty text to keep the original feed."""
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

    for block in page.blocks:
        text = " ".join(html_text(block).split())

        # Ignore sidebars and adverts; only take complete, ordered feed text.
        if text and (remaining == text or remaining.startswith(text + " ")):
            selected.append(block)
            remaining = remaining[len(text):].lstrip()

            if not remaining:
                return "\n".join(selected) if len(selected) > 1 else ""

    return ""
