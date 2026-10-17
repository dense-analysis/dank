"""Visible video destinations and excerpts within explicit HTML scopes."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import NamedTuple

from dank.youtube import canonical_video_id

# A short passage around the link, rather than the beginning of a long comment.
EXCERPT_CHARS = 360
URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
IGNORED_TAGS = {
    "script",
    "style",
    "template",
    "noscript",
    "nav",
    "header",
    "footer",
}
VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}


class Link(NamedTuple):
    video_id: str
    url: str
    excerpt: str


class _Content(HTMLParser):
    def __init__(
        self, *, embedded: bool = False, comments: bool = False,
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.comments = comments
        self.embedded = embedded
        self.anchor = False
        self.text = ""
        self.links: list[tuple[str, int]] = []
        self.ignored: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        tag = tag.rsplit(":", 1)[-1]
        attributes = dict(attrs)

        slot = tag == "template" and _content_slot(attributes)
        if (
            self.ignored
            or (tag in IGNORED_TAGS and not slot)
            or "hidden" in attributes
            or _noise(attributes, comments=self.comments)
        ):
            if tag not in VOID_TAGS:
                self.ignored.append(tag)

            return

        if tag in {"p", "div", "li", "br", "blockquote"}:
            self.text += " "

        if tag == "a":
            self.anchor = True
            target = attributes.get("href")
        elif tag in {"iframe", "embed"} or (
            tag == "div"
            and attributes.get("data-component") == "youtube-facade"
        ):
            target = attributes.get("src") or attributes.get("data-src")
        else:
            target = None

        if target and canonical_video_id(target):
            self.links.append((target, len(self.text)))

            if tag != "a":
                self.text += "[Embedded YouTube video] "

        if tag == "iframe" and attributes.get("srcdoc") and not self.embedded:
            inner = _Content(embedded=True)
            inner.feed(attributes["srcdoc"] or "")
            self.links.extend((url, len(self.text)) for url, _ in inner.links)
            self.text += inner.text

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.rsplit(":", 1)[-1]

        if tag == "a":
            self.anchor = False

        if self.ignored:
            if tag in self.ignored:
                index = len(self.ignored) - 1 - self.ignored[::-1].index(tag)
                del self.ignored[index:]

            return

        self.text += " "

    def handle_data(self, data: str) -> None:
        if not self.ignored:
            value = re.sub(r"\s+", " ", data)

            if not self.anchor:
                self.links.extend(
                    (m.group().rstrip(".,;:!?)]}"), len(self.text) + m.start())
                    for m in URL.finditer(value)
                )

            self.text += value


def _content_slot(attributes: dict[str, str | None]) -> bool:
    return attributes.get("slot") in {"content", "video"} or bool(
        attributes.keys() & {"v-slot:content", "v-slot:video"},
    )


def _noise(
    attributes: dict[str, str | None], *, comments: bool = False,
) -> bool:
    markers = (attributes.get("class") or "").split()
    markers.append(attributes.get("id") or "")

    if comments:
        markers = [m for m in markers if not m.startswith("comment")]

    return any(
        re.search(
            r"^(?:comment|comments|related|recommended)(?:[-_]|$)",
            marker,
        )
        for marker in markers
    )


class _ArticleContent(_Content):
    """Require an explicit primary article/body scope in raw page HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.scope = 0
        self.finished = False

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attr = dict(attrs)
        markers = set((attr.get("class") or "").split())
        markers.add(attr.get("id") or "")
        body = (
            bool(
                markers
                & {
                    "article-body",
                    "article-content",
                    "entry-content",
                    "post-content",
                    "post-body",
                    "paperBody",
                },
            )
            or attr.get("itemprop") == "articleBody"
        )
        slot = tag == "template" and _content_slot(attr)
        eligible = (tag == "article" or body or slot) and not _noise(attr)
        blocked = any(t in IGNORED_TAGS | {"aside"} for t in self.stack)

        if tag not in VOID_TAGS:
            self.stack.append(tag)

        if not self.scope and not self.finished and eligible and not blocked:
            self.scope = len(self.stack)

        if self.scope:
            super().handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if self.scope:
            super().handle_endtag(tag)

        if tag in self.stack:
            index = len(self.stack) - 1 - self.stack[::-1].index(tag)
            del self.stack[index:]

        if self.scope and len(self.stack) < self.scope:
            self.scope = 0
            self.finished = tag != "template"

    def handle_data(self, data: str) -> None:
        if self.scope:
            super().handle_data(data)


def _excerpt(text: str, position: int) -> str:
    start = max(0, position - EXCERPT_CHARS // 3)
    end = min(len(text), start + EXCERPT_CHARS)

    return (
        ("…" if start else "")
        + " ".join(text[start:end].split())
        + ("…" if end < len(text) else "")
    )


def content_links(
    content: str,
    *,
    markup: bool = True,
    article: bool = False,
) -> list[Link]:
    """Extract destinations and visible URLs from attributable content."""
    if markup:
        parser = _ArticleContent() if article else _Content()
        parser.feed(content)
        text, candidates = parser.text, parser.links
    else:
        text, candidates = html.unescape(content), []

    if not markup:
        candidates.extend(
            (match.group().rstrip(".,;:!?)]}"), match.start())
            for match in URL.finditer(text)
        )
    return _links(text, candidates)


def _links(text: str, candidates: list[tuple[str, int]]) -> list[Link]:
    links: dict[str, Link] = {}

    for target, position in candidates:
        video_id = canonical_video_id(target)

        if video_id and video_id not in links:
            links[video_id] = Link(
                video_id,
                target,
                _excerpt(text, position) or target,
            )

    return list(links.values())


class _CommentBlocks(HTMLParser):
    """Only comments with explicit IDs; nested replies stay separate."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.active: list[tuple[str, int, _Content]] = []
        self.comments: list[tuple[str, Link]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        comment_id = dict(attrs).get("id") or ""

        if tag not in VOID_TAGS:
            self.stack.append(tag)

        if (tag in {"article", "li", "div"}
                and re.fullmatch(r"comment-\d+", comment_id)):
            self.active.append((
                comment_id, len(self.stack), _Content(comments=True),
            ))
        elif self.active:
            self.active[-1][2].handle_starttag(tag, attrs)

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag not in self.stack:
            return

        index = len(self.stack) - 1 - self.stack[::-1].index(tag)

        if self.active:
            comment_id, depth, parser = self.active[-1]

            if index < depth:
                self.comments.extend(
                    (comment_id, link)
                    for link in _links(parser.text, parser.links)
                )
                self.active.pop()
            else:
                parser.handle_endtag(tag)

        del self.stack[index:]

    def handle_data(self, data: str) -> None:
        if self.active:
            self.active[-1][2].handle_data(data)


def comment_links(content: str) -> list[tuple[str, Link]]:
    parser = _CommentBlocks()
    parser.feed(content)

    return parser.comments
