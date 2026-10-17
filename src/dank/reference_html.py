"""Visible link destinations and excerpts within explicit HTML scopes."""

from __future__ import annotations

import html
import re
from collections.abc import Callable, Sequence
from html.parser import HTMLParser
from typing import NamedTuple
from urllib.parse import urljoin

from dank.links import normalized_url

Identity = Callable[[str], str | None]

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
    key: str
    url: str
    excerpt: str


class _Content(HTMLParser):
    def __init__(
        self, *, embedded: bool = False, comments: bool = False,
        identity: Identity = normalized_url, base: str = "",
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.identity = identity
        self.base = base
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
            or attributes.get("aria-hidden") == "true"
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

        target = self._destination(target)

        if target and self.identity(target):
            self.links.append((target, len(self.text)))

            if tag != "a":
                self.text += "[Embedded media] "

        if tag == "iframe" and attributes.get("srcdoc") and not self.embedded:
            inner = _Content(
                embedded=True, identity=self.identity, base=self.base,
            )
            inner.feed(attributes["srcdoc"] or "")
            self.links.extend((url, len(self.text)) for url, _ in inner.links)
            self.text += inner.text

    def _destination(self, target: str | None) -> str | None:
        if not target or target.startswith("#"):
            return None

        try:
            return urljoin(self.base, target)
        except ValueError:
            return None

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
                    (_text_url(m.group()), len(self.text) + m.start())
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
        if (
            "author" in (attributes.get("rel") or "").split()
            or set(markers) & {
                "comment-author", "comment-meta", "comment-metadata", "reply",
                "comment-reply-link", "fn", "url",
            }
        ):
            return True

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

    def __init__(self, *, identity: Identity, base: str) -> None:
        super().__init__(identity=identity, base=base)
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
    identity: Identity = normalized_url,
    base: str = "",
    known_urls: Sequence[str] = (),
) -> list[Link]:
    """Extract destinations and visible URLs from attributable content."""
    if markup:
        parser = (
            _ArticleContent(identity=identity, base=base) if article
            else _Content(identity=identity, base=base)
        )
        parser.feed(content)
        text, candidates = parser.text, parser.links
    else:
        text, candidates = html.unescape(content), []

    if not markup:
        candidates = _text_candidates(text, known_urls)
    return _links(text, candidates, identity)


def _text_url(value: str) -> str:
    # Closing punctuation belongs to a URL when its delimiter is balanced
    # (Wikipedia paths are a common example). Query punctuation is meaningful.
    value = value if "?" in value else value.rstrip(".,;:")

    for closing, opening in ((")", "("), ("]", "["), ("}", "{")):
        while (value.endswith(closing)
               and value.count(closing) > value.count(opening)):
            value = value[:-1]

    return value


def _text_candidates(
    text: str, known_urls: Sequence[str],
) -> list[tuple[str, int]]:
    # Structured URL entities are authoritative, even when ordinary plaintext
    # tokenization would truncate punctuation or an apostrophe in their query.
    known = [(target, text.find(target)) for target in known_urls if target]
    known = [(target, start) for target, start in known if start >= 0]
    candidates = [
        (_text_url(match.group()), match.start())
        for match in URL.finditer(text)
        if not any(start <= match.start() < start + len(target)
                   for target, start in known)
    ]

    return sorted([*candidates, *known], key=lambda candidate: candidate[1])


def _links(
    text: str, candidates: list[tuple[str, int]], identity: Identity,
) -> list[Link]:
    links: dict[str, Link] = {}

    for target, position in candidates:
        key = identity(target)

        # Specialized identities (e.g. a YouTube video ID) can reject sentence
        # punctuation that is otherwise a valid generic URL query character.
        if key is None:
            trimmed = _text_url(target.rstrip(".,;:!?"))
            key = identity(trimmed)

            if key:
                target = trimmed

        if key and key not in links:
            links[key] = Link(
                key,
                target,
                _excerpt(text, position) or target,
            )

    return list(links.values())


class _CommentBlocks(HTMLParser):
    """Only comments with explicit IDs; nested replies stay separate."""

    def __init__(self, *, identity: Identity, base: str) -> None:
        super().__init__()
        self.identity = identity
        self.base = base
        self.stack: list[str] = []
        self.active: list[tuple[str, int, _Content]] = []
        self.blocked = 0
        self.comments: list[tuple[str, Link]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        comment_id = attributes.get("id") or ""

        if tag not in VOID_TAGS:
            self.stack.append(tag)

        if self.blocked:
            return

        if (
            tag in IGNORED_TAGS | {"aside"}
            or "hidden" in attributes
            or attributes.get("aria-hidden") == "true"
            or _noise(attributes, comments=True)
        ):
            self.blocked = len(self.stack) if tag not in VOID_TAGS else 0

            return

        if (tag in {"article", "li", "div"}
                and re.fullmatch(r"comment-\d+", comment_id)):
            self.active.append((
                comment_id, len(self.stack), _Content(
                    comments=True, identity=self.identity, base=self.base,
                ),
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

        if self.blocked:
            if index >= self.blocked:
                del self.stack[index:]

                return

            self.blocked = 0

        if self.active:
            comment_id, depth, parser = self.active[-1]

            if index < depth:
                self.comments.extend(
                    (comment_id, link)
                    for link in _links(
                        parser.text, parser.links, self.identity,
                    )
                )
                self.active.pop()
            else:
                parser.handle_endtag(tag)

        del self.stack[index:]

    def handle_data(self, data: str) -> None:
        if self.active and not self.blocked:
            self.active[-1][2].handle_data(data)


def comment_links(
    content: str, *, identity: Identity = normalized_url, base: str = "",
) -> list[tuple[str, Link]]:
    if not re.search(r"\bid\s*=\s*['\"]?comment-\d+", content):
        return []

    parser = _CommentBlocks(identity=identity, base=base)
    parser.feed(content)

    return parser.comments
