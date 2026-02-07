from __future__ import annotations

import datetime
import html
import json
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any, NamedTuple, cast

from dank.html_utils import is_youtube_url


class PageMetadata(NamedTuple):
    title: str
    author: str
    published_at: datetime.datetime | None


class _ContentCandidate(NamedTuple):
    kind: str
    html: str


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
CONTENT_CLASS_KEYS = {
    "article-body",
    "article-content",
    "content-body",
    "entry-content",
    "post-body",
    "post-content",
    "single-post",
    "single-video",
}
CONTENT_ID_KEYS = {
    "article-body",
    "article-content",
    "content-body",
    "entry-content",
    "post-body",
    "post-content",
}
CONTENT_PRIORITY = (
    "template-content",
    "template-video",
    "entry-content",
    "article-main",
    "article",
    "content-block",
)
MEDIA_SCOPE_CLASSES = {
    "article-body",
    "article-content",
    "content-body",
    "entry-content",
    "post-body",
    "post-content",
    "single-post",
    "single-video",
}
MEDIA_SCOPE_IDS = {
    "article-body",
    "article-content",
    "content-body",
    "entry-content",
    "post-body",
    "post-content",
}
MEDIA_SRC_ATTRS = (
    "src",
    "data-src",
    "data-lazy-src",
    "data-original",
    "data-lazy",
)


class _PageMetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.meta: dict[str, str] = {}
        self.title = ""
        self._in_title = False

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "meta":
            attr_map = _attrs_to_map(attrs)
            key = attr_map.get("property") or attr_map.get("name")
            content = attr_map.get("content") or attr_map.get("value")

            if key and content:
                key = key.lower()
                if key not in self.meta:
                    self.meta[key] = content

            return

        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data


class _JsonLdParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.blocks: list[str] = []
        self._in_block = False
        self._buffer: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "script":
            return

        attr_map = _attrs_to_map(attrs)
        script_type = attr_map.get("type", "").lower()

        if script_type == "application/ld+json":
            self._in_block = True
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag != "script" or not self._in_block:
            return

        self.blocks.append("".join(self._buffer))
        self._buffer = []
        self._in_block = False

    def handle_data(self, data: str) -> None:
        if self._in_block:
            self._buffer.append(data)


class _ContentCapture:
    def __init__(self, kind: str, tag: str) -> None:
        self.kind = kind
        self.tag = tag
        self.depth = 0
        self.parts: list[str] = []


class _ContentExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.candidates: list[_ContentCandidate] = []
        self._capture: _ContentCapture | None = None
        self._stack: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        tag = tag.lower()
        attr_map = _attrs_to_map(attrs)
        candidate_kind = None

        if self._capture is None:
            candidate_kind = _content_candidate_kind(
                tag, attr_map, self._stack,
            )

        is_void = tag in VOID_TAGS

        if not is_void:
            self._stack.append(tag)

        if candidate_kind and self._capture is None:
            self._capture = _ContentCapture(candidate_kind, tag)

            return

        if self._capture is None:
            return

        self._capture.parts.append(_render_start_tag(tag, attrs))

        if not is_void:
            self._capture.depth += 1

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if self._capture is None:
            return

        self._capture.parts.append(
            _render_start_tag(tag, attrs, self_closing=True),
        )

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if self._capture is not None:
            if tag == self._capture.tag and self._capture.depth == 0:
                html_value = "".join(self._capture.parts).strip()
                if html_value:
                    self.candidates.append(
                        _ContentCandidate(self._capture.kind, html_value),
                    )
                self._capture = None
            else:
                self._capture.parts.append(f"</{tag}>")
                if self._capture.depth > 0:
                    self._capture.depth -= 1

        if self._stack and self._stack[-1] == tag:
            self._stack.pop()

    def handle_data(self, data: str) -> None:
        if self._capture is None:
            return

        self._capture.parts.append(data)


class _YouTubeEmbedParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.iframes: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "iframe":
            return

        attr_map = _attrs_to_map(attrs)
        src = attr_map.get("src")

        if not src or not is_youtube_url(src):
            return

        self.iframes.append(_render_start_tag(tag, attrs) + "</iframe>")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()

        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)


def extract_page_metadata(page_html: str) -> PageMetadata:
    if not page_html:
        return PageMetadata(title="", author="", published_at=None)

    parser = _PageMetaParser()
    parser.feed(page_html)
    meta = parser.meta
    title = (
        meta.get("og:title")
        or meta.get("twitter:title")
        or parser.title.strip()
    )
    author = _select_author(meta, _extract_jsonld_author(page_html))
    published_at = _parse_datetime(
        meta.get("article:published_time")
        or meta.get("og:published_time")
        or meta.get("published_time"),
    )

    return PageMetadata(title=title, author=author, published_at=published_at)


def extract_article_html(page_html: str) -> str:
    if not page_html:
        return ""

    extractor = _ContentExtractor()
    extractor.feed(page_html)
    candidate = _select_best_candidate(extractor.candidates)

    return candidate


def extract_youtube_iframes(page_html: str) -> str:
    if not page_html:
        return ""

    parser = _YouTubeEmbedParser()
    parser.feed(page_html)

    if not parser.iframes:
        return ""

    return "\n".join(parser.iframes)


def strip_html(value: str) -> str:
    if not value:
        return ""

    extractor = _TextExtractor()
    extractor.feed(value)

    return "".join(extractor.parts).strip()


def _select_author(meta: dict[str, str], jsonld_author: str) -> str:
    author = meta.get("author") or meta.get("article:author")

    if author:
        return author

    label = meta.get("twitter:label1", "").strip().lower()
    if label in {"written by", "author", "by"}:
        data = meta.get("twitter:data1", "").strip()
        if data:
            return data

    creator = meta.get("twitter:creator", "").strip()
    if creator.startswith("@"):
        return creator[1:]

    return jsonld_author


def _extract_jsonld_author(page_html: str) -> str:
    parser = _JsonLdParser()
    parser.feed(page_html)

    for block in parser.blocks:
        try:
            parsed = json.loads(block)
        except ValueError:
            continue

        for node in _iter_jsonld_nodes(parsed):
            if not isinstance(node, dict):
                continue

            author = _author_from_jsonld_node(cast(dict[str, Any], node))
            if author:
                return author

    return ""


def _author_from_jsonld_node(node: dict[str, Any]) -> str:
    author = node.get("author") or node.get("creator")

    if isinstance(author, str):
        return author

    if isinstance(author, dict):
        author = cast(dict[str, Any], author)
        name = author.get("name")
        if isinstance(name, str):
            return name

    if isinstance(author, list):
        for item in cast(list[Any], author):
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                item = cast(dict[str, Any], item)
                name = item.get("name")
                if isinstance(name, str):
                    return name

    return ""


def _iter_jsonld_nodes(value: Any) -> list[Any]:
    nodes: list[Any] = []

    if isinstance(value, dict):
        value = cast(dict[str, Any], value)
        nodes.append(value)
        for item in value.values():
            nodes.extend(_iter_jsonld_nodes(item))
    elif isinstance(value, list):
        for item in cast(list[Any], value):
            nodes.extend(_iter_jsonld_nodes(item))

    return nodes


def _select_best_candidate(candidates: list[_ContentCandidate]) -> str:
    if not candidates:
        return ""

    for kind in CONTENT_PRIORITY:
        entries = [
            candidate
            for candidate in candidates
            if candidate.kind == kind and candidate.html.strip()
        ]

        if entries:
            best = max(entries, key=lambda candidate: len(candidate.html))

            return best.html

    return ""


def _content_candidate_kind(
    tag: str,
    attr_map: dict[str, str],
    stack: list[str],
) -> str | None:
    if tag == "template":
        if _has_parent_tag(stack, {"single-post", "single-video"}):
            if _is_template_slot(attr_map, "content"):
                return "template-content"

        if _has_parent_tag(stack, {"single-video"}):
            if _is_template_slot(attr_map, "video"):
                return "template-video"

        return None

    if tag == "article":
        if _has_parent_tag(stack, {"main"}):
            return "article-main"

        return "article"

    if tag in {"div", "section", "main"}:
        classes = _classes_from_attrs(attr_map)

        if "entry-content" in classes:
            return "entry-content"

        if classes & CONTENT_CLASS_KEYS:
            return "content-block"

        element_id = attr_map.get("id", "")
        if _id_has_content_key(element_id):
            return "content-block"

    return None



def _has_parent_tag(stack: list[str], targets: set[str]) -> bool:
    return any(tag in targets for tag in stack)


def _is_template_slot(attr_map: dict[str, str], slot: str) -> bool:
    if f"v-slot:{slot}" in attr_map:
        return True

    return attr_map.get("slot") == slot


def _classes_from_attrs(attr_map: dict[str, str]) -> set[str]:
    class_value = attr_map.get("class", "")

    return {part for part in class_value.lower().split() if part}


def _id_has_content_key(value: str) -> bool:
    if not value:
        return False

    lowered = value.lower()

    return any(key in lowered for key in CONTENT_ID_KEYS)


def _attrs_to_map(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
    return {key.lower(): (value or "") for key, value in attrs if key}


def _render_start_tag(
    tag: str,
    attrs: list[tuple[str, str | None]],
    *,
    self_closing: bool = False,
) -> str:
    if not attrs:
        if self_closing:
            return f"<{tag} />"

        return f"<{tag}>"

    rendered: list[str] = [tag]

    for key, value in attrs:
        if value is None:
            rendered.append(key)
        else:
            escaped = html.escape(value, quote=True)
            rendered.append(f'{key}="{escaped}"')

    suffix = " /" if self_closing else ""

    return f"<{' '.join(rendered)}{suffix}>"


def _parse_datetime(value: str | None) -> datetime.datetime | None:
    if value is None:
        return None

    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        try:
            return parsedate_to_datetime(value)
        except (TypeError, ValueError):
            try:
                return datetime.datetime.fromisoformat(
                    value.replace("Z", "+00:00"),
                )
            except ValueError:
                return None
