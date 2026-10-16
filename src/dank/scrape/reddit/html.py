from __future__ import annotations

import html
import json
from html.parser import HTMLParser
from typing import cast
from urllib.parse import urlsplit

from .payloads import as_dict, normalize_post, public_url

VOID_TAGS = frozenset(
    {
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
    },
)
AD_ATTRIBUTES = frozenset({"promoted", "is-promoted", "is-ad"})


class _ListingParser(HTMLParser):
    def __init__(self, source: str, community: str) -> None:
        super().__init__(convert_charrefs=False)
        self.source = source
        self.community = community
        self.posts: list[dict[str, object]] = []
        self.stack: list[str] = []
        self.attributes: dict[str, str] | None = None
        self.start = 0
        self.body_start: int | None = None
        self.body_depth = 0
        self.body = ""
        self.media: list[dict[str, object]] = []
        self.flair: list[str] = []
        self.excluded_depth = 0
        self.line_offsets = [0]

        for line in source.splitlines(keepends=True):
            self.line_offsets.append(self.line_offsets[-1] + len(line))

    def _offset(self) -> int:
        line, column = self.getpos()

        return self.line_offsets[line - 1] + column

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = {key: value or "" for key, value in attrs}

        if self.excluded_depth:
            self.excluded_depth += int(tag not in VOID_TAGS)
            return

        if (
            tag == "shreddit-ad-post"
            or attributes.get("data-promoted") == "true"
            or any(
                key in attributes and attributes[key] != "false"
                for key in AD_ATTRIBUTES
            )
            or (tag == "shreddit-post" and self.attributes is not None)
        ):
            self.excluded_depth = int(tag not in VOID_TAGS)
            return

        if tag == "shreddit-post" and self.attributes is None:
            self.attributes = attributes
            self.start = self._offset()
            self.stack = []
            self.body = ""
            self.media = []
            self.flair = []
            self.body_start = None

        if self.attributes is None:
            return

        if tag not in VOID_TAGS:
            self.stack.append(tag)

        if attributes.get("slot") == "text-body":
            self.body_start = self._offset() + len(
                self.get_starttag_text() or "",
            )
            self.body_depth = len(self.stack)

        self._capture_media(tag, attributes)

    def _capture_media(self, tag: str, attributes: dict[str, str]) -> None:
        if tag in {"shreddit-player", "video", "source"}:
            video = _player_media(attributes)

            if video:
                self.media.append(video)
        elif tag == "img":
            url = public_url(attributes.get("src", ""))
            host = urlsplit(url).hostname

            if (
                host
                in {"i.redd.it", "preview.redd.it", "external-preview.redd.it"}
                and "/snoovatar/" not in url
            ):
                self.media.append({"url": url, "asset_type": "photo"})

    def handle_data(self, data: str) -> None:
        if not self.excluded_depth and "shreddit-post-flair" in self.stack:
            self.flair.append(data)

    def handle_entityref(self, name: str) -> None:
        self.handle_data(html.unescape(f"&{name};"))

    def handle_charref(self, name: str) -> None:
        self.handle_data(html.unescape(f"&#{name};"))

    def handle_endtag(self, tag: str) -> None:
        if tag in VOID_TAGS:
            return

        if self.excluded_depth:
            self.excluded_depth -= 1
            return

        if self.attributes is None or tag not in self.stack:
            return

        depth = len(self.stack) - self.stack[::-1].index(tag)

        if self.body_start is not None and depth == self.body_depth:
            self.body = self.source[self.body_start : self._offset()]
            self.body_start = None

        self.stack = self.stack[: depth - 1]

        if tag == "shreddit-post":
            end = self.source.find(">", self._offset()) + 1
            self._finish(self.source[self.start : end])
            self.attributes = None

    def _finish(self, original: str) -> None:
        attrs = self.attributes or {}

        if any(
            key in attrs and attrs[key] != "false" for key in AD_ATTRIBUTES
        ):
            return

        candidate: dict[str, object] = {
            "id": attrs.get("id", ""),
            "title": attrs.get("post-title", ""),
            "author": attrs.get("author", ""),
            "permalink": attrs.get("permalink", ""),
            "url": attrs.get("content-href", ""),
            "created_utc": attrs.get("created-timestamp", ""),
            "selftext_html": self.body,
            "score": attrs.get("score"),
            "num_comments": attrs.get("comment-count"),
            "is_self": attrs.get("post-type") == "text",
            "over_18": "nsfw" in attrs,
            "spoiler": "spoiler" in attrs,
            "media": self.media,
            "link_flair_text": " ".join("".join(self.flair).split()),
            "raw": {"html": original},
        }
        post = normalize_post(candidate, self.community)

        if post is not None:
            self.posts.append(post)


def extract_html_posts(source: str, community: str) -> list[dict[str, object]]:
    parser = _ListingParser(source, community)
    parser.feed(source)
    parser.close()

    return parser.posts


def _player_media(attributes: dict[str, str]) -> dict[str, object]:
    # Reddit exposes muxed MP4 variants in this player attribute.
    try:
        playback = as_dict(json.loads(
            attributes.get("packaged-media-json")
            or attributes.get("packaged-media", "{}"),
        ))
    except (ValueError, RecursionError):
        playback = {}

    sources = as_dict(playback.get("playbackUrls"))
    hls = public_url(sources.get("hls")) or public_url(
        attributes.get("hls-url"),
    )
    url = public_url(attributes.get("src"))

    if urlsplit(url).path.endswith(".m3u8"):
        hls = hls or url

    permutations = as_dict(playback.get("playbackMp4s")).get("permutations")
    packaged = ""

    if isinstance(permutations, list):
        for item in cast(list[object], permutations):
            packaged = public_url(
                as_dict(as_dict(item).get("source")).get("url"),
            ) or packaged

    poster = public_url(attributes.get("poster"))
    video_url = hls or url or public_url(playback.get("fallbackUrl"))
    direct = packaged or (url if url != hls else "")

    if not video_url:
        return {}

    return {
        "url": video_url, "asset_type": "video",
        **({"hls_url": hls} if hls else {}),
        **({"playback_url": direct} if direct else {}),
        **({"playback_has_audio": True} if packaged else {}),
        **({"poster_url": poster} if poster else {}),
    }
