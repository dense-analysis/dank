"""Safe reader projections of archived Reddit discussions and video."""

from __future__ import annotations

import datetime as dt
import html
import math
import re
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import urljoin, urlsplit

from dank.html_utils import remove_page_noise
from dank.web.app import _sanitize_html  # pyright: ignore[reportPrivateUsage]

COMMENT_SORTS = {"best", "top", "new", "controversial", "old", "qa"}
# Match the collector's bounded snapshot, not arbitrary raw JSON.
MAX_COMMENTS = 500


def _mapping(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _count(value: object, minimum: int = 0) -> int | None:
    return value if (
        isinstance(value, int) and not isinstance(value, bool)
        and minimum <= value < 2 ** 53
    ) else None


def _url(value: object) -> str | None:
    value = html.unescape(_text(value)).strip()

    try:
        parsed = urlsplit(value)
        _ = parsed.port

        if (parsed.scheme in {"http", "https"} and parsed.hostname
                and not parsed.username and not parsed.password):
            return value
    except ValueError:
        pass

    return None


def comment_capture(
    snapshot: dict[str, Any], permalink: str,
) -> dict[str, Any]:
    capture = _mapping(snapshot.get("comment_capture"))
    status = _text(capture.get("status"))

    if status not in {"captured", "disabled", "failed", "deferred"}:
        status = "not_captured"

    total = _count(capture.get("total_available"))

    if total is None:
        total = _count(snapshot.get("num_comments"))

    raw = capture.get("comments")
    comments: list[dict[str, Any]] = []
    seen: set[str] = set()

    if status == "captured" and isinstance(raw, list):
        for item in cast(list[object], raw)[:MAX_COMMENTS]:
            comment = _comment(_mapping(item), permalink)

            if comment is not None and comment["id"] not in seen:
                seen.add(comment["id"])
                comments.append(comment)

    sort = _text(capture.get("sort"))

    result = {
        "status": status,
        "sort": sort if sort in COMMENT_SORTS else "best",
        "limit": min(_count(capture.get("limit")) or 50, MAX_COMMENTS),
        "total_available": total,
        "has_more": status == "captured" and (
            capture.get("has_more") is True
            or (total is not None and total > len(comments))
            or (isinstance(raw, list)
                and len(cast(list[object], raw)) > len(comments))
        ),
        "comments": comments,
    }
    previous_status = _text(capture.get("latest_attempt_status"))
    captured_at = _text(capture.get("captured_at"))

    if status == "captured" and captured_at:
        try:
            captured = dt.datetime.fromisoformat(captured_at)
        except ValueError:
            pass
        else:
            result["captured_at"] = captured.replace(
                tzinfo=captured.tzinfo or dt.UTC,
            ).isoformat()

            if previous_status in {"disabled", "failed", "deferred"}:
                result["latest_attempt_status"] = previous_status

    return result


def _comment(value: dict[str, Any], permalink: str) -> dict[str, Any] | None:
    comment_id = _text(value.get("id"))

    if not re.fullmatch(r"(?:t1_)?[a-z0-9]+", comment_id):
        return None

    body = _text(value.get("body"))
    body_html = _text(value.get("body_html")).strip()

    if body_html.startswith("&lt;"):
        body_html = html.unescape(body_html)

    if not body_html:
        body_html = f"<p>{html.escape(body).replace(chr(10), '<br>')}</p>"

    body_html = _sanitize_html(remove_page_noise(
        body_html, base_url=permalink,
    ))
    created = value.get("created_utc")

    if (isinstance(created, bool) or not isinstance(created, (int, float))
            or not math.isfinite(created) or not 0 <= created <= 253402300799):
        created = None

    comment_url = _url(urljoin(permalink, _text(value.get("permalink"))))

    if comment_url and urlsplit(comment_url).hostname not in {
        "reddit.com", "www.reddit.com", "old.reddit.com",
    }:
        comment_url = None

    return {
        "id": comment_id,
        "parent_id": _text(value.get("parent_id")),
        "author": _text(value.get("author")) or "[deleted]",
        "body": body,
        "body_html": body_html,
        "score": _count(value.get("score"), -(2 ** 53 - 1)),
        "created_utc": created,
        "permalink": comment_url or permalink,
        "is_op": value.get("is_op") is True,
        "depth": min(_count(value.get("depth")) or 0, 20),
    }


def images(
    snapshot: dict[str, Any], local_urls: dict[str, str],
) -> list[dict[str, str]]:
    media = snapshot.get("media")

    if not isinstance(media, list):
        return []

    result: dict[str, dict[str, str]] = {}

    for item in cast(list[object], media):
        asset = _mapping(item)
        source = _url(asset.get("url"))

        if source and asset.get("asset_type") == "photo":
            result[source] = {
                "url": _local_media(source, local_urls) or source,
                "source_url": source,
            }

    return list(result.values())


def videos(
    snapshot: dict[str, Any], local_urls: dict[str, str],
) -> list[dict[str, Any]]:
    media = snapshot.get("media")

    if not isinstance(media, list):
        return []

    merged: dict[str, dict[str, Any]] = {}

    for item in cast(list[object], media):
        asset = _mapping(item)
        url = _url(asset.get("url"))

        if not url or asset.get("asset_type") != "video":
            continue

        key = _video_identity(url)
        merged[key] = {**merged.get(key, {}), **asset, "url": url}

    if len(merged) == 1 and not snapshot.get("selftext_html"):
        photos = {
            _url(_mapping(item).get("url"))
            for item in cast(list[object], media)
            if _mapping(item).get("asset_type") == "photo"
        } - {None}

        if len(photos) == 1:
            # Older feed captures stored the video's preview separately.
            next(iter(merged.values())).setdefault("poster_url", photos.pop())

    return [
        _video(asset, asset["url"], local_urls) for asset in merged.values()
    ]


def _video_identity(url: str) -> str:
    parsed = urlsplit(url)
    parts = parsed.path.strip("/").split("/")

    return f"v.redd.it/{parts[0]}" if (
        parsed.hostname == "v.redd.it" and parts[0]
    ) else url


def _video(
    asset: dict[str, Any], url: str, local_urls: dict[str, str],
) -> dict[str, Any]:
    hls = _url(asset.get("hls_url"))

    if not hls and urlsplit(url).path.lower().endswith(".m3u8"):
        hls = url

    fallback = (
        _url(asset.get("playback_url")) or _url(asset.get("fallback_url"))
    )
    poster = _url(asset.get("poster_url"))
    local = _local_media(url, local_urls)

    return {
        "url": local or hls or fallback or url,
        "source_url": url,
        "poster_url": (_local_media(poster, local_urls) or poster)
        if poster else None,
        "remote": local is None,
        "hls_url": hls if not local else None,
        "fallback_url": fallback
        if not local and asset.get("playback_has_audio") is True else None,
    }


def _local_media(url: str, local_urls: dict[str, str]) -> str | None:
    if url in local_urls:
        return local_urls[url]

    parsed = urlsplit(url)

    if parsed.hostname not in {
        "v.redd.it", "preview.redd.it", "external-preview.redd.it",
    }:
        return None

    # Signed Reddit URLs rotate; the media path still identifies the same
    # asset within this post. Previously archived video remains usable.
    for original, local in local_urls.items():
        candidate = urlsplit(original)

        if (candidate.hostname, candidate.path) == (
            parsed.hostname, parsed.path,
        ) or (parsed.hostname == "v.redd.it"
              and _video_identity(original) == _video_identity(url)):
            return local

    return None


def without_video_posters(
    body: str, posters: set[str],
) -> str:
    parser = _VideoPosters(posters)
    parser.feed(body)

    for tag in parser.tags:
        body = body.replace(tag, "")

    return body


class _VideoPosters(HTMLParser):
    def __init__(self, posters: set[str]) -> None:
        super().__init__()
        self.posters = posters
        self.paths = {
            (parsed.hostname, parsed.path) for value in posters
            if (parsed := urlsplit(value)).hostname in {
                "preview.redd.it", "external-preview.redd.it", "i.redd.it",
            }
        }
        self.tags: set[str] = set()

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "img":
            return

        value = dict(attrs).get("src") or ""
        parsed = urlsplit(_url(value) or "")

        if (value in self.posters
                or (parsed.hostname, parsed.path) in self.paths):
            self.tags.add(self.get_starttag_text() or "")
