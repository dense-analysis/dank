"""Reuse bounded, successful detail snapshots without refreshing their age."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import NamedTuple, cast
from urllib.parse import urlsplit

from dank.config import RedditSettings
from dank.model import RawPost

from .html import extract_html_posts
from .payloads import as_dict, extract_json_posts, merge_candidates, text


class DetailSnapshot(NamedTuple):
    payload: dict[str, object]
    scraped_at: dt.datetime


def listing_signature(post: dict[str, object]) -> str:
    # Votes/rank fluctuate without changing a post. Discussion counts do
    # invalidate the archive; edits invisible in listings expire with the TTL.
    fields = {
        key: post.get(key) for key in (
            "id", "title", "author", "permalink", "url", "is_self",
            "selftext", "selftext_html", "num_comments", "edited",
        )
    }
    media: list[dict[str, object]] = []

    for value in cast(list[object], post.get("media", [])):
        item = as_dict(value)
        url = text(item.get("url"))
        parsed = urlsplit(url)

        if parsed.hostname in {"v.redd.it", "i.redd.it", "preview.redd.it"}:
            url = parsed._replace(query="", fragment="").geturl()

        media.append({"url": url, "asset_type": item.get("asset_type")})

    fields["media"] = media

    return hashlib.sha256(json.dumps(
        fields, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def _original_signature(post: dict[str, object]) -> str | None:
    signature = text(
        as_dict(post.get("detail_capture")).get("listing_signature"),
    )

    if signature:
        return signature

    # Existing archives already retain the listing that led to the visit.
    original = as_dict(as_dict(post.get("raw")).get("listing"))
    community = text(post.get("subreddit"))
    pending = [original]
    candidates: list[tuple[dict[str, object], str]] = []

    while pending:
        node = pending.pop()
        captures = node.get("captures")

        if isinstance(captures, list):
            pending.extend(
                as_dict(item)
                for item in reversed(cast(list[object], captures))
            )
            continue

        extracted = (
            extract_html_posts(text(node["html"]), community)
            if node.get("html") else extract_json_posts(node, community)
        )
        candidates.extend((item, "") for item in extracted)

    return next((
        listing_signature(candidate)
        for candidate, _ in merge_candidates(candidates)
        if candidate.get("id") == post.get("id")
    ), None)


def reuse_detail(
    raw: RawPost, cached: DetailSnapshot, settings: RedditSettings,
) -> RawPost | None:
    previous = cached.payload
    detail = as_dict(previous.get("detail_capture"))
    listing = as_dict(json.loads(raw.payload))

    if (
        not settings.detail_cache_hours or detail.get("status") != "captured"
        or previous.get("id") != raw.post_id
        or text(previous.get("subreddit")).casefold()
        != text(listing.get("subreddit")).casefold()
        or _original_signature(previous) != listing_signature(listing)
    ):
        return None

    try:
        captured = (
            dt.datetime.fromisoformat(text(detail["captured_at"]))
            if "captured_at" in detail else cached.scraped_at
        )
        captured = captured.replace(tzinfo=captured.tzinfo or dt.UTC)
    except ValueError:
        return None

    age = (raw.scraped_at - captured).total_seconds()

    if not 0 <= age < settings.detail_cache_hours * 3600:
        return None

    comments = as_dict(previous.get("comment_capture"))

    if settings.capture_comments and (
        comments.get("status") != "captured"
        or comments.get("sort") != settings.comment_sort
        or comments.get("limit") != settings.comment_limit
    ):
        return None

    post = dict(listing)

    for field in ("selftext", "selftext_html", "media"):
        if field in previous:
            post[field] = previous[field]

    post["detail_capture"] = {
        **detail, "captured_at": captured.isoformat(), "reused": True,
        "listing_signature": listing_signature(listing),
    }
    post["comment_capture"] = {
        **comments,
        "captured_at": comments.get("captured_at") or captured.isoformat(),
        "reused": True,
    } if settings.capture_comments else {
        "status": "disabled", "comments": [], "sort": settings.comment_sort,
        "limit": settings.comment_limit,
        "total_available": listing.get("num_comments"),
    }
    post["raw"] = {
        "listing": listing.get("raw"),
        "detail": as_dict(previous.get("raw")).get("detail"),
    }

    return raw._replace(payload=json.dumps(post, separators=(",", ":")))
