"""One saved recommendation per identity, with every accepted observation."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from typing import Any

from dank.config import YouTubeSettings
from dank.model import RawPost
from dank.scrape.types import ScrapeBatch

from .payloads import as_dict, discover_assets


def merge_home_posts(
    browser: Sequence[RawPost], ytdlp: Sequence[RawPost],
    settings: YouTubeSettings,
) -> ScrapeBatch:
    grouped: dict[str, list[tuple[str, RawPost, dict[str, Any]]]] = {}

    for method, posts in (("browser", browser), ("yt_dlp", ytdlp)):
        for post in posts:
            payload = as_dict(json.loads(post.payload))
            grouped.setdefault(post.post_id, []).append(
                (method, post, payload),
            )

    merged: list[RawPost] = []
    ranks: Counter[str] = Counter()

    for position, observations in enumerate(grouped.values(), 1):
        # A Shorts renderer is stronger evidence than a generic video card.
        primary = next(
            (item for item in observations if item[2].get("kind") == "short"),
            observations[0],
        )
        _, post, original = primary
        payload = dict(original)

        for _, _, other in observations:
            for key in (
                "title", "text", "channel_id", "channel_name", "channel_url",
                "thumbnail_url", "duration_text", "views_text",
                "published_text", "likes_text",
            ):
                if not payload.get(key) and other.get(key):
                    payload[key] = other[key]

        captures = [{
            "method": method, "kind": raw["kind"],
            "request_url": item.request_url,
            "discovery": raw["discovery"], "renderer": raw["renderer"],
        } for method, item, raw in observations]
        discovery = dict(as_dict(payload.get("discovery")))
        shelf = str(discovery.get("shelf", ""))
        ranks[shelf] += 1
        discovery.update(position=position, rank=ranks[shelf])
        payload.update(
            discovery=discovery, recommendation_captures=captures,
            collection_methods=list(dict.fromkeys(
                method for method, _, _ in observations
            )),
        )
        merged.append(post._replace(payload=json.dumps(
            payload, separators=(",", ":"),
        )))

    return ScrapeBatch(
        merged,
        [asset for post in merged
         for asset in discover_assets(post, settings)],
    )
