from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import NamedTuple, cast


class XAsset(NamedTuple):
    url: str
    asset_type: str
    should_download: bool


class XExtractedPost(NamedTuple):
    post_id: str
    author: str
    created_at: datetime.datetime | None
    url: str
    payload: dict[str, object]
    assets: tuple[XAsset, ...]


def _as_dict(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return cast(dict[str, object], value)

    return None


def _as_list(value: object) -> list[object] | None:
    if isinstance(value, list):
        return cast(list[object], value)

    return None


def extract_posts_from_payload(
    payload: Mapping[str, object],
) -> list[XExtractedPost]:
    extracted: dict[str, XExtractedPost] = {}

    for tweet in _iter_tweet_results(payload):
        post = _parse_tweet_result(tweet)

        if post is None:
            continue

        extracted[post.post_id] = post

    return list(extracted.values())


def _iter_tweet_results(
    payload: Mapping[str, object],
) -> list[dict[str, object]]:
    stack: list[object] = [payload]
    results: list[dict[str, object]] = []
    while stack:
        current = stack.pop()
        current_dict = _as_dict(current)
        if current_dict is not None:
            tweet_results = _as_dict(current_dict.get("tweet_results"))
            if tweet_results is not None:
                result = _as_dict(tweet_results.get("result"))
                if result is not None:
                    results.append(result)
            for value in current_dict.values():
                if isinstance(value, dict | list):
                    stack.append(cast(object, value))
            continue
        current_list = _as_list(current)
        if current_list is None:
            continue
        for value in current_list:
            if isinstance(value, dict | list):
                stack.append(cast(object, value))
    return results


def _parse_tweet_result(tweet: dict[str, object]) -> XExtractedPost | None:
    post_id = _get_post_id(tweet)
    if not post_id:
        return None
    author = _get_author_handle(tweet)
    created_at = _get_created_at(tweet)
    url = _build_post_url(author, post_id)
    assets = _extract_assets(tweet)
    return XExtractedPost(
        post_id=post_id,
        author=author,
        created_at=created_at,
        url=url,
        payload=tweet,
        assets=assets,
    )


def _get_post_id(tweet: dict[str, object]) -> str | None:
    post_id = tweet.get("rest_id")
    if isinstance(post_id, str) and post_id:
        return post_id
    legacy = _as_dict(tweet.get("legacy"))
    if legacy is not None:
        legacy_id = legacy.get("id_str")
        if isinstance(legacy_id, str) and legacy_id:
            return legacy_id
    return None


def _get_author_handle(tweet: dict[str, object]) -> str:
    core = _as_dict(tweet.get("core"))
    if core is None:
        return ""
    user_results = _as_dict(core.get("user_results"))
    if user_results is None:
        return ""
    result = _as_dict(user_results.get("result"))
    if result is None:
        return ""
    legacy = _as_dict(result.get("legacy"))
    if legacy is not None:
        handle = legacy.get("screen_name")
        if isinstance(handle, str) and handle:
            return handle
    user_core = _as_dict(result.get("core"))
    if user_core is not None:
        handle = user_core.get("screen_name")
        if isinstance(handle, str) and handle:
            return handle
    return ""


def _get_created_at(tweet: dict[str, object]) -> datetime.datetime | None:
    legacy = _as_dict(tweet.get("legacy"))
    if legacy is None:
        return None
    created_at = legacy.get("created_at")
    if not isinstance(created_at, str):
        return None
    try:
        return datetime.datetime.strptime(
            created_at,
            "%a %b %d %H:%M:%S %z %Y",
        )
    except ValueError:
        return None


def _build_post_url(author: str, post_id: str) -> str:
    if author:
        return f"https://x.com/{author}/status/{post_id}"
    return f"https://x.com/i/status/{post_id}"


def _extract_assets(tweet: dict[str, object]) -> tuple[XAsset, ...]:
    assets: list[XAsset] = []
    legacy = _as_dict(tweet.get("legacy"))
    if legacy is not None:
        entities = _as_dict(legacy.get("entities"))
        if entities is not None:
            assets.extend(_extract_links(entities))
            assets.extend(_extract_media(entities))
        extended_entities = _as_dict(legacy.get("extended_entities"))
        if extended_entities is not None:
            assets.extend(_extract_media(extended_entities))
    deduped: dict[str, XAsset] = {
        asset.url: asset for asset in assets if asset.url
    }
    return tuple(deduped.values())


def _extract_links(entities: dict[str, object]) -> list[XAsset]:
    urls = _as_list(entities.get("urls"))
    if urls is None:
        return []
    assets: list[XAsset] = []
    for item in urls:
        item_dict = _as_dict(item)
        if item_dict is None:
            continue
        expanded = item_dict.get("expanded_url")
        if isinstance(expanded, str) and expanded:
            assets.append(
                XAsset(url=expanded, asset_type="link", should_download=False),
            )
    return assets


def _extract_media(entities: dict[str, object]) -> list[XAsset]:
    media_items = _as_list(entities.get("media"))
    if media_items is None:
        return []
    assets: list[XAsset] = []
    for item in media_items:
        item_dict = _as_dict(item)
        if item_dict is None:
            continue
        media_url = item_dict.get("media_url_https")
        if not isinstance(media_url, str) or not media_url:
            media_url = item_dict.get("media_url")
        media_type = item_dict.get("type")
        if not isinstance(media_type, str) or not media_type:
            media_type = "media"
        if isinstance(media_url, str) and media_url:
            assets.append(
                XAsset(
                    url=media_url,
                    asset_type=str(media_type),
                    should_download=True,
                ),
            )
        if media_type in {"video", "animated_gif"}:
            video_info = _as_dict(item_dict.get("video_info"))
            if video_info is not None:
                assets.extend(_extract_video_variants(video_info))
    return assets


def _extract_video_variants(video_info: dict[str, object]) -> list[XAsset]:
    variants = _as_list(video_info.get("variants"))
    if variants is None:
        return []
    assets: list[XAsset] = []
    for variant in variants:
        variant_dict = _as_dict(variant)
        if variant_dict is None:
            continue
        url = variant_dict.get("url")
        content_type = variant_dict.get("content_type")
        if (
            isinstance(url, str)
            and url
            and isinstance(content_type, str)
            and "video" in content_type
        ):
            assets.append(
                XAsset(url=url, asset_type="video", should_download=True),
            )
    return assets
