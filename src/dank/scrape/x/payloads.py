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
    timeline_results = _iter_timeline_tweet_results(payload)
    stack: list[object] = [payload]
    results: list[dict[str, object]] = list(timeline_results)

    while stack:
        current = stack.pop()
        current_dict = _as_dict(current)

        if current_dict is not None:
            results.extend(_extract_tweet_results(current_dict))

            if _looks_like_tweet(current_dict):
                results.append(current_dict)

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


def _iter_timeline_tweet_results(
    payload: Mapping[str, object],
) -> list[dict[str, object]]:
    data = _as_dict(payload.get("data"))

    if data is None:
        return []

    user = _as_dict(data.get("user"))

    if user is None:
        return []

    user_result = _as_dict(user.get("result"))

    if user_result is None:
        return []

    timeline = _as_dict(user_result.get("timeline"))

    if timeline is None:
        return []

    timeline_data = _as_dict(timeline.get("timeline"))

    if timeline_data is None:
        return []

    instructions = _as_list(timeline_data.get("instructions"))

    if instructions is None:
        return []

    extracted: list[dict[str, object]] = []

    for instruction in instructions:
        instruction_dict = _as_dict(instruction)

        if instruction_dict is None:
            continue

        if instruction_dict.get("type") != "TimelineAddEntries":
            continue

        entries = _as_list(instruction_dict.get("entries"))

        if entries is None:
            continue

        for entry in entries:
            entry_dict = _as_dict(entry)

            if entry_dict is None:
                continue

            extracted.extend(_extract_tweet_results_from_entry(entry_dict))

    return extracted


def _extract_tweet_results_from_entry(
    entry: dict[str, object],
) -> list[dict[str, object]]:
    content = _as_dict(entry.get("content"))

    if content is None:
        return []

    match content.get("entryType"):
        case "TimelineTimelineItem":
            return _extract_tweet_results_from_item_content(
                _as_dict(content.get("itemContent")),
            )
        case "TimelineTimelineModule":
            return _extract_tweet_results_from_module(content)
        case _:
            return []


def _extract_tweet_results_from_module(
    content: dict[str, object],
) -> list[dict[str, object]]:
    items = _as_list(content.get("items"))

    if items is None:
        return []

    extracted: list[dict[str, object]] = []

    for item in items:
        item_dict = _as_dict(item)

        if item_dict is None:
            continue

        item_wrapper = _as_dict(item_dict.get("item"))
        node = item_wrapper if item_wrapper is not None else item_dict
        extracted.extend(
            _extract_tweet_results_from_item_content(
                _as_dict(node.get("itemContent")),
            ),
        )

    return extracted


def _extract_tweet_results_from_item_content(
    item_content: dict[str, object] | None,
) -> list[dict[str, object]]:
    if item_content is None:
        return []

    item_type = item_content.get("itemType")

    if item_type not in {None, "TimelineTweet"}:
        return []

    return _extract_tweet_results(item_content)


def _extract_tweet_results(node: dict[str, object]) -> list[dict[str, object]]:
    extracted: list[dict[str, object]] = []

    for key in ("tweet_results", "tweetResult"):
        tweet_results = _as_dict(node.get(key))

        if tweet_results is None:
            continue

        result = _as_dict(tweet_results.get("result"))

        if result is None:
            continue

        unwrapped = _unwrap_tweet_result(result)
        extracted.append(unwrapped or result)

    return extracted


def _unwrap_tweet_result(
    result: dict[str, object],
) -> dict[str, object] | None:
    stack: list[dict[str, object]] = [result]

    while stack:
        current = stack.pop()

        if _looks_like_tweet(current):
            return current

        for key in ("tweet", "result"):
            nested = _as_dict(current.get(key))

            if nested is not None:
                stack.append(nested)

    return None


def _looks_like_tweet(node: dict[str, object]) -> bool:
    if node.get("__typename") == "Tweet":
        return True

    if _as_dict(node.get("note_tweet")) is not None:
        return True

    legacy = _as_dict(node.get("legacy"))

    if legacy is None:
        return False

    if isinstance(legacy.get("full_text"), str):
        return True

    if isinstance(legacy.get("conversation_id_str"), str):
        return True

    return False


def _parse_tweet_result(tweet: dict[str, object]) -> XExtractedPost | None:
    tweet = _unwrap_tweet_result(tweet) or tweet
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
