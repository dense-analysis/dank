from __future__ import annotations

import datetime
import html
import re
from collections.abc import Iterator, Mapping
from typing import cast
from urllib.parse import urljoin, urlsplit

POST_PATH = re.compile(r"^/r/([A-Za-z0-9_]+)/comments/([a-z0-9]+)(?:/|$)")


def as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def text(value: object) -> str:
    return value if isinstance(value, str) else ""


def number(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def timestamp(value: object) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)

    if isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value)
            return parsed.replace(
                tzinfo=parsed.tzinfo or datetime.UTC,
            ).timestamp()
        except (ValueError, OverflowError):
            pass

    return None


def public_url(value: object) -> str:
    url = html.unescape(text(value))
    try:
        parsed = urlsplit(url)
    except ValueError:
        return ""

    return url if parsed.scheme in {"http", "https"} and parsed.netloc else ""


def _promoted(value: Mapping[str, object]) -> bool:
    return any(
        value.get(key) not in (None, False, "", "false", 0)
        for key in ("promoted", "is_promoted", "isPromoted", "isAd", "ad")
    )


def _candidates(payload: object) -> Iterator[dict[str, object]]:
    if isinstance(payload, list):
        for item in cast(list[object], payload):
            yield from _candidates(item)
    elif isinstance(payload, dict):
        candidate = cast(dict[str, object], payload)

        if _promoted(candidate):
            return

        if candidate.get("kind") == "t3":
            yield as_dict(candidate.get("data"))
            return

        if candidate.get("title") and any(
            candidate.get(key) for key in ("id", "name")
        ):
            yield candidate
            return

        for key, child in candidate.items():
            if key not in {"crosspost_parent_list", "comments"}:
                yield from _candidates(child)


def extract_json_posts(
    payload: object,
    community: str,
) -> list[dict[str, object]]:
    posts: list[dict[str, object]] = []

    for candidate in _candidates(payload):
        post = normalize_post(candidate, community)

        if post is not None:
            posts.append(post)

    return posts


def normalize_post(
    candidate: dict[str, object],
    community: str,
) -> dict[str, object] | None:
    if _promoted(candidate):
        return None

    try:
        permalink = urljoin(
            "https://www.reddit.com/",
            text(candidate.get("permalink"))
            or text(candidate.get("permaLink")),
        )
        parsed = urlsplit(permalink)
    except ValueError:
        return None

    match = POST_PATH.match(parsed.path)
    title = text(candidate.get("title")).strip()

    if (
        parsed.hostname
        not in {"reddit.com", "www.reddit.com", "old.reddit.com"}
        or match is None
        or not title
        or match[1].casefold() != community.casefold()
    ):
        return None

    post_id = f"t3_{match[2]}"
    supplied_id = text(candidate.get("name")) or text(candidate.get("id"))

    if supplied_id and supplied_id.removeprefix("t3_") != match[2]:
        return None

    author = candidate.get("author")

    if isinstance(author, dict):
        author = cast(dict[str, object], author).get("name")

    permalink = f"https://www.reddit.com{parsed.path}"
    post = {
        **candidate,
        "id": post_id,
        "name": post_id,
        "subreddit": match[1],
        "title": title,
        "author": text(author),
        "permalink": permalink,
        "created_utc": timestamp(
            candidate.get("created_utc", candidate.get("createdAt")),
        ),
        "selftext": text(candidate.get("selftext")),
        "selftext_html": text(candidate.get("selftext_html")),
        "url": public_url(candidate.get("url_overridden_by_dest"))
        or public_url(candidate.get("url"))
        or permalink,
        "score": number(candidate.get("score")),
        "num_comments": number(candidate.get("num_comments")),
        "is_self": bool(candidate.get("is_self", False)),
        "over_18": bool(candidate.get("over_18", False)),
        "spoiler": bool(candidate.get("spoiler", False)),
        "raw": candidate.get("raw", candidate),
    }
    media = _media(candidate)
    parents = candidate.get("crosspost_parent_list")

    if isinstance(parents, list) and not any(
        item["asset_type"] == "video" for item in media
    ):
        # Crossposts own their discussion; media can belong to the parent.
        inherited = [
            item for parent in cast(list[object], parents)[:1]
            for item in _media(as_dict(parent))
            if item["asset_type"] == "video" or not media
        ]
        media = list({
            text(item["url"]): item for item in media + inherited
        }.values())

    post["media"] = media

    return post


def _media(candidate: dict[str, object]) -> list[dict[str, object]]:
    assets: dict[str, dict[str, object]] = {}
    supplied = candidate.get("media")

    if isinstance(supplied, list):
        for item in cast(list[object], supplied):
            asset = as_dict(item)
            url = public_url(asset.get("url"))
            kind = text(asset.get("asset_type"))

            if url and kind in {"photo", "video"}:
                assets[url] = {
                    **assets.get(url, {}), "url": url, "asset_type": kind,
                    **({"playback_has_audio": True}
                       if asset.get("playback_has_audio") is True else {}),
                    **{
                        key: value for key in (
                            "playback_url", "hls_url",
                            "poster_url", "audio_url",
                        ) if (value := public_url(asset.get(key)))
                    },
                }

    destination = public_url(
        candidate.get("url_overridden_by_dest", candidate.get("url")),
    )

    if urlsplit(destination).hostname == "i.redd.it":
        return [{"url": destination, "asset_type": "photo"}]

    preview = as_dict(candidate.get("preview")).get("images")
    poster = ""

    if isinstance(preview, list):
        for image in cast(list[object], preview):
            url = public_url(as_dict(as_dict(image).get("source")).get("url"))

            if url:
                poster = poster or url
                assets.setdefault(url, {"url": url, "asset_type": "photo"})

    assets.update({
        url: {"url": url, "asset_type": kind}
        for url, kind in _gallery_images(candidate).items()
    })
    video = (
        as_dict(as_dict(candidate.get("secure_media")).get("reddit_video"))
        or as_dict(as_dict(candidate.get("media")).get("reddit_video"))
        or as_dict(
            as_dict(candidate.get("preview")).get("reddit_video_preview"),
        )
    )
    hls = public_url(video.get("hls_url"))
    playback = public_url(video.get("fallback_url"))
    video_url = hls or playback

    if video_url:
        assets[video_url] = {
            "url": video_url, "asset_type": "video",
            **({"hls_url": hls} if hls else {}),
            **({"playback_url": playback} if playback else {}),
            **({"poster_url": poster} if poster else {}),
        }

    return _video_posters(list(assets.values()), poster)


def _video_posters(
    media: list[dict[str, object]], poster: str,
) -> list[dict[str, object]]:
    poster = poster or next((
        text(item["url"]) for item in media if item["asset_type"] == "photo"
    ), "")

    for item in media:
        if item["asset_type"] == "video" and poster:
            item.setdefault("poster_url", poster)

    return media


def _gallery_images(candidate: dict[str, object]) -> dict[str, str]:
    return {
        url: "photo"
        for value in as_dict(candidate.get("media_metadata")).values()
        if (url := public_url(as_dict(as_dict(value).get("s")).get("u")))
    }


def merge_candidates(
    candidates: list[tuple[dict[str, object], str]],
) -> list[tuple[dict[str, object], str]]:
    merged: dict[str, tuple[dict[str, object], str]] = {}

    for post, origin in candidates:
        post_id = text(post.get("id"))

        if post_id not in merged:
            merged[post_id] = (dict(post), origin)
            continue

        previous, _ = merged[post_id]

        for key in ("selftext", "selftext_html"):
            if len(text(post.get(key))) > len(text(previous.get(key))):
                previous[key] = post[key]

        media = {
            text(as_dict(item).get("url")): item
            for item in cast(list[object], previous.get("media", []))
            + cast(list[object], post.get("media", []))
        }
        previous["media"] = list(media.values())

        if previous.get("raw") != post.get("raw"):
            previous["raw"] = {
                "captures": [previous.get("raw"), post.get("raw")],
            }

        for key in (
            "author",
            "link_flair_text",
            "created_utc",
            "score",
            "num_comments",
        ):
            if not previous.get(key) and post.get(key):
                previous[key] = post[key]

        if previous.get("url") == previous.get("permalink"):
            previous["url"] = post.get("url", previous["url"])

    return list(merged.values())
