"""Pure interpretation of captured X text, links and media."""

from __future__ import annotations

import html
import math
import re
from collections.abc import Mapping
from typing import Any, Literal, NotRequired, TypedDict, cast
from urllib.parse import quote, urlsplit


class XLink(TypedDict):
    url: str
    title: str
    description: str
    image_url: str | None


class XMedia(TypedDict):
    type: Literal["image", "video", "gif"]
    url: str
    poster_url: str | None
    alt: str
    remote: bool
    aspect_ratio: NotRequired[float]
    source_url: NotRequired[str]



def is_x_reference(payload: Mapping[str, object]) -> bool:
    """Identify the observed ID-only Tweet shape, not other sparse posts."""
    return (
        payload.get("__typename") == "Tweet"
        and isinstance(payload.get("rest_id"), str)
        and bool(payload.get("rest_id"))
        and set(payload) == {"__typename", "rest_id"}
    )


def object_value(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _objects(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    return [object_value(item) for item in cast(list[object], value)]


def _string(value: object) -> str:
    return value if isinstance(value, str) else ""


def http_url(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or any(
            character.isspace() or ord(character) < 32 or character == "\\"
            for character in value
        )
    ):
        return None

    try:
        url = urlsplit(value)

        if (
            url.scheme in {"http", "https"}
            and url.hostname
            and url.username is None
            and url.password is None
        ):
            # Accessing port rejects malformed/non-numeric port values.
            _ = url.port

            return value
    except ValueError:
        pass

    return None


def media_url(value: object, *, video: bool = False) -> str | None:
    value = http_url(value)

    if not value:
        return None

    url = urlsplit(value)
    host = "video.twimg.com" if video else "pbs.twimg.com"

    if (
        url.scheme == "https"
        and url.hostname == host
        and url.port in {None, 443}
    ):
        return value

    return None


def _note(payload: dict[str, Any]) -> dict[str, Any]:
    return object_value(
        object_value(
            object_value(
                payload.get("note_tweet"),
            ).get("note_tweet_results"),
        ).get("result"),
    )


def post_text(payload: dict[str, Any]) -> str:
    text = _string(_note(payload).get("text"))

    if not text:
        text = _string(object_value(payload.get("legacy")).get("full_text"))

    # X payload text is entity-encoded plain text, never source HTML.
    return html.unescape(text).replace("\r\n", "\n").replace("\r", "\n")


def _url_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    legacy = object_value(payload.get("legacy"))
    note = _note(payload)

    return (
        _objects(object_value(note.get("entity_set")).get("urls"))
        + _objects(object_value(legacy.get("entities")).get("urls"))
        + _media_entities(payload)
    )


def _media_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    legacy = object_value(payload.get("legacy"))
    extended = _objects(
        object_value(legacy.get("extended_entities")).get("media"),
    )

    return extended or _objects(
        object_value(legacy.get("entities")).get("media"),
    )


def post_html(payload: dict[str, Any]) -> str:
    text = post_text(payload)
    urls: dict[str, str] = {}

    for entity in _url_entities(payload):
        short = http_url(entity.get("url"))
        expanded = http_url(entity.get("expanded_url"))

        if short and expanded:
            urls.setdefault(short, expanded)

    def link(match: re.Match[str]) -> str:
        token = match[0]

        if token.startswith(("http://", "https://")):
            target = token.rstrip(".,!;:")

            while target.endswith(")") and target.count(")") > target.count(
                "(",
            ):
                target = target[:-1]

            suffix = token[len(target) :]
            target = urls.get(target, target)

            return _anchor(target, target) + html.escape(suffix)

        if token.startswith("@"):
            return _anchor(f"https://x.com/{token[1:]}", token)

        return _anchor(f"https://x.com/hashtag/{quote(token[1:])}", token)

    # Split before escaping so literal markup remains visible and harmless.
    pattern = re.compile(
        r"https?://[^\s<>\"']+|(?<![\w@])@[A-Za-z0-9_]{1,15}(?!\w)"
        r"|(?<![\w&])#\w+",
    )
    parts: list[str] = []
    cursor = 0

    for match in pattern.finditer(text):
        parts.extend((html.escape(text[cursor : match.start()]), link(match)))
        cursor = match.end()

    parts.append(html.escape(text[cursor:]))
    body = "".join(parts).strip()

    return "".join(
        f"<p>{paragraph.replace(chr(10), '<br>')}</p>"
        for paragraph in re.split(r"\n\s*\n", body)
        if paragraph
    )


def _anchor(url: str, label: str) -> str:
    if not http_url(url):
        return html.escape(label)

    return f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>'


def _card(payload: dict[str, Any]) -> dict[str, Any]:
    values = object_value(object_value(payload.get("card")).get("legacy"))
    bindings = values.get("binding_values")

    if isinstance(bindings, dict):
        return cast(dict[str, Any], bindings)

    return {
        _string(item.get("key")): object_value(item.get("value"))
        for item in _objects(bindings)
    }


def _card_string(card: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = object_value(card.get(key)).get("string_value")

        if isinstance(value, str) and value.strip():
            return html.unescape(value.strip())

    return None


def post_links(
    payload: dict[str, Any],
    local_urls: dict[str, str],
) -> list[XLink]:
    card = _card(payload)
    target = http_url(
        _card_string(
            card, "card_url", "player_url", "expandedurl", "vanity_url",
        ),
    )
    urls = _objects(
        object_value(
            object_value(payload.get("legacy")).get("entities"),
        ).get("urls"),
    )
    urls = (
        _objects(object_value(_note(payload).get("entity_set")).get("urls"))
        + urls
    )
    links: dict[str, XLink] = {}

    for entity in urls:
        url = http_url(entity.get("expanded_url")) or http_url(
            entity.get("url"),
        )

        if url:
            links.setdefault(
                url,
                {
                    "url": url,
                    "title": "",
                    "description": "",
                    "image_url": None,
                },
            )

    # Cards often identify the same destination by its short URL.
    for entity in urls:
        if target == entity.get("url"):
            target = http_url(entity.get("expanded_url")) or target

    if not target and len(links) == 1 and card:
        target = next(iter(links))

    if target:
        image = _card_image(card)
        links[target] = {
            "url": target,
            "title": _card_string(card, "title") or "",
            "description": _card_string(card, "description") or "",
            "image_url": local_urls.get(image, image) if image else None,
        }

    return list(links.values())


def _card_image(card: dict[str, Any]) -> str | None:
    for key in (
        "player_image_large",
        "summary_photo_image_large",
        "thumbnail_image_large",
        "photo_image_full_size",
        "player_image",
        "summary_photo_image",
        "thumbnail_image",
        "photo_image",
    ):
        image = object_value(object_value(card.get(key)).get("image_value"))
        url = media_url(image.get("url"))

        if url:
            return url

    return None


def post_media(
    payload: dict[str, Any],
    local_urls: dict[str, str],
) -> list[XMedia]:
    media: list[XMedia] = []
    seen: set[str] = set()

    for item in _media_entities(payload):
        attachment = _attachment(item, local_urls)

        if attachment and attachment["url"] not in seen:
            seen.add(attachment["url"])
            media.append(attachment)

    return media


def _attachment(
    item: dict[str, Any],
    local_urls: dict[str, str],
) -> XMedia | None:
    image = media_url(item.get("media_url_https") or item.get("media_url"))
    kind = _string(item.get("type"))
    alt = _string(item.get("ext_alt_text"))

    if kind in {"video", "animated_gif"}:
        variants = _objects(
            object_value(item.get("video_info")).get("variants"),
        )
        playable = [
            variant
            for variant in variants
            if variant.get("content_type") == "video/mp4"
            and media_url(variant.get("url"), video=True)
        ]
        playable.sort(
            key=lambda variant: (
                variant["url"] in local_urls,
                _bitrate(variant.get("bitrate")),
            ),
            reverse=True,
        )

        if playable:
            url = str(playable[0]["url"])

            attachment: XMedia = {
                "type": "gif" if kind == "animated_gif" else "video",
                "url": local_urls.get(url, url),
                "poster_url": local_urls.get(image, image) if image else None,
                "alt": alt,
                "remote": url not in local_urls,
            }
            aspect_ratio = _video_aspect_ratio(item)
            source_url = _video_source_url(item.get("expanded_url"))

            if aspect_ratio is not None:
                attachment["aspect_ratio"] = aspect_ratio

            if source_url:
                attachment["source_url"] = source_url

            return attachment

    if image:
        return {
            "type": "image",
            "url": local_urls.get(image, image),
            "poster_url": None,
            "alt": alt,
            "remote": image not in local_urls,
        }

    return None


def _bitrate(value: object) -> int:
    return (
        value if isinstance(value, int) and not isinstance(value, bool) else 0
    )


def _video_aspect_ratio(item: dict[str, Any]) -> float | None:
    ratio = object_value(item.get("video_info")).get("aspect_ratio")
    original = object_value(item.get("original_info"))
    dimensions: list[object] = [
        ratio, [original.get("width"), original.get("height")],
    ]

    for pair in dimensions:
        if not isinstance(pair, list) or len(cast(list[object], pair)) != 2:
            continue

        width, height = cast(list[object], pair)

        if (
            not isinstance(width, (int, float))
            or isinstance(width, bool)
            or not isinstance(height, (int, float))
            or isinstance(height, bool)
            or width <= 0
            or height <= 0
        ):
            continue

        try:
            result = width / height
        except OverflowError:
            continue

        if math.isfinite(result) and result > 0:
            return result

    return None


def _video_source_url(value: object) -> str | None:
    source_url = http_url(value)

    if not source_url:
        return None

    parsed = urlsplit(source_url)

    if (
        parsed.hostname in {
            "x.com", "www.x.com", "twitter.com", "www.twitter.com",
            "mobile.twitter.com",
        }
        and parsed.port in {None, 80 if parsed.scheme == "http" else 443}
        and re.fullmatch(
            r"/[A-Za-z0-9_]{1,15}/status/[0-9]+/video/[1-9][0-9]*",
            parsed.path,
        )
    ):
        return source_url

    return None
