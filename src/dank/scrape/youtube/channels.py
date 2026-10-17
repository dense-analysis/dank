"""Channel identity comes from page metadata and the channel header only."""

from __future__ import annotations

import datetime
import json
import re
from decimal import Decimal
from urllib.parse import urlsplit

from dank.model import YouTubeChannelDiscovery

from .payloads import ORIGIN, as_dict, objects, text

CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}")
VIDEO_COUNT = re.compile(r"([\d,]+(?:\.\d+)?)\s*([KMB]?)\s+videos?", re.I)


def channel_icon_url(value: str) -> bool:
    try:
        url = urlsplit(value)

        return (
            url.scheme == "https"
            and url.hostname in {"yt3.ggpht.com", "yt3.googleusercontent.com"}
            and url.username is None and url.password is None
            and url.port in {None, 443} and not url.fragment
        )
    except ValueError:
        return False


def discover_channel(
    initial: object, *, feed_url: str, request_url: str,
    observed_at: datetime.datetime,
) -> YouTubeChannelDiscovery | None:
    data = as_dict(initial)
    metadata = as_dict(as_dict(data.get("metadata")).get(
        "channelMetadataRenderer",
    ))
    channel_id = str(metadata.get("externalId") or "")

    if not CHANNEL_ID.fullmatch(channel_id):
        return None

    header = as_dict(data.get("header"))
    avatar = as_dict(metadata.get("avatar"))
    icons = [str(node.get("url") or "") for node in objects(avatar)]
    # Modern headers nest the avatar in a decoratedAvatarViewModel.
    for node in objects(header):
        decorated = node.get("decoratedAvatarViewModel")

        if decorated:
            icons.extend(str(part.get("url") or "")
                         for part in objects(decorated))

    icon = next((url for url in reversed(icons) if channel_icon_url(url)), "")
    count_text = ""

    for node in objects(header):
        value = text(node)
        match = VIDEO_COUNT.fullmatch(value.strip())

        if match:
            count_text = value.strip()
            break

    match = VIDEO_COUNT.fullmatch(count_text)
    total = None

    if match:
        multiplier = {"": 1, "K": 1000, "M": 1000000, "B": 1000000000}[
            match[2].upper()
        ]
        total = int(Decimal(match[1].replace(",", "")) * multiplier)

    return YouTubeChannelDiscovery(
        channel_id=channel_id, channel_name=text(metadata.get("title")),
        channel_url=ORIGIN + "/channel/" + channel_id,
        feed_url=feed_url, icon_url=icon,
        reported_video_count=total, reported_video_count_text=count_text,
        observed_at=observed_at, request_url=request_url,
        payload=json.dumps({"metadata": metadata, "header": header},
                           separators=(",", ":")),
    )
