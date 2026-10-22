"""Configured YouTube feeds, enriched with their collected identities."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from dank.config import Settings

YOUTUBE_ORIGIN = "https://www.youtube.com"
NEWS_PATH = "/feed/news_destination"
NEWS_LABELS = {
    "top_stories": "Top stories", "business": "Business", "world": "World",
    "sports": "Sports", "entertainment": "Entertainment",
}


def configured_sources(
    settings: Settings, captured: list[dict[str, Any]],
    metadata: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    feeds = _configured_feeds(settings)

    for row in captured:
        url = str(row["feed_url"])
        feeds.setdefault(url, _feed(
            url, str(row["name"]), str(row["kind"]), str(row["region"]),
            str(row.get("tab") or ""),
        ))

    observations = {str(row["feed_url"]): row for row in captured}
    channels: list[dict[str, Any]] = []
    identities = {
        key: row for row in metadata or []
        for key in (str(row["channel_id"]), _channel_url(str(row["feed_url"])))
    }

    for url, feed in feeds.items():
        observation = observations.get(url, {})

        for key in ("name", "kind", "region", "tab", "count"):
            if observation.get(key):
                feed[key] = observation[key]

        if feed["kind"] == "home":
            feed["name"] = "Recommendations"

        if feed["kind"] == "channel":
            channel_url = _channel_url(str(
                observation.get("channel_url") or url,
            ))
            identity = identities.get(
                str(observation.get("channel_id")),
                identities.get(_channel_url(url), {}),
            )
            channels.append({
                "id": str(identity.get("channel_id")
                          or observation.get("channel_id") or channel_url),
                "name": str(identity.get("channel_name")
                            or observation.get("channel_name")
                            or feed["name"]),
                "url": channel_url, "count": int(feed["count"]),
                "avatar_url": identity.get("avatar_url"),
                "reported_video_count": identity.get("reported_video_count"),
                "reported_video_count_text": (
                    identity.get("reported_video_count_text") or None
                ),
            })

    # Accounts and explicit feed URLs can name the same channel twice.
    channels = list({str(c["id"]): c for c in channels}.values())

    return list(feeds.values()), channels


def _configured_feeds(settings: Settings) -> dict[str, dict[str, Any]]:
    youtube = getattr(settings, "youtube", None)
    region = str(getattr(youtube, "region", "GB"))
    feeds: dict[str, dict[str, Any]] = {}

    playlists: dict[str, str] = dict(
        getattr(youtube, "hype_playlists", ()),
    )

    if getattr(youtube, "hype", True) and region not in playlists:
        feeds[YOUTUBE_ORIGIN + "/feed/hype"] = _feed(
            YOUTUBE_ORIGIN + "/feed/hype", "Hype · Top 100", "hype", region,
        )

    tabs = getattr(youtube, "news_tabs", ("top_stories", "business", "world"))

    for tab in tabs:
        path = NEWS_PATH + ("" if tab == "top_stories" else "/" + tab)
        url = YOUTUBE_ORIGIN + path
        feeds[url] = _feed(url, "News · " + NEWS_LABELS.get(tab, tab),
                           "news", region, tab)

    if getattr(youtube, "homepage", False):
        url = YOUTUBE_ORIGIN + "/"
        feeds[url] = _feed(url, "Recommendations", "home", region)

    for source in settings.sources:
        if source.domain != "youtube.com":
            continue

        for account in source.accounts:
            url = account if account.startswith(("https://", "http://")) else (
                _channel_url(account) + "/videos"
            )
            feed = _configured_feed(url, region)
            feeds[str(feed["url"])] = feed

        for url in source.feed_urls:
            feed = _configured_feed(url, region)
            feeds[str(feed["url"])] = feed

    if getattr(youtube, "hype", True):
        for country, url in playlists.items():
            feeds[url] = _feed(url, "Hype · " + country, "hype", country)

    return feeds


def _channel_url(value: str) -> str:
    if not value.startswith(("https://", "http://")):
        path = "/channel/" + value if value.startswith("UC") else (
            "/@" + value.removeprefix("@")
        )
    else:
        path = urlsplit(value).path.rstrip("/")

        for suffix in (
            "/videos", "/shorts", "/posts", "/streams",
            "/featured", "/community",
        ):
            path = path.removesuffix(suffix)

    return YOUTUBE_ORIGIN + path


def _configured_feed(url: str, region: str) -> dict[str, Any]:
    path = urlsplit(url).path

    if path.startswith("/channel/UCYfdidRxbB8Qhf0Nx7ioOYw"):
        return _feed(YOUTUBE_ORIGIN + NEWS_PATH, "News · Top stories",
                     "news", region, "top_stories")

    if path.startswith(("/@", "/channel/", "/c/", "/user/")):
        name = path.split("/")[1 if path.startswith("/@") else 2]

        return _feed(url, name, "channel", region)

    if path.startswith(NEWS_PATH):
        tab = path.removeprefix(NEWS_PATH).strip("/") or "top_stories"

        return _feed(url, "News · " + NEWS_LABELS.get(tab, tab),
                     "news", region, tab)

    if not path.rstrip("/"):
        return _feed(YOUTUBE_ORIGIN + "/", "Recommendations",
                     "home", region)

    kind = "hype" if path == "/feed/hype" else "playlist"

    return _feed(url, "Hype · Top 100" if kind == "hype" else "Playlist",
                 kind, region)


def _feed(
    url: str, name: str, kind: str, region: str, tab: str = "",
) -> dict[str, Any]:
    return {"id": url, "url": url, "name": name, "kind": kind,
            "region": region, "tab": tab, "count": 0}
