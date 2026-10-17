from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from dank.config import SourceConfig, YouTubeSettings

from .payloads import ORIGIN, YouTubeFeed

NEWS_PATH = "/feed/news_destination"
NEWS_CHANNEL = "/channel/UCYfdidRxbB8Qhf0Nx7ioOYw"


def describe_feed(url: str) -> YouTubeFeed:
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/")

    if path.startswith(NEWS_PATH) or path.startswith(NEWS_CHANNEL):
        tab = path.removeprefix(NEWS_PATH).strip("/") or "top_stories"

        if path.startswith(NEWS_CHANNEL):
            tab = "top_stories"

        url = ORIGIN + NEWS_PATH + ("/" + tab if tab != "top_stories" else "")

        return YouTubeFeed(
            url, "News · " + tab.replace("_", " ").title(), "news", tab,
        )

    if path == "/feed/hype":
        return YouTubeFeed(ORIGIN + path, "Hype · Top 100", "hype")

    if path == "/playlist":
        identifier = parse_qs(parsed.query).get("list", [""])[0]

        if not identifier:
            raise ValueError(
                "YouTube playlist feed requires a list identifier",
            )

        return YouTubeFeed(url, "Playlist", "playlist")

    if not path:
        return YouTubeFeed(ORIGIN + "/", "Home recommendations", "home")

    parts = path.split("/")
    channel = parts[1] if parts[1].startswith("@") else parts[2]
    tab = (
        parts[-1]
        if parts[-1] in {"videos", "shorts", "posts", "community"}
        else "videos"
    )

    if tab == "videos" and not path.endswith("/videos"):
        url = ORIGIN + path + "/videos"

    return YouTubeFeed(url, channel, "channel", tab)


def configured_feeds(
    settings: YouTubeSettings, source: SourceConfig,
) -> tuple[YouTubeFeed, ...]:
    urls: list[str] = []

    if settings.hype:
        urls.append(ORIGIN + "/feed/hype")

    for tab in settings.news_tabs:
        urls.append(
            ORIGIN + NEWS_PATH + ("/" + tab if tab != "top_stories" else ""),
        )

    if settings.homepage:
        urls.append(ORIGIN + "/")

    urls.extend(source.feed_urls)

    for account in source.accounts:
        if account.startswith(("http://", "https://")):
            urls.append(account)
        elif account.startswith("UC"):
            urls.append(ORIGIN + "/channel/" + account + "/videos")
        else:
            urls.append(ORIGIN + "/@" + account.lstrip("@") + "/videos")

    feeds = {feed.url: feed for feed in map(describe_feed, urls)}

    if any(feed.kind == "home" for feed in feeds.values()) and not (
        settings.authenticated and settings.homepage
    ):
        raise ValueError(
            "YouTube home requires authenticated=true and homepage=true",
        )

    return tuple(feeds.values())
