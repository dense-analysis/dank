"""Resolve reader context into collection targets already known to DANK."""

from __future__ import annotations

import re
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

from dank.config import (
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
)
from dank.scrape.youtube.feeds import configured_feeds
from dank.scrape.youtube.payloads import YouTubeFeed
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_accounts import source_kind
from dank.youtube import VIDEO_ID

LABELS = {
    "all": "All sources", "rss": "Blogs", "reddit": "Reddit",
    "x": "X", "hacker_news": "Hacker News", "youtube": "YouTube",
}
DETAIL_FIELDS: dict[str, set[str]] = {
    "all": set(), "rss": set(), "reddit": {"community"},
    "x": {"account"}, "hacker_news": set(),
    "youtube": {"channel_id", "feed_id"}, "post": {"domain", "post_id"},
}


class RefreshPlan(NamedTuple):
    scope: dict[str, Any]
    label: str
    settings: Settings
    target_post: tuple[str, str] | None = None


async def resolve_scope(
    settings: Settings, client: ClickHouseClient, raw: object,
) -> RefreshPlan:
    scope = _parse_scope(raw)
    kind = str(scope["kind"])

    if kind == "post":
        return await _post_plan(settings, client, scope)

    sources = _selected_sources(settings, scope)
    label = LABELS[kind]

    if kind == "all" and (scope.get("domains") or scope.get("tags")):
        label = "Selected sources"

    if community := scope.get("community"):
        sources = _community_sources(sources, community)
        label = f"r/{community}"
    elif account := scope.get("account"):
        sources = _account_sources(sources, account)
        label = f"@{account}"

    if not sources:
        raise ValueError("No configured sources match this refresh context")

    selected = settings._replace(sources=sources)

    if kind == "youtube" and (scope.get("channel_id") or scope.get("feed_id")):
        selected, label = await _youtube_settings(selected, client, scope)
    elif scope.get("domains") and len(scope["domains"]) == 1:
        label = label if kind != "rss" else scope["domains"][0]

    return RefreshPlan(scope, label, selected)


def _parse_scope(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("Refresh scope must be an object")

    value = cast(dict[str, object], raw)
    kind = value.get("kind")

    if not isinstance(kind, str) or kind not in DETAIL_FIELDS:
        raise ValueError("Unknown refresh scope kind")

    allowed = {"kind"} | DETAIL_FIELDS[kind]

    if kind != "post":
        allowed.update({"domains", "tags"})

    if set(value) - allowed:
        raise ValueError("Refresh scope has fields that do not match its kind")

    scope: dict[str, Any] = {"kind": kind}

    for key in ("domains", "tags"):
        if key in value:
            values = value[key]

            if not isinstance(values, list):
                raise ValueError(f"Refresh {key} must be a list")

            entries = [_text(item, key).lower()
                       for item in cast(list[object], values)]

            if entries:
                scope[key] = sorted(set(entries))

    for key in DETAIL_FIELDS[kind].intersection(value):
        scope[key] = _text(value[key], key)

    _normalize_details(scope)

    return scope


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ValueError(f"Refresh {field} must be a non-empty string")

    return value.strip()


def _normalize_details(scope: dict[str, Any]) -> None:
    if scope["kind"] == "post":
        if "domain" not in scope or "post_id" not in scope:
            raise ValueError("Post refresh needs domain and post_id")

        scope["domain"] = scope["domain"].lower()

    if "community" in scope:
        scope["community"] = scope["community"].lower().removeprefix("r/")

        if not re.fullmatch(r"[a-z0-9_]{2,21}", scope["community"]):
            raise ValueError("Invalid Reddit community")

    if "account" in scope:
        scope["account"] = scope["account"].lower().removeprefix("@")

        if not re.fullmatch(r"[a-z0-9_]{1,15}", scope["account"]):
            raise ValueError("Invalid X account")

    if "channel_id" in scope and "feed_id" in scope:
        raise ValueError("Choose a YouTube channel or a feed, not both")


def _selected_sources(
    settings: Settings, scope: dict[str, Any],
) -> tuple[SourceConfig, ...]:
    domains = set(cast(list[str], scope.get("domains", [])))
    tags = set(cast(list[str], scope.get("tags", [])))

    if domains - {source.domain for source in settings.sources}:
        raise ValueError(
            "Refresh includes a source that is no longer configured",
        )

    if tags - {tag for source in settings.sources for tag in source.tags}:
        raise ValueError("Refresh includes a tag that is no longer configured")

    selected: list[SourceConfig] = []

    for source in settings.sources:
        if domains and source.domain not in domains:
            continue

        if tags and not tags.intersection(source.tags):
            continue

        # Reader sections identify platforms by domain. A Hacker News source
        # still belongs to Hacker News when it uses RSS or several adapters.
        if (scope["kind"] == "all"
                or source_kind(source.domain) == scope["kind"]):
            selected.append(source)

    return tuple(selected)


def _community_sources(
    sources: tuple[SourceConfig, ...], community: str,
) -> tuple[SourceConfig, ...]:
    selected: list[SourceConfig] = []

    for source in sources:
        urls = tuple(
            url for url in source.feed_urls
            if urlsplit(url).path.strip("/").lower() == f"r/{community}"
        )

        if urls:
            selected.append(source._replace(
                feed_urls=urls,
                adapters=(SourceAdapterConfig(SourceAdapter.REDDIT, urls),),
            ))

    return tuple(selected)


def _account_sources(
    sources: tuple[SourceConfig, ...], account: str,
) -> tuple[SourceConfig, ...]:
    selected: list[SourceConfig] = []

    for source in sources:
        accounts = tuple(
            value for value in source.accounts
            if value.strip().removeprefix("@").lower() == account
        )

        if accounts:
            selected.append(source._replace(accounts=accounts))

    return tuple(selected)


async def _youtube_settings(
    settings: Settings, client: ClickHouseClient, scope: dict[str, Any],
) -> tuple[Settings, str]:
    feeds = {
        feed.url: feed for source in settings.sources
        for feed in configured_feeds(settings.youtube, source)
    }

    if feed_id := scope.get("feed_id"):
        selected = [feeds[feed_id]] if feed_id in feeds else []
    else:
        selected = await _channel_feeds(client, feeds, scope["channel_id"])

    if not selected:
        raise ValueError("This YouTube channel or feed is not configured")

    urls = tuple(feed.url for feed in selected)
    source = settings.sources[0]._replace(
        accounts=(), feed_urls=urls,
        adapters=(SourceAdapterConfig(SourceAdapter.YOUTUBE, urls),),
    )
    youtube = settings.youtube._replace(
        news_tabs=(), hype=False, hype_playlists=(),
        homepage=any(feed.kind == "home" for feed in selected),
    )
    regional = [feed for feed in selected
                if feed.kind == "hype" and feed.region]

    if regional:
        # Keep the selected playlist's regional identity without adding the
        # global Hype feed or any other configured country's playlist.
        youtube = youtube._replace(
            region=regional[0].region, hype=True,
            hype_playlists=tuple((feed.region, feed.url) for feed in regional),
        )

    return (settings._replace(sources=(source,), youtube=youtube),
            selected[0].name)


async def _channel_feeds(
    client: ClickHouseClient, feeds: dict[str, YouTubeFeed], channel_id: str,
) -> list[YouTubeFeed]:
    result = await client.fetch_json(
        "SELECT feed_url, channel_id, channel_url FROM youtube_channels FINAL "
        "UNION ALL SELECT feed_url, "
        "argMax(JSONExtractString(payload, 'channel_id'), observed_at) "
        "AS channel_id, "
        "argMax(JSONExtractString(payload, 'channel_url'), observed_at) "
        "AS channel_url FROM youtube_discoveries "
        "WHERE feed_kind = 'channel' GROUP BY feed_url",
    )
    configured = {_channel_url(feed.url) for feed in feeds.values()
                  if feed.kind == "channel"}
    matches = {_channel_url(channel_id)} & configured

    for row in result.rows:
        if channel_id in {str(row.get("channel_id")),
                          str(row.get("channel_url"))}:
            # Captured feeds can outlive configuration. Only their identity
            # mapping is useful here; collection must remain configured.
            matches.add(_channel_url(str(row["feed_url"])))

    return [feed for feed in feeds.values()
            if feed.kind == "channel" and _channel_url(feed.url) in matches]


def _channel_url(value: str) -> str:
    if not value.startswith(("http://", "https://")):
        path = "/channel/" + value if value.startswith("UC") else (
            "/@" + value.removeprefix("@")
        )
    else:
        parsed = urlsplit(value)

        if parsed.netloc.lower() not in {"youtube.com", "www.youtube.com"}:
            return ""

        path = parsed.path.rstrip("/")

        for suffix in ("/videos", "/shorts", "/posts", "/streams",
                       "/featured", "/community"):
            path = path.removesuffix(suffix)

    return "https://www.youtube.com" + path


async def _post_plan(
    settings: Settings, client: ClickHouseClient, scope: dict[str, Any],
) -> RefreshPlan:
    domain, post_id = str(scope["domain"]), str(scope["post_id"])

    if domain == "youtube.com":
        video_id = post_id.removeprefix("video:")

        if (not post_id.startswith("video:")
                or not VIDEO_ID.fullmatch(video_id)):
            raise ValueError("Only individual YouTube videos can be refreshed")

        result = await client.fetch_json(
            "SELECT video_id FROM (SELECT video_id "
            "FROM youtube_reference_evidence WHERE video_id = %(id)s "
            "UNION ALL SELECT substring(item_id, 7) AS video_id "
            "FROM youtube_discoveries WHERE item_id = %(post_id)s "
            "UNION ALL SELECT substring(post_id, 7) AS video_id "
            "FROM raw_posts WHERE domain = %(domain)s "
            "AND post_id = %(post_id)s) LIMIT 1",
            {"id": video_id, "domain": domain, "post_id": post_id},
        )
        label = "YouTube video"
    else:
        result = await client.fetch_json(
            "SELECT domain, post_id FROM raw_posts "
            "WHERE domain = %(domain)s AND post_id = %(post_id)s LIMIT 1",
            {"domain": domain, "post_id": post_id},
        )
        label = "Post"

    if not result.rows:
        raise ValueError("This post has no saved collection target")

    return RefreshPlan(scope, label, settings, (domain, post_id))
