"""Refresh rules: what a scope may select, and which jobs history keeps.

A scope stays the JSON object the reader sent, once normalized. Absent keys
mean "not narrowed", and it is saved and answered as is, so it is a dict rather
than a NamedTuple with empty values standing in for absent ones.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

from dank.api.sources.model import source_kind
from dank.config import (
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
)
from dank.errors import InvalidInput
from dank.scrape.concurrency import collection_resources
from dank.scrape.youtube.feeds import configured_feeds
from dank.scrape.youtube.payloads import YouTubeFeed
from dank.youtube import VIDEO_ID

ACTIVE_STATUSES = frozenset({"queued", "running", "processing"})
STATUSES = ACTIVE_STATUSES | {"completed", "partial", "failed", "interrupted"}
MAX_PENDING_JOBS = 5
# Each worker owns an embedding model and may open browsers.
MAX_CONCURRENT_JOBS = 3
MAX_HISTORY_JOBS = 20
RESTART_MESSAGE = (
    "Refresh was interrupted when DANK restarted. You can start it again."
)

LABELS = {
    "all": "All sources", "rss": "Blogs", "reddit": "Reddit",
    "x": "X", "hacker_news": "Hacker News", "youtube": "YouTube",
}
DETAIL_FIELDS: dict[str, set[str]] = {
    "all": set(), "rss": {"blog_feed"}, "reddit": {"community"},
    "x": {"account", "feed_id"}, "hacker_news": set(),
    "youtube": {"channel_id", "feed_id", "playlist_id"},
    "post": {"domain", "post_id"},
}
PLAYLIST_ID = re.compile(r"[A-Za-z0-9_-]{1,200}")

type Scope = dict[str, Any]


class RefreshPlan(NamedTuple):
    scope: Scope
    label: str
    settings: Settings
    target_post: tuple[str, str] | None = None


class RefreshJob(NamedTuple):
    id: str
    scope: Scope
    label: str
    status: str
    message: str
    created_at: str
    finished_at: str | None
    posts_saved: int
    run_id: str | None


class ChannelFeed(NamedTuple):
    """A saved link between a feed URL and the channel it belongs to."""

    feed_url: str
    channel_id: str
    channel_url: str


def full_refresh(plan: RefreshPlan) -> bool:
    return plan.scope["kind"] == "all" and not any(
        plan.scope.get(key) for key in ("domains", "tags", "platforms")
    )


def plans_overlap(left: RefreshPlan, right: RefreshPlan) -> bool:
    if full_refresh(left) or full_refresh(right):
        return True

    return bool(
        collection_resources(left.settings, left.target_post)
        & collection_resources(right.settings, right.target_post),
    )


def retained_jobs(jobs: Iterable[RefreshJob]) -> tuple[RefreshJob, ...]:
    """The newest finished jobs, without ever dropping an active one."""
    jobs = tuple(jobs)
    active = {job.id for job in jobs if job.status in ACTIVE_STATUSES}
    finished = [job.id for job in jobs if job.id not in active]
    retain = active | set(finished[-(MAX_HISTORY_JOBS - len(active)):])

    return tuple(job for job in jobs if job.id in retain)


def restored_jobs(saved: object, now: str) -> tuple[RefreshJob, ...]:
    """Saved history after a restart, which replays nothing."""
    if not isinstance(saved, list):
        raise ValueError("Invalid refresh history")

    items = cast(list[object], saved)[-MAX_HISTORY_JOBS:]
    jobs = (job for job in map(saved_job, items) if job is not None)

    return tuple(
        job._replace(
            status="interrupted", finished_at=now, message=RESTART_MESSAGE,
        ) if job.status in ACTIVE_STATUSES else job
        for job in jobs
    )


def saved_job(item: object) -> RefreshJob | None:
    # A file from another version may hold shapes the API can't describe, and
    # one such job must not take the whole list down.
    if not isinstance(item, dict):
        return None

    match cast(dict[str, object], item):
        case {
            "id": str() as job_id,
            "scope": raw_scope,
            "label": str() as label,
            "status": str() as status,
            "message": str() as message,
            "created_at": str() as created_at,
            "finished_at": (str() | None) as finished_at,
            "posts_saved": int() as posts_saved,
            "run_id": (str() | None) as run_id,
        } if (
            status in STATUSES
            and type(posts_saved) is int
            and posts_saved >= 0
            and (scope := _saved_scope(raw_scope)) is not None
        ):
            return RefreshJob(
                job_id, scope, label, status, message, created_at,
                finished_at, posts_saved, run_id,
            )
        case _:
            return None


def _saved_scope(raw: object) -> Scope | None:
    try:
        return parse_scope(raw)
    except InvalidInput:
        return None


def parse_scope(raw: object) -> Scope:
    if not isinstance(raw, dict):
        raise InvalidInput("Refresh scope must be an object")

    value = cast(dict[str, object], raw)
    kind = value.get("kind")

    if not isinstance(kind, str) or kind not in DETAIL_FIELDS:
        raise InvalidInput("Unknown refresh scope kind")

    allowed = {"kind"} | DETAIL_FIELDS[kind]

    if kind != "post":
        allowed.update({"domains", "tags"})

    if kind == "all":
        allowed.add("platforms")

    if set(value) - allowed:
        raise InvalidInput(
            "Refresh scope has fields that do not match its kind",
        )

    scope: Scope = {"kind": kind, **_filter_lists(value)}

    for key in DETAIL_FIELDS[kind].intersection(value):
        scope[key] = _text(value[key], key)

    _normalize_details(scope)

    if scope.get("blog_feed") == "external" and (
        scope.get("domains") or scope.get("tags")
    ):
        raise InvalidInput(
            "External blog refresh cannot use publisher domains or tags",
        )

    return scope


def _filter_lists(value: dict[str, object]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}

    for key in ("domains", "tags", "platforms"):
        if key not in value:
            continue

        values = value[key]

        if not isinstance(values, list):
            raise InvalidInput(f"Refresh {key} must be a list")

        entries = [_text(item, key).lower()
                   for item in cast(list[object], values)]

        if entries:
            result[key] = sorted(set(entries))

    return result


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise InvalidInput(f"Refresh {field} must be a non-empty string")

    return value.strip()


def _normalize_details(scope: Scope) -> None:
    platforms = set(cast(list[str], scope.get("platforms", [])))

    if platforms - (LABELS.keys() - {"all"}):
        raise InvalidInput("Refresh includes an unknown platform")

    if ("blog_feed" in scope
            and scope["blog_feed"] not in {"sources", "external"}):
        raise InvalidInput("Refresh blog_feed must be sources or external")

    if scope["kind"] == "post":
        if "domain" not in scope or "post_id" not in scope:
            raise InvalidInput("Post refresh needs domain and post_id")

        scope["domain"] = scope["domain"].lower()

    if "community" in scope:
        scope["community"] = scope["community"].lower().removeprefix("r/")

        if not re.fullmatch(r"[a-z0-9_]{2,21}", scope["community"]):
            raise InvalidInput("Invalid Reddit community")

    if "account" in scope:
        scope["account"] = scope["account"].lower().removeprefix("@")

        if not re.fullmatch(r"[a-z0-9_]{1,15}", scope["account"]):
            raise InvalidInput("Invalid X account")

    _validate_youtube_target(scope)
    _validate_x_feed(scope)


def _validate_youtube_target(scope: Scope) -> None:
    if scope["kind"] == "youtube" and sum(
        key in scope for key in ("channel_id", "feed_id", "playlist_id")
    ) > 1:
        raise InvalidInput("Choose one YouTube channel, feed, or playlist")

    if "playlist_id" in scope and not PLAYLIST_ID.fullmatch(
        scope["playlist_id"],
    ):
        raise InvalidInput("Invalid YouTube playlist identifier")


def _validate_x_feed(scope: Scope) -> None:
    if scope["kind"] == "x" and "feed_id" in scope and (
        scope["feed_id"] != "for_you" or "account" in scope
    ):
        raise InvalidInput("Choose X For You or an account")


def source_plan(settings: Settings, scope: Scope) -> RefreshPlan:
    """The configured sources a scope selects, before any saved targets."""
    kind = str(scope["kind"])
    sources = _selected_sources(settings, scope)
    label = LABELS[kind]

    if platforms := scope.get("platforms"):
        label = " + ".join(LABELS[platform] for platform in platforms)

    if kind == "all" and (scope.get("domains") or scope.get("tags")):
        label = (f"{label} · selected sources" if platforms
                 else "Selected sources")

    if kind == "rss" and (blog_feed := scope.get("blog_feed")):
        label = ("Blog source feeds" if blog_feed == "sources"
                 else "Blogs from external")

    if community := scope.get("community"):
        sources = _community_sources(sources, community)
        label = f"r/{community}"
    elif account := scope.get("account"):
        sources = _account_sources(sources, account)
        label = f"@{account}"

    if not sources:
        raise InvalidInput("No configured sources match this refresh context")

    selected, label = _x_settings(
        settings._replace(sources=sources), scope, label,
    )

    if kind == "rss" and len(scope.get("domains", ())) == 1:
        label = scope["domains"][0]

    return RefreshPlan(scope, label, selected)


def _x_settings(
    settings: Settings, scope: Scope, label: str,
) -> tuple[Settings, str]:
    if scope["kind"] != "x":
        return settings, label

    if scope.get("feed_id") == "for_you":
        return settings._replace(
            x=settings.x._replace(homepage=True),
            sources=(settings.sources[0]._replace(accounts=()),),
        ), "X For You"

    if scope.get("account"):
        settings = settings._replace(x=settings.x._replace(homepage=False))

    return settings, label


def _selected_sources(
    settings: Settings, scope: Scope,
) -> tuple[SourceConfig, ...]:
    domains = set(cast(list[str], scope.get("domains", [])))
    tags = set(cast(list[str], scope.get("tags", [])))
    platforms = set(cast(list[str], scope.get("platforms", [])))

    if domains - {source.domain for source in settings.sources}:
        raise InvalidInput(
            "Refresh includes a source that is no longer configured",
        )

    if tags - {tag for source in settings.sources for tag in source.tags}:
        raise InvalidInput(
            "Refresh includes a tag that is no longer configured",
        )

    tag_domains = {source.domain for source in settings.sources
                   if tags.intersection(source.tags)}
    selected: list[SourceConfig] = []
    kinds = _scope_source_kinds(scope)
    platform_kinds = {kind for platform in platforms for kind in (
        {"rss", "hacker_news", "x", "reddit"}
        if platform == "rss" else {platform}
    )}

    for source in settings.sources:
        if domains and source.domain not in domains:
            continue

        if tags and source.domain not in tag_domains:
            continue

        if platforms and source_kind(source.domain) not in platform_kinds:
            continue

        # Keep compound adapters intact when a source is selected.
        if scope["kind"] == "all" or source_kind(source.domain) in kinds:
            selected.append(source)

    return tuple(selected)


def _scope_source_kinds(scope: Scope) -> set[str]:
    if scope["kind"] != "rss":
        return {scope["kind"]}

    if scope.get("blog_feed") == "sources" or scope.get("domains"):
        return {"rss", "hacker_news"}

    # External publishers are discovered by these configured feeds. Refresh the
    # discovery sources instead of inventing configuration for publisher hosts.
    return {"rss", "hacker_news", "x", "reddit"}


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


def targets_youtube_item(scope: Scope) -> bool:
    return scope["kind"] == "youtube" and any(scope.get(key) for key in (
        "channel_id", "feed_id", "playlist_id",
    ))


def targets_playlists(scope: Scope) -> bool:
    return bool(
        scope.get("playlist_id") or scope.get("feed_id") == "playlists",
    )


def configured_youtube_feeds(settings: Settings) -> dict[str, YouTubeFeed]:
    return {
        feed.url: feed for source in settings.sources
        for feed in configured_feeds(settings.youtube, source)
    }


def youtube_feed_settings(
    settings: Settings, selected: Sequence[YouTubeFeed],
) -> tuple[Settings, str]:
    if not selected:
        raise InvalidInput("This YouTube channel or feed is not configured")

    urls = tuple(feed.url for feed in selected)
    source = settings.sources[0]._replace(
        accounts=(), feed_urls=urls,
        adapters=(SourceAdapterConfig(SourceAdapter.YOUTUBE, urls),),
    )
    youtube = settings.youtube._replace(
        news_tabs=(), hype=False, hype_playlists=(), playlists=False,
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

    label = (
        "Recommendations" if selected[0].kind == "home" else selected[0].name
    )

    return settings._replace(sources=(source,), youtube=youtube), label


def youtube_playlist_settings(
    settings: Settings, scope: Scope, saved: Mapping[str, str],
) -> tuple[Settings, str]:
    configured = {
        parse_qs(urlsplit(feed.url).query).get("list", [""])[0]
        for source in settings.sources
        for feed in configured_feeds(settings.youtube, source)
        if feed.kind == "playlist"
    }
    playlist_id = str(scope.get("playlist_id") or "")
    known = {
        identifier: title or "Playlist"
        for identifier, title in saved.items()
        if PLAYLIST_ID.fullmatch(identifier)
    }
    targets = configured

    if playlist_id:
        if playlist_id not in configured | known.keys():
            raise InvalidInput("This YouTube playlist has no saved target")

        targets = {playlist_id}

    if not targets and not known and not settings.youtube.authenticated:
        raise InvalidInput(
            "No saved or configured YouTube playlists to refresh",
        )

    urls = tuple("https://www.youtube.com/playlist?list=" + identifier
                 for identifier in sorted(targets) if identifier)
    source = settings.sources[0]._replace(
        accounts=(), feed_urls=urls,
        adapters=(SourceAdapterConfig(SourceAdapter.YOUTUBE, urls),),
    )
    youtube = settings.youtube._replace(
        news_tabs=(), hype=False, hype_playlists=(), homepage=False,
        playlists=not bool(playlist_id),
    )
    label = known.get(playlist_id, "Playlist") if playlist_id else "Playlists"

    return settings._replace(sources=(source,), youtube=youtube), label


def channel_feeds_matching(
    feeds: Mapping[str, YouTubeFeed],
    channel_id: str,
    saved: Iterable[ChannelFeed],
) -> list[YouTubeFeed]:
    configured = {_channel_url(feed.url) for feed in feeds.values()
                  if feed.kind == "channel"}
    matches = {_channel_url(channel_id)} & configured

    for row in saved:
        if channel_id in {row.channel_id, row.channel_url}:
            # Captured feeds can outlive configuration. Only their identity
            # mapping is useful here; collection must remain configured.
            matches.add(_channel_url(row.feed_url))

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


def youtube_video_id(post_id: str) -> str:
    video_id = post_id.removeprefix("video:")

    if not post_id.startswith("video:") or not VIDEO_ID.fullmatch(video_id):
        raise InvalidInput("Only individual YouTube videos can be refreshed")

    return video_id


def post_plan(settings: Settings, scope: Scope, label: str) -> RefreshPlan:
    domain, post_id = str(scope["domain"]), str(scope["post_id"])
    sources = tuple(source for source in settings.sources
                    if source.domain == domain)
    # Linked posts can be known without their publisher being configured.
    # The exact-target collector still needs a source for run bookkeeping.
    selected = settings._replace(sources=sources[:1] or (
        SourceConfig(domain, ()),
    ))

    return RefreshPlan(scope, label, selected, (domain, post_id))
