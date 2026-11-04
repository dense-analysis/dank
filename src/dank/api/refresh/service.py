"""Refresh use cases, and the saved targets and job queue they need."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol

from dank.api.refresh.model import (
    ChannelFeed,
    RefreshJob,
    RefreshPlan,
    Scope,
    channel_feeds_matching,
    configured_youtube_feeds,
    parse_scope,
    post_plan,
    source_plan,
    targets_playlists,
    targets_youtube_item,
    youtube_feed_settings,
    youtube_playlist_settings,
    youtube_video_id,
)
from dank.config import Settings
from dank.errors import InvalidInput


class RefreshTargets(Protocol):
    """Saved content that says what may be collected again."""

    async def saved_playlists(self, playlist_id: str) -> Mapping[str, str]:
        """Titles by playlist ID: that playlist, or all of them if empty."""
        ...

    async def channel_feeds(self) -> Iterable[ChannelFeed]: ...

    async def has_post(self, domain: str, post_id: str) -> bool: ...

    async def has_video(self, video_id: str) -> bool: ...


class RefreshQueue(Protocol):
    """The queue the lifespan owns."""

    def jobs(self) -> tuple[RefreshJob, ...]: ...

    def submit(self, plan: RefreshPlan) -> RefreshJob: ...


class RefreshService:
    def __init__(
        self,
        settings: Settings,
        targets: RefreshTargets,
        queue: RefreshQueue,
    ) -> None:
        self._settings = settings
        self._targets = targets
        self._queue = queue

    def jobs(self) -> tuple[RefreshJob, ...]:
        """Recent jobs, newest first."""
        return tuple(reversed(self._queue.jobs()))

    async def start(self, scope: object) -> RefreshJob:
        plan = await resolve_scope(self._settings, self._targets, scope)

        return self._queue.submit(plan)


async def resolve_scope(
    settings: Settings, targets: RefreshTargets, raw: object,
) -> RefreshPlan:
    """Turn reader context into collection targets already known to DANK."""
    try:
        return await _plan(settings, targets, parse_scope(raw))
    except InvalidInput:
        raise
    except ValueError as error:
        # urlsplit and configured_feeds reject values with plain ValueErrors,
        # and the reader shows what they say, such as what YouTube home needs.
        raise InvalidInput(str(error)) from error


async def _plan(
    settings: Settings, targets: RefreshTargets, scope: Scope,
) -> RefreshPlan:
    if scope["kind"] == "post":
        return await _saved_post_plan(settings, targets, scope)

    plan = source_plan(settings, scope)

    if not targets_youtube_item(scope):
        return plan

    selected, label = await _youtube_settings(plan.settings, targets, scope)

    return plan._replace(settings=selected, label=label)


async def _youtube_settings(
    settings: Settings, targets: RefreshTargets, scope: Scope,
) -> tuple[Settings, str]:
    if targets_playlists(scope):
        saved = await targets.saved_playlists(
            str(scope.get("playlist_id") or ""),
        )

        return youtube_playlist_settings(settings, scope, saved)

    feeds = configured_youtube_feeds(settings)

    if feed_id := scope.get("feed_id"):
        selected = [feeds[feed_id]] if feed_id in feeds else []
    else:
        selected = channel_feeds_matching(
            feeds, scope["channel_id"], await targets.channel_feeds(),
        )

    return youtube_feed_settings(settings, selected)


async def _saved_post_plan(
    settings: Settings, targets: RefreshTargets, scope: Scope,
) -> RefreshPlan:
    domain, post_id = str(scope["domain"]), str(scope["post_id"])

    if domain == "youtube.com":
        known = await targets.has_video(youtube_video_id(post_id))
        label = "YouTube video"
    else:
        known = await targets.has_post(domain, post_id)
        label = "Post"

    if not known:
        raise InvalidInput("This post has no saved collection target")

    return post_plan(settings, scope, label)
