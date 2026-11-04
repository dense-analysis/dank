"""In-memory stand-ins for the refresh feature's driven adapters."""

from collections.abc import Awaitable, Callable, Mapping

from dank.api.refresh.manager import CollectionWorker, Report
from dank.api.refresh.model import ChannelFeed, RefreshPlan
from dank.api.refresh.service import RefreshTargets


class FakeRefreshTargets:
    """Saved collection targets a test sets as plain values."""

    def __init__(self) -> None:
        self.playlists: dict[str, str] = {}
        self.channels: list[ChannelFeed] = []
        self.posts: set[tuple[str, str]] = set()
        self.videos: set[str] = set()

    async def saved_playlists(self, playlist_id: str) -> Mapping[str, str]:
        return {
            key: title for key, title in self.playlists.items()
            if not playlist_id or key == playlist_id
        }

    async def channel_feeds(self) -> list[ChannelFeed]:
        return self.channels

    async def has_post(self, domain: str, post_id: str) -> bool:
        return (domain, post_id) in self.posts

    async def has_video(self, video_id: str) -> bool:
        return video_id in self.videos


class UnreachableTargets:
    """For rules that must settle a scope before reading anything saved."""

    async def saved_playlists(self, playlist_id: str) -> Mapping[str, str]:
        raise AssertionError("Read saved playlists")

    async def channel_feeds(self) -> list[ChannelFeed]:
        raise AssertionError("Read saved channels")

    async def has_post(self, domain: str, post_id: str) -> bool:
        raise AssertionError("Read saved posts")

    async def has_video(self, video_id: str) -> bool:
        raise AssertionError("Read saved videos")


type Script = Callable[[RefreshPlan, Report], Awaitable[None]]


async def complete(plan: RefreshPlan, report: Report) -> None:
    report({"status": "completed", "message": "Done"})


class FakeWorker:
    """Runs a test's script for each job in place of a spawned process."""

    def __init__(self) -> None:
        self.script: Script = complete

    async def collect(
        self, job_id: str, plan: RefreshPlan, report: Report,
    ) -> None:
        await self.script(plan, report)


# Pyright checks these assignments, so the fakes can't drift from the ports.
_TARGETS: tuple[type[RefreshTargets], ...] = (
    FakeRefreshTargets, UnreachableTargets,
)
_WORKER: type[CollectionWorker] = FakeWorker
