from __future__ import annotations

import asyncio
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.config import Settings, SourceConfig, load_settings
from dank.scrape import runner
from dank.scrape.concurrency import collection_resources
from dank.scrape.runner import (
    _run_source_sessions,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.zendriver import BrowserConfig


def settings(tmp_path: pathlib.Path, *domains: str) -> Settings:
    return load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
        sources=tuple(SourceConfig(domain, ()) for domain in domains),
        media_download_types=(),
    )


async def test_independent_refreshes_collect_concurrently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    domains: list[str] = []

    async def collect(configured: Settings, **kwargs: Any) -> Any:
        domains.append(configured.sources[0].domain)

        if len(domains) == 2:
            started.set()

        await release.wait()

    monkeypatch.setattr(runner, "_run_scrape", collect)
    active = [asyncio.create_task(runner.run_scrape(
        settings(tmp_path, domain), scoped=True,
    )) for domain in ("example.test", "news.ycombinator.com")]

    try:
        async with asyncio.timeout(1):
            await started.wait()

        assert set(domains) == {"example.test", "news.ycombinator.com"}
        assert all(not task.done() for task in active)
    finally:
        release.set()
        await asyncio.gather(*active)


@pytest.mark.parametrize(("active_scoped", "active_exclusive",
                          "pending_scoped", "pending_exclusive"), [
    (False, False, True, False),
    (True, True, True, False),
    (True, False, False, False),
    (True, False, True, True),
])
async def test_cli_and_all_refreshes_exclude_scoped_collection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, *,
    active_scoped: bool, active_exclusive: bool,
    pending_scoped: bool, pending_exclusive: bool,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def collect(*args: Any, **kwargs: Any) -> Any:
        started.set()
        await release.wait()

    monkeypatch.setattr(runner, "_run_scrape", collect)
    active = asyncio.create_task(runner.run_scrape(
        settings(tmp_path, "example.test"), scoped=active_scoped,
        exclusive=active_exclusive,
    ))
    await started.wait()

    try:
        with pytest.raises(runner.CollectionBusyError):
            await runner.run_scrape(
                settings(tmp_path, "news.ycombinator.com"),
                scoped=pending_scoped, exclusive=pending_exclusive,
            )
    finally:
        release.set()
        await active

    await runner.run_scrape(
        settings(tmp_path, "news.ycombinator.com"),
        scoped=pending_scoped, exclusive=pending_exclusive,
    )


async def test_failed_overlap_releases_partial_resource_acquisitions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def collect(configured: Settings, **kwargs: Any) -> Any:
        if configured.sources[0].domain == "z.test":
            started.set()
            await release.wait()

    monkeypatch.setattr(runner, "_run_scrape", collect)
    active = asyncio.create_task(runner.run_scrape(
        settings(tmp_path, "z.test"), scoped=True,
    ))
    await started.wait()

    try:
        with pytest.raises(runner.CollectionBusyError):
            await runner.run_scrape(
                settings(tmp_path, "a.test", "z.test"), scoped=True,
            )

        # The failed two-source attempt must not retain its first lock.
        await runner.run_scrape(settings(tmp_path, "a.test"), scoped=True)
    finally:
        release.set()
        await active

    await runner.run_scrape(settings(tmp_path, "z.test"), scoped=True)


async def test_cancelled_refresh_releases_all_locks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started = asyncio.Event()

    async def collect(*args: Any, **kwargs: Any) -> Any:
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "_run_scrape", collect)
    configured = settings(tmp_path, "reddit.com")
    active = asyncio.create_task(runner.run_scrape(configured, scoped=True))
    await started.wait()
    active.cancel()

    with pytest.raises(asyncio.CancelledError):
        await active

    monkeypatch.setattr(runner, "_run_scrape", AsyncMock())
    await runner.run_scrape(configured, scoped=True)
    await runner.run_scrape(configured)


async def test_shared_profile_blocks_different_source_domains(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def collect(*args: Any, **kwargs: Any) -> Any:
        started.set()
        await release.wait()

    monkeypatch.setattr(runner, "_run_scrape", collect)
    x_settings = settings(tmp_path, "x.com")
    youtube_settings = settings(tmp_path, "youtube.com")
    youtube_settings = youtube_settings._replace(youtube=(
        youtube_settings.youtube._replace(
            authenticated=True, profile_dir=tmp_path / "browser-profile",
        )
    ))
    active = asyncio.create_task(runner.run_scrape(x_settings, scoped=True))
    await started.wait()

    try:
        with pytest.raises(runner.CollectionBusyError):
            await runner.run_scrape(youtube_settings, scoped=True)
    finally:
        release.set()
        await active


def test_resource_identity_uses_exact_selected_sources_and_profile_paths(
    tmp_path: pathlib.Path,
) -> None:
    configured = settings(tmp_path, "reddit.com", "news.ycombinator.com")
    assert collection_resources(
        configured, ("news.ycombinator.com", "saved"),
    ) == frozenset({"domain:news.ycombinator.com"})
    assert not collection_resources(settings(tmp_path, "example.test")) & (
        collection_resources(settings(tmp_path, "news.ycombinator.com"))
    )
    youtube_settings = settings(tmp_path, "youtube.com")
    youtube_settings = youtube_settings._replace(youtube=(
        youtube_settings.youtube._replace(
            authenticated=True,
            profile_dir=tmp_path / "unused" / ".." / "browser-profile",
        )
    ))
    assert collection_resources(youtube_settings) & collection_resources(
        settings(tmp_path, "x.com"),
    ) == {f"profile:{(tmp_path / 'browser-profile').resolve()}"}
    assert collection_resources(
        youtube_settings, ("youtube.com", "video:abcdefghijk"),
    ) == {"domain:youtube.com"}


async def test_scoped_recovery_profiles_are_independent_and_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    profiles: list[pathlib.Path] = []
    ready, release = asyncio.Event(), asyncio.Event()

    class Session:
        def __init__(self, config: BrowserConfig) -> None:
            profile = config.profile_dir

            if profile is not None and profile.name.startswith("dank-rss-"):
                profiles.append(profile)

        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    async def collect(*args: Any, **kwargs: Any) -> None:
        if len(profiles) == 2:
            ready.set()

        await release.wait()

    monkeypatch.setattr(runner, "BrowserSession", Session)
    monkeypatch.setattr(runner, "_collect_with_history", collect)
    configured = settings(tmp_path, "example.test")
    config = BrowserConfig.from_settings(configured)
    active = [asyncio.create_task(_run_source_sessions(
        configured, cast(Any, None), cast(Any, None), tmp_path,
        config, config, config, retry_failed_pages=False, x_posts=(),
        reddit_details=False, x_details=False, scoped=True,
    )) for _ in range(2)]

    try:
        async with asyncio.timeout(1):
            await ready.wait()

        assert len(set(profiles)) == 2
        assert all(profile.is_dir() for profile in profiles)
    finally:
        release.set()
        await asyncio.gather(*active)

    assert all(not profile.exists() for profile in profiles)
