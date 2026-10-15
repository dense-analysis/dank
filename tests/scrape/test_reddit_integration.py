import asyncio
import datetime
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import (
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
    SourceConfig,
    load_settings,
)
from dank.scrape import runner
from dank.scrape.runner import (
    _collect_source,  # pyright: ignore[reportPrivateUsage]
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch


async def test_loaded_defaults_reach_the_collector_as_top_for_the_day(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "reddit.fixture.toml"
    path.write_text(
        'sources=[{domain="https://www.reddit.com", '
        'feed_urls=["r/LocalLLaMA"]}]',
    )
    settings = load_settings(path)
    source = settings.sources[0]
    received: list[dict[str, object]] = []

    async def collect(*args: object, **kwargs: object):
        received.append(kwargs)
        yield ScrapeBatch([], [])

    monkeypatch.setattr(runner, "scrape_reddit_communities", collect)
    _ = [batch async for batch in _discover_source_batches(
        settings, source, cast(Any, None), cast(Any, None), cast(Any, None),
        feed_staleness=datetime.timedelta(days=1), batch_size=50,
    )]
    assert received[0]["sort"] is RedditSort.TOP
    assert received[0]["time_filter"] is RedditTimeFilter.DAY


async def test_reddit_dispatch_uses_community_sort_and_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings("config.native.example.toml")
    source = SourceConfig(
        "reddit.com", (), ("https://www.reddit.com/r/LocalLLaMA/",),
        reddit=RedditSourceConfig(time_filter=RedditTimeFilter.WEEK),
    )
    session = object()
    calls: list[tuple[object, ...]] = []

    async def collect(*args: object, **kwargs: object):
        calls.append((*args, kwargs))
        yield ScrapeBatch([], [])

    monkeypatch.setattr(runner, "scrape_reddit_communities", collect)
    # Any attempt to use RSS discovery or a client fails this test.
    batches = [batch async for batch in _discover_source_batches(
        settings, source, cast(Any, None), cast(Any, None),
        cast(Any, session), feed_staleness=datetime.timedelta(days=1),
        batch_size=50,
    )]
    assert batches == [ScrapeBatch([], [])]
    assert calls == [(
        settings.reddit, ("LocalLLaMA",), session,
        {"sort": "top", "time_filter": "week"},
    )]


async def test_reddit_and_x_do_not_navigate_the_shared_browser_concurrently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(sources=(
        SourceConfig("x.com", ("someone",)),
        SourceConfig("reddit.com", (), ("https://www.reddit.com/r/python/",)),
    ))
    active = 0
    peak = 0
    visited: list[str] = []

    async def discover(*args: Any, **kwargs: Any):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        visited.append(args[1].domain)
        await asyncio.sleep(0)
        yield ScrapeBatch([], [])
        active -= 1

    monkeypatch.setattr(runner, "_discover_source_batches", discover)
    history = MagicMock()
    history.sources = {}
    history.saved_sources = set()
    history.batch_size = 50
    history.queue_size = 0
    lock = asyncio.Lock()
    client = AsyncMock()
    await asyncio.gather(*(
        _collect_source(
            settings, source, index, client, cast(Any, None),
            cast(Any, None), history, tmp_path, tmp_path, lock,
        )
        for index, source in enumerate(settings.sources, 1)
    ))
    assert visited == ["x.com", "reddit.com"]
    assert peak == 1
    assert history.sources[2].row(history.run_id, 2)["source"] == "reddit"
