from __future__ import annotations

import asyncio
import datetime
import pathlib
from collections import Counter
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock
from urllib.parse import urlsplit

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from dank.config import ScrapeSettings, SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import download_file_http
from dank.scrape.http import (
    HostCooldownError,
    RequestLimiter,
    current_limiter,
    http_response,
)
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.rss import (
    _fetch_text,  # pyright: ignore[reportPrivateUsage]
    fetch_feed_links,
)
from dank.scrape.tasks import gather_tasks


class _Response:
    def __init__(
        self, url: str, status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.url = URL(url)
        self.status = status
        self.headers = CIMultiDictProxy(CIMultiDict(headers or {}))
        self.request_info = aiohttp.RequestInfo(
            self.url, "GET", self.headers, self.url,
        )
        self.content_length = 2
        self.content = self

    async def text(self) -> str:
        return '<head><link type="application/rss+xml" href="/feed"></head>'

    async def iter_chunked(self, size: int) -> AsyncGenerator[bytes]:
        yield b"ok"

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                self.request_info, (), status=self.status,
                headers=self.headers,
            )


class _Client:
    def __init__(self) -> None:
        self.responses: dict[str, list[_Response]] = {}
        self.requests: list[str] = []
        self.active = self.peak = 0
        self.hosts: Counter[str] = Counter()
        self.host_peak: Counter[str] = Counter()

    async def __aenter__(self) -> _Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    @asynccontextmanager
    async def get(
        self, url: str, **kwargs: object,
    ) -> AsyncGenerator[_Response]:
        assert kwargs.get("allow_redirects") is False
        self.requests.append(url)
        self.active += 1
        self.peak = max(self.peak, self.active)
        host = urlsplit(url).hostname or ""
        self.hosts[host] += 1
        self.host_peak[host] = max(self.host_peak[host], self.hosts[host])

        try:
            await asyncio.sleep(0.001)
            responses = self.responses.get(url, [])
            yield responses.pop(0) if responses else _Response(url)
        finally:
            self.active -= 1
            self.hosts[host] -= 1


async def test_homepages_articles_and_media_share_http_limits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    monkeypatch.setattr(
        "dank.scrape.rss.aiohttp.ClientSession", Mock(return_value=client),
    )
    token = current_limiter.set(RequestLimiter(ScrapeSettings(
        http_concurrency=2, http_per_host=1,
    )))

    async def collect(host: str) -> None:
        await fetch_feed_links(host)
        await _fetch_text(cast(Any, client), f"https://{host}/post", accept=[])
        result = await download_file_http(
            discovery=AssetDiscovery(
                "rss", host, "post", f"https://{host}/a.jpg", "image", None,
            ),
            target_dir=tmp_path / host, http_client=cast(Any, client),
            max_asset_bytes=100, timestamp=datetime.datetime.now(datetime.UTC),
        )
        assert result is not None and result.local_path

    try:
        await gather_tasks(asyncio.create_task(collect(host)) for host in (
            "a.test", "a.test", "b.test", "c.test",
        ))
    finally:
        current_limiter.reset(token)

    assert client.peak == 2
    assert set(client.host_peak.values()) == {1}
    assert client.active == 0


async def test_cooldown_releases_capacity_and_applies_to_other_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    waiting = asyncio.Event()
    release = asyncio.Event()
    client = _Client()
    url = "https://a.test/feed"
    client.responses[url] = [_Response(url, 429, {"Retry-After": "20"})]
    limiter = RequestLimiter(ScrapeSettings(http_concurrency=1))
    stats = SourceMetrics(1, SourceConfig("a.test", ()))
    source_token = current_source.set(stats)
    token = current_limiter.set(limiter)
    monkeypatch.setattr(
        "dank.scrape.http.time", SimpleNamespace(monotonic=lambda: now[0]),
    )

    async def sleep(delay: float) -> None:
        assert client.hosts["a.test"] == 0
        assert delay == 20
        waiting.set()
        await release.wait()
        now[0] = 20

    monkeypatch.setattr("dank.scrape.http.retry_sleep", sleep)
    first = asyncio.create_task(_fetch_text(cast(Any, client), url, accept=[]))
    second: asyncio.Task[str | None] | None = None

    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        # A second source with the same physical host must also wait.
        second = asyncio.create_task(_fetch_text(
            cast(Any, client), "https://a.test/other", accept=[],
        ))
        result = await asyncio.wait_for(_fetch_text(
            cast(Any, client), "https://b.test/feed", accept=[],
        ), timeout=1)
        assert result
        assert client.requests == [url, "https://b.test/feed"]
        assert stats.active_http == 0
        release.set()
        assert await first
        assert await second
        assert stats.counts["http_429"] == 1
        assert stats.counts["retries"] == 1
    finally:
        release.set()
        await gather_tasks([first, *([second] if second else [])])
        current_limiter.reset(token)
        current_source.reset(source_token)


async def test_extended_and_long_cooldowns_do_not_wait_forever(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    limiter = RequestLimiter(ScrapeSettings())
    monkeypatch.setattr(
        "dank.scrape.http.time", SimpleNamespace(monotonic=lambda: now[0]),
    )
    url = "https://a.test/page"
    limiter.cool_down(url, 20)
    sleeps: list[float] = []

    async def extend(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay
        limiter.cool_down(url, 20)

    monkeypatch.setattr("dank.scrape.http.retry_sleep", extend)

    with pytest.raises(HostCooldownError, match="30s wait budget"):
        async with limiter.slot(url):
            pytest.fail("cooling host admitted")

    assert sleeps == [20]
    limiter.cool_down(url, 3600)

    with pytest.raises(HostCooldownError):
        async with limiter.slot(url):
            pytest.fail("long Retry-After ignored")

    assert sleeps == [20]


async def test_redirects_obey_destination_cooldown() -> None:
    client = _Client()
    first = "https://a.test/start"
    final = "https://b.test/end"
    client.responses[first] = [_Response(first, 302, {"Location": final})]
    limiter = RequestLimiter(ScrapeSettings(http_concurrency=1))
    limiter.cool_down(final, 3600)
    token = current_limiter.set(limiter)

    try:
        with pytest.raises(HostCooldownError):
            async with http_response(cast(Any, client), first):
                pytest.fail("redirect ignored cooldown")

        assert client.requests == [first]
        async with http_response(cast(Any, client), "https://c.test/ok"):
            pass
    finally:
        current_limiter.reset(token)

    assert client.active == 0


async def test_cancellation_releases_http_slots() -> None:
    limiter = RequestLimiter(ScrapeSettings(http_concurrency=1))
    acquired = asyncio.Event()

    async def hold() -> None:
        async with limiter.slot("https://a.test/one"):
            acquired.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold())
    await acquired.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    async with asyncio.timeout(1), limiter.slot("https://b.test/two"):
        pass


@pytest.mark.parametrize("limit", [2, 6])
async def test_media_limit_is_shared_across_source_batches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, limit: int,
) -> None:
    active = peak = 0

    async def download(*args: Any, **kwargs: Any) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.001)
        active -= 1

    monkeypatch.setattr("dank.scrape.assets._download_one", download)
    token = current_limiter.set(RequestLimiter(ScrapeSettings(
        media_concurrency=limit,
    )))

    try:
        await gather_tasks(asyncio.create_task(download_assets(
            [AssetDiscovery(
                "rss", host, str(i), f"https://{host}/{i}", "image", None,
            ) for i in range(8)],
            assets_dir=tmp_path, http_client=cast(Any, None),
        )) for host in ("a.test", "b.test", "c.test"))
    finally:
        current_limiter.reset(token)

    assert peak == limit
    assert active == 0


async def test_large_media_batch_allows_another_source_to_download(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    calls: list[str] = []

    async def download(
        discovery: AssetDiscovery, *args: Any, **kwargs: Any,
    ) -> None:
        calls.append(discovery.domain)
        await asyncio.sleep(0)

    monkeypatch.setattr("dank.scrape.assets._download_one", download)
    token = current_limiter.set(RequestLimiter(ScrapeSettings(
        media_concurrency=1,
    )))

    try:
        await gather_tasks(asyncio.create_task(download_assets(
            [AssetDiscovery(
                "rss", host, str(i), f"https://{host}/{i}", "image", None,
            ) for i in range(size)],
            assets_dir=tmp_path, http_client=cast(Any, None),
        )) for host, size in (("large.test", 100), ("small.test", 1)))
    finally:
        current_limiter.reset(token)

    assert calls.index("small.test") <= 4
    assert len(calls) == 101
