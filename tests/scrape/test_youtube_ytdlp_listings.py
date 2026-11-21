from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from yt_dlp import YoutubeDL  # type: ignore[import-untyped]
from yt_dlp.networking import Request, Response  # type: ignore[import-untyped]
from yt_dlp.networking.exceptions import (
    HTTPError,  # type: ignore[import-untyped]
)

from dank.config import (
    ScrapeSettings,
    SourceConfig,
    YouTubeSettings,
    load_settings,
)
from dank.scrape.account_schedule import AccountCheck
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.runner import (
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    _load_account_checks,  # pyright: ignore[reportPrivateUsage]
    _plan_account_checks,  # pyright: ignore[reportPrivateUsage]
    _save_account_checks,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.youtube import scrape_youtube_feeds, ytdlp_listings
from dank.scrape.youtube.feeds import describe_feed
from dank.scrape.youtube.playlists import playlist_capture, playlist_metadata
from dank.scrape.youtube.ytdlp_listings import (
    ListingFailure,
    ListingStopped,
    ListingWorker,
    _hype_content,  # pyright: ignore[reportPrivateUsage]
    _read_pages,  # pyright: ignore[reportPrivateUsage]
    collect_listing,
)
from dank.storage.clickhouse import QueryResult
from tests.worker import MemoryProcess, write_message

CHANNEL = "https://www.youtube.com/@example/videos"
PLAYLIST = "https://www.youtube.com/playlist?list=PLtest"
CHANNEL_ID = "UC" + "a" * 22
VIDEO = "abcdefghijk"


def renderer(identifier: str) -> dict[str, Any]:
    return {"videoRenderer": {"videoId": identifier,
                              "title": {"simpleText": "Video"}}}


def continuation(token: str) -> dict[str, Any]:
    return {"continuationItemRenderer": {"continuationEndpoint": {
        "continuationCommand": {"token": token},
    }}}


def channel_page() -> dict[str, Any]:
    return {
        "metadata": {"channelMetadataRenderer": {"externalId": CHANNEL_ID}},
        "contents": {"twoColumnBrowseResultsRenderer": {"tabs": [
            {"tabRenderer": {"tabIdentifier": "videos", "selected": True,
                             "content": {"richGridRenderer": {"contents": [
                                 renderer(VIDEO), continuation("next"),
                             ]}}}},
            {"tabRenderer": {"tabIdentifier": "featured",
                             "content": renderer("suggested1")}},
        ]}},
    }


@pytest.mark.parametrize("outcome", ["complete", "repeat", "stop"])
def test_channel_worker_uses_pinned_tab_paging_and_keeps_original_items(
    monkeypatch: pytest.MonkeyPatch, outcome: str,
) -> None:
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(ytdlp_listings, "emit", events.append)
    first = channel_page()
    next_video = renderer("zyxwvutsrqp")
    following = [next_video]

    if outcome == "repeat":
        following.append(continuation("next"))

    options = cast(Any, {"quiet": True, "cachedir": False})

    with YoutubeDL(options) as downloader:
        # Keep yt-dlp's tab identity and continuation parsing. Replace HTTP
        # at the pinned extractor seam so the fixture cannot visit YouTube.
        extractor = cast(Any, downloader).get_info_extractor("YoutubeTab")
        initial = Mock(return_value=(first, {}))
        browse = Mock(return_value={"onResponseReceivedActions": [{
            "appendContinuationItemsAction": {"continuationItems": following},
        }]})
        monkeypatch.setattr(extractor, "_extract_data", initial)
        monkeypatch.setattr(extractor, "_extract_response", browse)
        feed = describe_feed(CHANNEL)
        stop = "known channel overlap" if outcome == "stop" else None
        page_decision = Mock(return_value=stop)

        if outcome == "repeat":
            with pytest.raises(ListingFailure, match="Repeated listing"):
                _read_pages(extractor, feed, "GB", 3, page_decision)
        else:
            assert _read_pages(
                extractor, feed, "GB", 3, page_decision,
            ) == (stop or "listing exhausted")

        initial.assert_called_once_with(CHANNEL + "?gl=GB&hl=en-GB", feed.name)

        if stop:
            page_decision.assert_called_once()
            browse.assert_not_called()
        else:
            assert page_decision.call_count == 2
            browse.assert_called_once()
            assert browse.call_args.kwargs["query"]["continuation"] == "next"

    assert events[0]["raw"] == {"metadata": first["metadata"]}
    items = [event["raw"] for event in events if event["event"] == "renderer"]
    expected = [renderer(VIDEO)["videoRenderer"]]

    if not stop:
        expected.append(next_video["videoRenderer"])

    assert items == expected


def test_hype_uses_ranked_lockups_from_the_requested_playlist() -> None:
    listing = {"items": [{"lockupViewModel": {
        "contentId": VIDEO, "contentType": "LOCKUP_CONTENT_TYPE_VIDEO",
    }}]}
    later = {"items": [{"lockupViewModel": {
        "contentId": "zyxwvutsrqp", "contentType": "LOCKUP_CONTENT_TYPE_VIDEO",
    }}]}
    selected = {"selected": True, "content": {"sectionListRenderer": {
        "contents": [{"itemSectionRenderer": {"contents": [
            {"linearLayoutViewModel": listing},
            {"linearLayoutViewModel": later},
            renderer("suggested1"),
        ]}}],
    }}}
    data = {"header": {"playlistHeaderRenderer": {"playlistId": "PLtest"}}}

    assert _hype_content(data, [selected], selected, "PLtest") == [
        {"linearLayoutViewModel": listing}, {"linearLayoutViewModel": later},
    ]

    with pytest.raises(ValueError, match="scoped playlist"):
        _hype_content(data, [selected], selected, "PLwrong")


@pytest.mark.parametrize("authenticated", [False, True])
async def test_listings_use_worker_without_browser_or_watch_page(
    monkeypatch: pytest.MonkeyPatch, memory_path: Path, *, authenticated: bool,
) -> None:
    fixture = memory_path / "settings.toml"
    fixture.touch()
    settings = load_settings(fixture)._replace(
        data_dir=memory_path,
        youtube=YouTubeSettings(authenticated=authenticated, playlists=False,
                                news_tabs=("top_stories",)),
    )
    source = SourceConfig(
        "youtube.com", accounts=("@example",), feed_urls=(PLAYLIST,),
    )
    session = AsyncMock()
    session.get_browser.side_effect = AssertionError("Unexpected browser")
    cookies = [{"name": "SID", "domain": ".youtube.com", "value": "fixture"}]
    load = AsyncMock(return_value=list(cookies))
    monkeypatch.setattr(
        "dank.scrape.youtube.load_youtube_session_cookies", load,
    )
    feed_kinds: list[str] = []

    async def worker(
        self: ListingWorker, request: dict[str, Any], budget: float,
        receive: Any,
    ) -> str:
        feed = request["feed"]
        feed_kinds.append(feed["kind"])
        assert request["cookies"] == (cookies if authenticated else [])
        receive({"event": "listing", "url": feed["url"], "title": "",
                 "raw": {"metadata": channel_page()["metadata"]}})
        receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                 "raw": renderer(VIDEO)["videoRenderer"]})
        receive({"event": "page"})

        return "listing exhausted"

    playlist = AsyncMock(return_value=playlist_capture(
        "PLtest", playlist_metadata("PLtest", {}), (), status="complete",
    ))
    monkeypatch.setattr(ListingWorker, "collect", worker)
    monkeypatch.setattr("dank.scrape.youtube.collect_playlist", playlist)
    batches = [batch async for batch in scrape_youtube_feeds(
        settings, source, session,
    )]

    assert feed_kinds == ["hype", "news", "channel"]
    assert sum(len(batch.posts) for batch in batches) == 3
    assert all(not batch.assets for batch in batches)
    assert sum(len(batch.youtube_channels) for batch in batches) == 1
    assert sum(len(batch.youtube_playlists) for batch in batches) == 1
    session.get_browser.assert_not_awaited()
    playlist.assert_awaited_once()

    if authenticated:
        load.assert_awaited_once_with(
            memory_path / "youtube-browser-profile/youtube-session.json",
        )
    else:
        load.assert_not_awaited()


async def test_only_due_channels_are_checked_after_their_batches_are_saved(
    memory_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = memory_path / "settings.toml"
    config.write_text('[youtube]\nhype = false\nplaylists = false\n')
    settings = load_settings(config)._replace(data_dir=memory_path)
    sorted_feed = "https://www.youtube.com/@sorted/videos?sort=p"
    quiet = "https://www.youtube.com/@quiet/videos"
    failed = "https://www.youtube.com/@failed/videos"
    source = SourceConfig(
        "youtube.com", ("@example", "@quiet", "@failed"), (sorted_feed,),
    )
    now = dt.datetime(2026, 11, 21, tzinfo=dt.UTC)
    day = dt.timedelta(days=1)
    prior = AccountCheck(now - 10 * day, now - dt.timedelta(hours=1))
    state = memory_path / "youtube-account-checks.json"
    _save_account_checks(state, {CHANNEL: prior, quiet: prior, failed: prior})
    attempted: list[str] = []

    async def worker(
        self: ListingWorker, request: dict[str, Any], budget: float,
        receive: Any,
    ) -> str:
        url: str = request["feed"]["url"]
        attempted.append(url)
        receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                 "raw": renderer(VIDEO)["videoRenderer"]})
        receive({"event": "page"})

        if url == failed:
            raise ListingFailure("partial fixture")

        return "listing exhausted"

    activity = QueryResult([
        {"adapter": "youtube", "account": CHANNEL, "recent_posts": 250,
         "oldest_at": now - 8 * day,
         "latest_at": now - day},
        {"adapter": "youtube", "account": quiet, "recent_posts": 0,
         "oldest_at": now - 9 * day,
         "latest_at": now - 8 * day},
    ])
    client = Mock(fetch_json=AsyncMock(side_effect=[
        activity, QueryResult([]), QueryResult([]),
    ]))
    monkeypatch.setattr(ListingWorker, "collect", worker)
    plans = await _plan_account_checks(
        settings._replace(sources=(source,)), client, now,
    )

    async for batch in _discover_source_batches(
        settings, source, client, AsyncMock(), AsyncMock(), feed_staleness=day,
        batch_size=50, scrape_started_at=now, account_plans=plans,
    ):
        assert batch.posts

        for post in batch.posts:
            url = json.loads(post.payload)["discovery"]["feed_url"]

            if url == CHANNEL:
                assert _load_account_checks(state)[url].last_checked_at == (
                    prior.last_checked_at
                )

    assert sorted(attempted) == sorted([
        CHANNEL, failed, sorted_feed,
        "https://www.youtube.com/feed/news_destination",
        "https://www.youtube.com/feed/news_destination/business",
        "https://www.youtube.com/feed/news_destination/world",
    ])
    activity_call, overlap_call, _ = client.fetch_json.await_args_list
    assert activity_call.args[1]["accounts"] == [
        ("youtube", feed, now - 7 * day)
        for feed in sorted((CHANNEL, quiet, failed))
    ]
    assert overlap_call.args[1]["feeds"] == [CHANNEL, failed]
    assert _load_account_checks(state) == {
        CHANNEL: AccountCheck(prior.first_checked_at, now), quiet: prior,
        failed: prior._replace(retry_due=True),
    }


async def test_channel_yields_ready_pages_and_cancels_the_worker_on_close(
    listing_children: list[MemoryProcess],
) -> None:
    from dank.scrape.youtube import (
        _scrape_feed,  # pyright: ignore[reportPrivateUsage]
    )

    feed = describe_feed(CHANNEL.replace("example", "hang"))

    async with aclosing(_scrape_feed(
        None, feed, YouTubeSettings(), set(),
    )) as batches:
        async with asyncio.timeout(5):
            batch = await anext(batches)

        assert [post.post_id for post in batch.posts] == [
            f"video:{VIDEO}", "video:aaaaaaaaaaa",
        ]

    assert len(listing_children) == 1
    assert listing_children[0].returncode is not None


@pytest.mark.parametrize("timed_out", [False, True])
async def test_channel_overlap_and_timeout_keep_posts_without_browser_fallback(
    monkeypatch: pytest.MonkeyPatch, memory_path: Path, *, timed_out: bool,
) -> None:
    fixture = memory_path / "settings.toml"
    fixture.touch()
    settings = load_settings(fixture)._replace(youtube=YouTubeSettings(
        hype=False, news_tabs=(), playlists=False, known_overlap=1,
    ))
    source = SourceConfig("youtube.com", accounts=("@example",))
    stats = SourceMetrics(0, source)
    session = AsyncMock()

    async def worker(
        self: ListingWorker, request: dict[str, Any], budget: float,
        receive: Any,
    ) -> str:
        try:
            receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                     "raw": renderer(VIDEO)["videoRenderer"]})
        except ListingStopped as stop:
            return str(stop)

        if timed_out:
            raise TimeoutError

        pytest.fail("The known overlap must stop the worker")

    monkeypatch.setattr(ListingWorker, "collect", worker)
    token = current_source.set(stats)

    try:
        batches = [batch async for batch in scrape_youtube_feeds(
            settings, source, session,
            known_ids={} if timed_out else {CHANNEL: {f"video:{VIDEO}"}},
        )]
    finally:
        current_source.reset(token)

    post = batches[0].posts[0]
    assert post.post_id == f"video:{VIDEO}"
    assert json.loads(post.payload)["renderer"] == renderer(VIDEO)
    assert stats.counts["fetch_failures"] == int(timed_out)
    session.get_browser.assert_not_awaited()


async def listing_peer(
    stdin: asyncio.StreamReader, stdout: asyncio.StreamReader,
) -> int:
    async def page(*video_ids: str) -> str | None:
        for video_id in video_ids:
            write_message(stdout, {
                "event": "renderer", "key": "videoRenderer", "shelf": "",
                "raw": {"videoId": video_id},
            })

        write_message(stdout, {"event": "page"})

        return cast(str | None, json.loads(await stdin.readline()))

    while line := await stdin.readline():
        request = json.loads(line)
        write_message(stdout, {
            "event": "http_request", "host": "www.youtube.com",
        })
        decision = json.loads(await stdin.readline())

        if decision is not None:
            write_message(stdout, {"event": "error", "error_type": decision})
            continue

        name = request["feed"]["name"]

        if name == "@limited":
            write_message(stdout, {"event": "rate_limited", "delay": 90.0})
            write_message(stdout, {
                "event": "error", "error_type": "rate limited",
            })
            continue

        stop = await page(VIDEO, "aaaaaaaaaaa")

        if stop is None:
            if name == "@hang":
                await asyncio.Event().wait()

            stop = await page("bbbbbbbbbbb")

        write_message(stdout, {
            "event": "done", "reason": stop or "listing exhausted",
        })

    return 0


@pytest.fixture
def listing_children(monkeypatch: pytest.MonkeyPatch) -> list[MemoryProcess]:
    children: list[MemoryProcess] = []

    async def start(module: str) -> MemoryProcess:
        assert module == ytdlp_listings.__name__
        process = MemoryProcess(listing_peer)
        children.append(process)

        return process

    monkeypatch.setattr(ytdlp_listings, "start_worker", start)

    return children


def test_listing_worker_translates_yt_dlp_rate_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    def collect(request: dict[str, str]) -> str:
        def fetch(req: Any) -> Any:
            raise HTTPError(Response(
                BytesIO(), req.url, {"Retry-After": "90"}, status=429,
            ))

        ytdlp_listings._open_request(  # pyright: ignore[reportPrivateUsage]
            fetch, Request(request["url"]),
        )
        pytest.fail("The worker accepted an HTTP 429")

    monkeypatch.setattr(ytdlp_listings, "_collect_request", collect)
    monkeypatch.setattr(sys, "stdin", StringIO(
        json.dumps({"url": CHANNEL}) + "\nnull\n",
    ))
    ytdlp_listings._worker()  # pyright: ignore[reportPrivateUsage]

    assert list(map(json.loads, capsys.readouterr().out.splitlines())) == [
        {"event": "http_request", "host": "www.youtube.com"},
        {"event": "rate_limited", "delay": 90.0},
        {
            "event": "error",
            "error_type": "YouTube listing request rate limited",
        },
    ]


async def test_scrape_reuses_listing_process_after_overlap(
    memory_path: Path, listing_children: list[MemoryProcess],
) -> None:
    fixture = memory_path / "settings.toml"
    fixture.write_text(
        "[youtube]\nhype = false\nnews_tabs = []\nplaylists = false\n"
        "known_overlap = 1\nlisting_workers = 1\n",
    )
    settings = load_settings(fixture)
    source = SourceConfig("youtube.com", accounts=("@example", "@next"))

    async with asyncio.timeout(5):
        batches = [batch async for batch in scrape_youtube_feeds(
            settings, source, AsyncMock(),
            known_ids={CHANNEL: {f"video:{VIDEO}"}},
        )]

    assert [[post.post_id for post in batch.posts] for batch in batches] == [
        [f"video:{VIDEO}"], [f"video:{VIDEO}", "video:aaaaaaaaaaa"],
        ["video:bbbbbbbbbbb"],
    ]
    assert len(listing_children) == 1
    assert listing_children[0].returncode is not None


async def test_concurrent_channels_share_http_slots_and_reuse_processes(
    memory_path: Path, listing_children: list[MemoryProcess],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = memory_path / "settings.toml"
    fixture.write_text("[youtube]\nhype = false\nnews_tabs = []\n")
    settings = load_settings(fixture)
    accounts = tuple(f"@channel{index}" for index in range(5))
    limiter = RequestLimiter(ScrapeSettings())
    slot = limiter.slot
    simultaneous = asyncio.Event()
    active = peak = 0

    @asynccontextmanager
    async def track_slot(url: str) -> AsyncGenerator[None]:
        nonlocal active, peak

        async with slot(url):
            active += 1
            peak = max(peak, active)

            if active == 2:
                simultaneous.set()

            try:
                await simultaneous.wait()
                yield
            finally:
                active -= 1

    monkeypatch.setattr(limiter, "slot", track_slot)
    token = current_limiter.set(limiter)

    try:
        async with asyncio.timeout(8):
            batches = [batch async for batch in scrape_youtube_feeds(
                settings, SourceConfig("youtube.com", accounts=accounts),
                AsyncMock(),
            )]
    finally:
        current_limiter.reset(token)

    assert len(listing_children) == 3
    assert all(child.returncode is not None for child in listing_children)
    assert peak == 2
    assert active == 0
    assert sum(len(batch.posts) for batch in batches) == 3 * len(accounts)
    feeds = {
        json.loads(post.payload)["discovery"]["feed_url"]
        for batch in batches for post in batch.posts
    }
    assert feeds == {
        f"https://www.youtube.com/{account}/videos" for account in accounts
    }


async def test_listing_429_uses_shared_cooldown(
    listing_children: list[MemoryProcess],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = RequestLimiter(ScrapeSettings())
    cooldown = Mock(wraps=limiter.cool_down)
    monkeypatch.setattr(limiter, "cool_down", cooldown)
    token = current_limiter.set(limiter)
    settings = YouTubeSettings()

    try:
        async with asyncio.timeout(5), aclosing(ListingWorker()) as worker:
            with pytest.raises(ListingFailure, match="rate limited"):
                await collect_listing(
                    describe_feed(CHANNEL.replace("example", "limited")),
                    settings, Mock(), worker=worker,
                )

            cooldown.assert_called_once_with("https://www.youtube.com", 90.0)

            with pytest.raises(ListingFailure, match="cooldown"):
                await collect_listing(
                    describe_feed(CHANNEL), settings, Mock(), worker=worker,
                )

            assert listing_children[0].returncode is None
    finally:
        current_limiter.reset(token)

    assert len(listing_children) == 1
    assert listing_children[0].returncode is not None


async def test_listing_worker_restarts_after_timeout(
    listing_children: list[MemoryProcess],
) -> None:
    settings = YouTubeSettings()
    events: list[dict[str, Any]] = []

    async with asyncio.timeout(8), aclosing(ListingWorker()) as worker:
        feed = describe_feed(CHANNEL)
        await collect_listing(feed, settings, events.append, worker=worker)
        events.clear()

        with pytest.raises(TimeoutError):
            await collect_listing(
                describe_feed(CHANNEL.replace("example", "hang")),
                settings, events.append, worker=worker, timeout_seconds=0.02,
            )

        assert [event["event"] for event in events] == [
            "renderer", "renderer", "page",
        ]
        assert listing_children[0].returncode is not None
        assert await collect_listing(
            feed, settings, events.append, worker=worker,
        ) == "listing exhausted"

    assert len(listing_children) == 2
    assert listing_children[1].returncode is not None
