from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

from dank.config import SourceConfig, YouTubeSettings, load_settings
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.youtube import scrape_youtube_feeds, ytdlp_listings
from dank.scrape.youtube.feeds import describe_feed
from dank.scrape.youtube.playlists import playlist_capture, playlist_metadata
from dank.scrape.youtube.ytdlp_listings import (
    ListingFailure,
    _hype_content,  # pyright: ignore[reportPrivateUsage]
    _read_pages,  # pyright: ignore[reportPrivateUsage]
)

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


@pytest.mark.parametrize("repeat", [False, True])
def test_channel_worker_uses_pinned_tab_paging_and_keeps_original_items(
    monkeypatch: pytest.MonkeyPatch, *, repeat: bool,
) -> None:
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(ytdlp_listings, "emit", events.append)
    first = channel_page()
    next_video = renderer("zyxwvutsrqp")
    following = [next_video, continuation("next")] if repeat else [next_video]

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

        if repeat:
            with pytest.raises(ListingFailure, match="Repeated listing"):
                _read_pages(extractor, feed, "GB", 3)
        else:
            assert _read_pages(extractor, feed, "GB", 3) == "listing exhausted"

        initial.assert_called_once_with(CHANNEL + "?gl=GB&hl=en-GB", feed.name)
        browse.assert_called_once()
        assert browse.call_args.kwargs["query"]["continuation"] == "next"

    assert events[0]["raw"] == {"metadata": first["metadata"]}
    items = [event["raw"] for event in events if event["event"] == "renderer"]
    assert items == [
        renderer(VIDEO)["videoRenderer"], next_video["videoRenderer"],
    ]


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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, authenticated: bool,
) -> None:
    fixture = tmp_path / "settings.toml"
    fixture.touch()
    settings = load_settings(fixture)._replace(
        data_dir=tmp_path,
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
        module: str, request: dict[str, Any], budget: float, receive: Any,
    ) -> int:
        feed = request["feed"]
        feed_kinds.append(feed["kind"])
        assert request["cookies"] == (cookies if authenticated else [])
        receive({"event": "listing", "url": feed["url"], "title": "",
                 "raw": {"metadata": channel_page()["metadata"]}})
        receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                 "raw": renderer(VIDEO)["videoRenderer"]})
        receive({"event": "page"})
        receive({"event": "done", "reason": "listing exhausted"})

        return 0

    playlist = AsyncMock(return_value=playlist_capture(
        "PLtest", playlist_metadata("PLtest", {}), (), status="complete",
    ))
    monkeypatch.setattr(ytdlp_listings, "run_worker", worker)
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
            tmp_path / "youtube-browser-profile/youtube-session.json",
        )
    else:
        load.assert_not_awaited()


async def test_channel_yields_ready_pages_and_cancels_the_worker_on_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.scrape.youtube import (
        _scrape_feed,  # pyright: ignore[reportPrivateUsage]
    )

    stopped = asyncio.Event()

    async def worker(
        module: str, request: dict[str, Any], budget: float, receive: Any,
    ) -> int:
        receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                 "raw": renderer(VIDEO)["videoRenderer"]})
        receive({"event": "page"})

        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

        return 0

    monkeypatch.setattr(ytdlp_listings, "run_worker", worker)
    batches = _scrape_feed(
        None, describe_feed(CHANNEL), YouTubeSettings(), set(),
    )

    try:
        async with asyncio.timeout(1):
            batch = await anext(batches)

        assert [post.post_id for post in batch.posts] == [f"video:{VIDEO}"]
    finally:
        await batches.aclose()

    assert stopped.is_set()


@pytest.mark.parametrize("timed_out", [False, True])
async def test_channel_overlap_and_timeout_keep_posts_without_browser_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, timed_out: bool,
) -> None:
    fixture = tmp_path / "settings.toml"
    fixture.touch()
    settings = load_settings(fixture)._replace(youtube=YouTubeSettings(
        hype=False, news_tabs=(), playlists=False, known_overlap=1,
    ))
    source = SourceConfig("youtube.com", accounts=("@example",))
    stats = SourceMetrics(0, source)
    session = AsyncMock()

    async def worker(
        module: str, request: dict[str, Any], budget: float, receive: Any,
    ) -> int:
        receive({"event": "renderer", "key": "videoRenderer", "shelf": "",
                 "raw": renderer(VIDEO)["videoRenderer"]})

        if timed_out:
            raise TimeoutError

        pytest.fail("The known overlap must stop the worker")

    monkeypatch.setattr(ytdlp_listings, "run_worker", worker)
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
