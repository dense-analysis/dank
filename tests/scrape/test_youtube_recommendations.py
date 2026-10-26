from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import BrowserScrapeSettings, YouTubeSettings
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube import (
    FeedCapture,
    YouTubeFeedError,
    _candidates,  # pyright: ignore[reportPrivateUsage]
    collect_browser_home,
)
from dank.scrape.youtube.feeds import describe_feed
from dank.scrape.youtube.payloads import YouTubeFeed
from dank.scrape.zendriver import NetworkResponse

HOME = YouTubeFeed("https://www.youtube.com/", "Recommendations", "home")
FIXTURE = (Path(__file__).parents[1]
           / "fixtures/youtube_home_recommendations.json")


def test_home_prunes_news_and_games_in_initial_dom_and_network_paths() -> None:
    state = json.loads(FIXTURE.read_text())
    posts = FeedCapture(HOME, YouTubeSettings(), set()).extract(
        _candidates(state, state["responses"], home=True), HOME.url,
    ).posts
    assert {post.post_id for post in posts} == {
        "video:normal00001", "video:shorts00001", "post:UgCommunity",
        "video:domvideo001", "video:network0001",
    }
    assert {json.loads(post.payload)["kind"] for post in posts} == {
        "video", "short", "post",
    }
    assert all(json.loads(post.payload)["discovery"]["feed_kind"] == "home"
               for post in posts)
    assert describe_feed(HOME.url).name == "Recommendations"


def test_home_exclusions_leave_news_collection_intact() -> None:
    state = json.loads(FIXTURE.read_text())
    feed = YouTubeFeed("https://www.youtube.com/feed/news_destination",
                       "News", "news")
    posts = FeedCapture(feed, YouTubeSettings(), set()).extract(
        _candidates(state, state["responses"]), feed.url,
    ).posts
    assert {"video:topnews0001", "video:breaknews01", "video:newsshort01"} <= {
        post.post_id for post in posts
    }


def test_home_keeps_recommended_news_videos_and_respects_kind_flags() -> None:
    capture = FeedCapture(HOME, YouTubeSettings(
        include_shorts=False, include_posts=False,
    ), set())
    state = json.loads(FIXTURE.read_text())
    posts = capture.extract(
        _candidates(state, state["responses"], home=True), HOME.url,
    ).posts
    assert all(json.loads(post.payload)["kind"] == "video" for post in posts)
    duplicate = {"videoId": "topnews0001", "title": {"simpleText": "News"}}
    posts = capture.extract([
        ("videoRenderer", duplicate, "Top news"),
        ("videoRenderer", duplicate, ""),
    ], HOME.url).posts
    assert len(posts) == 1
    assert json.loads(posts[0].payload)["discovery"]["shelf"] == ""


def _install_listing(
    monkeypatch: pytest.MonkeyPatch, *, state: dict[str, object] | None = None,
) -> tuple[Any, Any]:
    state = state or {
        "initial": {"videoRenderer": {"videoId": "normal00001"}},
        "at_bottom": False, "scroll_height": 2000,
    }
    monkeypatch.setattr("dank.scrape.youtube._resolve_feed", AsyncMock(
        return_value=(HOME.url, state),
    ))
    monkeypatch.setattr("dank.scrape.youtube.snapshot", AsyncMock(
        return_value=state,
    ))
    monkeypatch.setattr(
        "dank.scrape.youtube.HOME_SCROLL_PAUSE_SECONDS", (0, 0),
    )
    monkeypatch.setattr("dank.scrape.youtube.HOME_END_SECONDS", 0)
    monkeypatch.setattr("dank.scrape.youtube.HOME_STALL_SECONDS", 0)
    capture = AsyncMock()
    capture.drain.return_value = []
    monkeypatch.setattr("dank.scrape.youtube.NetworkCapture",
                        Mock(return_value=capture))

    return AsyncMock(), capture


async def test_browser_budget_preserves_collected_items(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    page, capture = _install_listing(monkeypatch)
    monkeypatch.setattr("dank.scrape.youtube.HOME_BUDGET_SECONDS", 0.03)

    async def stalled_snapshot(*args: Any) -> None:
        await asyncio.sleep(5)

    monkeypatch.setattr("dank.scrape.youtube.snapshot", stalled_snapshot)
    caplog.set_level(logging.INFO, logger="dank.scrape.youtube")
    started = asyncio.get_running_loop().time()
    batches: list[ScrapeBatch] = []

    with pytest.raises(YouTubeFeedError, match="incomplete: time budget"):
        await collect_browser_home(page, HOME, YouTubeSettings(), batches)

    assert [post.post_id for batch in batches for post in batch.posts] == [
        "video:normal00001",
    ]
    assert asyncio.get_running_loop().time() - started < 0.5
    assert "reason=time budget" in caplog.text
    capture.stop.assert_awaited_once()


async def test_budget_includes_navigation_and_reports_empty_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page, capture = _install_listing(monkeypatch)
    monkeypatch.setattr("dank.scrape.youtube.HOME_BUDGET_SECONDS", 0.02)

    async def stalled_navigation(*args: Any) -> None:
        await asyncio.sleep(5)

    monkeypatch.setattr(
        "dank.scrape.youtube._resolve_feed", stalled_navigation,
    )

    with pytest.raises(YouTubeFeedError, match="time budget"):
        await collect_browser_home(page, HOME, YouTubeSettings(), [])

    page.evaluate.assert_not_awaited()
    capture.stop.assert_awaited_once()


@pytest.mark.parametrize("continuation", [False, True])
async def test_home_ends_on_stable_bottom_and_reports_stall_separately(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, *,
    continuation: bool,
) -> None:
    page, _ = _install_listing(monkeypatch, state={
        "initial": {"videoRenderer": {"videoId": "normal00001"}},
        "at_bottom": True, "scroll_height": 2000,
        "continuation": continuation,
    })
    caplog.set_level(logging.INFO, logger="dank.scrape.youtube")
    batches: list[ScrapeBatch] = []

    try:
        await collect_browser_home(page, HOME, YouTubeSettings(), batches)
    except YouTubeFeedError as error:
        assert continuation
        assert "incomplete: repeated stall" in str(error)
    else:
        assert not continuation

    assert len(batches) == 1
    assert page.evaluate.await_count == YouTubeSettings().stall_scrolls
    reason = "repeated stall" if continuation else "end of feed"
    assert f"reason={reason}" in caplog.text


async def test_home_does_not_stop_while_excluded_responses_arrive(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    page, capture = _install_listing(monkeypatch, state={
        "initial": {"videoRenderer": {"videoId": "normal00001"}},
        "at_bottom": True, "scroll_height": 2000,
    })
    response = NetworkResponse("https://www.youtube.com/youtubei/v1/browse",
                               200, "application/json", '{"contents": []}',
                               "1", "Fetch")
    capture.drain.side_effect = [[response]] * 7 + [[]] * 10
    caplog.set_level(logging.INFO, logger="dank.scrape.youtube")
    await collect_browser_home(page, HOME, YouTubeSettings(), [])
    assert page.evaluate.await_count == 9
    assert "reason=end of feed" in caplog.text


async def test_home_retains_batch_before_later_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page, capture = _install_listing(monkeypatch)
    capture.drain.side_effect = [[], [NetworkResponse(
        "https://www.youtube.com/youtubei/v1/browse", 429,
        "application/json", "{}", "1", "Fetch",
    )]]
    batches: list[ScrapeBatch] = []

    with pytest.raises(YouTubeFeedError, match="HTTP 429"):
        await collect_browser_home(page, HOME, YouTubeSettings(), batches)

    assert [post.post_id for batch in batches for post in batch.posts] == [
        "video:normal00001",
    ]
    capture.stop.assert_awaited_once()


async def test_home_uses_timed_pass_instead_of_generic_feed_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page, _ = _install_listing(monkeypatch, state={
        "initial": [{"videoRenderer": {"videoId": f"{index:011d}"}}
                    for index in range(250)],
        "at_bottom": True, "scroll_height": 2000,
    })
    settings = YouTubeSettings(scrape=BrowserScrapeSettings(
        max_posts=1, max_scrolls=0, scroll_pause_seconds=100,
    ))
    batches: list[ScrapeBatch] = []
    await collect_browser_home(page, HOME, settings, batches)
    assert sum(len(batch.posts) for batch in batches) == 250
    assert page.evaluate.await_count > 0


def test_home_network_order_precedes_partial_dom_rendering() -> None:
    first = {"videoRenderer": {"videoId": "aaaaaaaaaaa"}}
    second = {"videoRenderer": {"videoId": "bbbbbbbbbbb"}}
    state: dict[str, object] = {"items": [{"value": second}]}
    response = {"appendContinuationItemsAction": {
        "continuationItems": [first, second],
    }}
    posts = FeedCapture(HOME, YouTubeSettings(), set()).extract(
        _candidates(state, [response], home=True), HOME.url,
    ).posts
    assert [post.post_id for post in posts] == [
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    ]


async def test_truncated_home_keeps_posts_and_marks_source_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.config import SourceConfig, load_settings
    from dank.scrape.metrics import SourceMetrics, current_source
    from dank.scrape.youtube import scrape_youtube_feeds

    page, _ = _install_listing(monkeypatch)
    from dank.scrape.youtube.ytdlp_home import YtDlpHomeResult
    from dank.scrape.youtube.ytdlp_renderers import HomeRenderer

    monkeypatch.setattr("dank.scrape.youtube.session_cookies", AsyncMock(
        return_value=[],
    ))
    monkeypatch.setattr("dank.scrape.youtube.collect_ytdlp_home", AsyncMock(
        return_value=YtDlpHomeResult((), 0.1, 0.01, "timeout", renderers=(
            HomeRenderer("videoRenderer", {"videoId": "normal00001"}, ""),
        )),
    ))
    monkeypatch.setattr(
        "dank.scrape.youtube._verify_saved_session", AsyncMock(),
    )
    fixture = tmp_path / "settings.fixture.toml"
    fixture.write_text("")
    settings = load_settings(fixture)._replace(youtube=YouTubeSettings(
        hype=False, news_tabs=(), homepage=True, authenticated=True,
        playlists=False,
    ))
    source = SourceConfig("youtube.com", ())
    statistics = SourceMetrics(0, source)
    session = AsyncMock()
    session.get_browser.return_value.get.return_value = page
    token = current_source.set(statistics)

    try:
        batches = [batch async for batch in scrape_youtube_feeds(
            settings, source, session,
        )]
        statistics.counts["posts_saved"] = sum(
            len(batch.posts) for batch in batches
        )
        statistics.collected()
    finally:
        current_source.reset(token)

    assert statistics.counts["posts_saved"] == 1
    assert statistics.counts["fetch_failures"] == 1
    assert statistics.error_type == "YouTubeFeedIncomplete"
    assert statistics.status == "partial"
