from __future__ import annotations

import asyncio
import datetime as dt
import json
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import yt_dlp

from dank.config import YouTubeSettings
from dank.model import RawPost
from dank.scrape import youtube
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube.home_merge import merge_home_posts
from dank.scrape.youtube.payloads import YouTubeFeed, normalize_renderer
from dank.scrape.youtube.ytdlp_home import YtDlpHomeResult
from dank.scrape.youtube.ytdlp_renderers import (
    HomeRenderer,
    HomeResponseCapture,
    RendererLimitReached,
    UnknownHomeContinuation,
    install_home_capture,
)

HOME = YouTubeFeed("https://www.youtube.com/", "Recommendations", "home")
SETTINGS = YouTubeSettings(authenticated=True, homepage=True)


def video(identifier: str) -> dict[str, Any]:
    return {"videoRenderer": {"videoId": identifier}}


def continuation(token: str) -> dict[str, Any]:
    return {"continuationItemRenderer": {
        "continuationEndpoint": {"continuationCommand": {"token": token}},
    }}


def raw_post(
    identifier: str, *, key: str = "videoRenderer", shelf: str = "",
) -> RawPost:
    raw = {"postId": identifier} if key == "backstagePostRenderer" else {
        "videoId": identifier,
    }
    post = normalize_renderer(
        key, raw, feed=HOME, shelf=shelf, rank=1, settings=SETTINGS,
        request_url=HOME.url, observed_at=dt.datetime.now(dt.UTC),
    )
    assert post is not None

    return post


def test_raw_capture_keeps_shorts_posts_and_excludes_whole_shelves() -> None:
    fixture = json.loads((Path(__file__).parents[1]
                          / "fixtures/youtube_home_recommendations.json")
                         .read_text())
    emitted: list[dict[str, Any]] = []
    capture = HomeResponseCapture(emitted.append, 100)
    capture.observe(fixture["initial"])
    assert {event["key"] for event in emitted} == {
        "videoRenderer", "shortsLockupViewModel", "backstagePostRenderer",
    }
    assert len(emitted) == 3
    assert emitted[1]["shelves"] == ["Shorts"]
    assert emitted[2]["raw"]["postId"] == "UgCommunity"


def test_headingless_continuations_inherit_excluded_ancestor_context() -> None:
    emitted: list[dict[str, Any]] = []
    capture = HomeResponseCapture(emitted.append, 100)
    capture.observe({"richShelfRenderer": {
        "title": {"simpleText": "Top News"}, "contents": [
            {"richShelfRenderer": {"title": {"simpleText": "Shorts"},
                                   "contents": [continuation("news")] }},
        ],
    }})
    context = capture.continuation_context("news")
    assert context.shelves == ("Top News", "Shorts")
    assert context.excluded
    capture.observe({"appendContinuationItemsAction": {"continuationItems": [
        video("news0000001"), continuation("news-next"),
    ]}}, context)
    assert not emitted
    assert capture.continuation_context("news-next").excluded
    capture.observe({"richShelfRenderer": {
        "title": {"simpleText": "Shorts"},
        "contents": [continuation("shorts")],
    }})
    capture.observe({"reelItemRenderer": {"videoId": "shorts00001"}},
                    capture.continuation_context("shorts"))
    assert emitted[0]["shelf"] == "Shorts"


def test_unknown_continuation_is_not_assumed_safe() -> None:
    capture = HomeResponseCapture(lambda _: None, 10)

    with pytest.raises(UnknownHomeContinuation):
        capture.continuation_context("unobserved")


def test_raw_capture_limit_preserves_already_emitted_renderers() -> None:
    emitted: list[dict[str, Any]] = []
    capture = HomeResponseCapture(emitted.append, 1)

    with pytest.raises(RendererLimitReached):
        capture.observe([video("aaaaaaaaaaa"), video("bbbbbbbbbbb")])

    assert len(emitted) == 1


def test_pinned_extractor_dispatch_keeps_raw_shorts_omitted_by_flat_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from yt_dlp.extractor.youtube import YoutubeTabIE  # type: ignore

    contents = [
        {"richItemRenderer": {"content": video("aaaaaaaaaaa")}},
        {"richSectionRenderer": {"content": {"richShelfRenderer": {
            "title": {"simpleText": "Shorts"}, "contents": [
                {"reelItemRenderer": {"videoId": "shorts00001"}},
            ],
        }}}},
    ]
    data: dict[str, Any] = {"contents": {
        "twoColumnBrowseResultsRenderer": {"tabs": [{"tabRenderer": {
            "selected": True, "tabIdentifier": "FEwhat_to_watch",
            "content": {"richGridRenderer": {"contents": contents}},
        }}]},
    }}
    monkeypatch.setattr(YoutubeTabIE, "_extract_data",
                        Mock(return_value=(data, {"LOGGED_IN": True})))
    events: list[dict[str, Any]] = []
    options: Any = {"quiet": True, "extract_flat": True,
                    "compat_opts": ["no-youtube-channel-redirect"]}

    with yt_dlp.YoutubeDL(options) as provider:
        install_home_capture(provider, events.append, 100)
        result: Any = provider.extract_info(
            "https://www.youtube.com/feed/recommended?gl=GB&hl=en-GB",
            download=False, process=False,
        )
        assert result is not None
        entries = list(result["entries"])

    assert [entry["id"] for entry in entries] == ["aaaaaaaaaaa"]
    assert {event["key"] for event in events} == {
        "videoRenderer", "reelItemRenderer",
    }


def test_merge_preserves_method_and_shelf_observations() -> None:
    browser = [raw_post("aaaaaaaaaaa"), raw_post("bbbbbbbbbbb")]
    ytdlp = [raw_post("aaaaaaaaaaa", shelf="For you"),
             raw_post("ccccccccccc"), raw_post("UgCommunity",
                                               key="backstagePostRenderer")]
    merged = merge_home_posts(browser, ytdlp, SETTINGS)
    assert [post.post_id for post in merged.posts] == [
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb", "video:ccccccccccc",
        "post:UgCommunity",
    ]
    payload = json.loads(merged.posts[0].payload)
    assert payload["collection_methods"] == ["browser", "yt_dlp"]
    assert [row["discovery"]["shelf"]
            for row in payload["recommendation_captures"]] == ["", "For you"]
    assert payload["recommendation_captures"][0]["renderer"] == (
        json.loads(browser[0].payload)["renderer"]
    )
    assert [json.loads(post.payload)["discovery"]["position"]
            for post in merged.posts] == [1, 2, 3, 4]
    assert "recommendation_captures" not in json.loads(browser[0].payload)


def test_merge_keeps_short_category_on_duplicate_video_identity() -> None:
    merged = merge_home_posts(
        [raw_post("aaaaaaaaaaa")],
        [raw_post("aaaaaaaaaaa", key="reelItemRenderer", shelf="Shorts")],
        SETTINGS,
    )
    assert len(merged.posts) == 1
    payload = json.loads(merged.posts[0].payload)
    assert payload["kind"] == "short"
    assert payload["discovery"]["shelf"] == "Shorts"
    assert len(payload["recommendation_captures"]) == 2


def setup_parallel(monkeypatch: pytest.MonkeyPatch) -> Any:
    cookie = SimpleNamespace(domain=".youtube.com", to_json=lambda: {
        "name": "SID", "value": "synthetic-cookie", "domain": ".youtube.com",
    })
    page = SimpleNamespace(send=AsyncMock(return_value=[cookie]))
    monkeypatch.setattr(youtube, "snapshot", AsyncMock(return_value={
        "url": HOME.url, "signed_in": True,
        "initial": video("aaaaaaaaaaa"),
    }))

    return page


async def test_parallel_methods_start_together_merge_and_reuse_loaded_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = setup_parallel(monkeypatch)
    started = [asyncio.Event(), asyncio.Event()]
    budgets: list[float] = []

    async def browser(
        page: Any, feed: Any, settings: Any, batches: list[ScrapeBatch],
        **kwargs: Any,
    ) -> None:
        started[0].set()
        await asyncio.wait_for(started[1].wait(), 1)
        budgets.append(kwargs["timeout_seconds"])
        assert kwargs["initial_state"]["signed_in"]
        batches.append(ScrapeBatch([raw_post("aaaaaaaaaaa")], []))

    async def ytdlp(cookies: Any, **kwargs: Any) -> YtDlpHomeResult:
        started[1].set()
        await asyncio.wait_for(started[0].wait(), 1)
        assert kwargs["include_renderers"]
        assert cookies[0]["value"] == "synthetic-cookie"
        budgets.append(kwargs["timeout_seconds"])

        return YtDlpHomeResult((), 0.1, 0.01, "completed", renderers=(
            HomeRenderer("videoRenderer", {"videoId": "aaaaaaaaaaa"}, ""),
            HomeRenderer("reelItemRenderer", {"videoId": "bbbbbbbbbbb"},
                         "Shorts", ("Shorts",)),
        ))

    monkeypatch.setattr(youtube, "_collect_home_feed", browser)
    monkeypatch.setattr(youtube, "collect_ytdlp_home", ytdlp)
    batches = [batch async for batch in youtube._scrape_feed(  # type: ignore
        page, HOME, SETTINGS, set(),
    )]
    assert budgets[0] == budgets[1] <= 30
    assert {post.post_id for post in batches[0].posts} == {
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    }


@pytest.mark.parametrize("failed_method", ["browser", "yt_dlp"])
async def test_one_failed_method_retains_both_partial_outputs(
    monkeypatch: pytest.MonkeyPatch, failed_method: str,
) -> None:
    page = setup_parallel(monkeypatch)

    async def browser(
        page: Any, feed: Any, settings: Any, batches: list[ScrapeBatch],
        **kwargs: Any,
    ) -> None:
        batches.append(ScrapeBatch([raw_post("aaaaaaaaaaa")], []))

        if failed_method == "browser":
            raise youtube.YouTubeFeedIncomplete("time budget")

    result = YtDlpHomeResult(
        (), 0.1, 0.01, "timeout" if failed_method == "yt_dlp" else "completed",
        renderers=(HomeRenderer("videoRenderer", {"videoId": "bbbbbbbbbbb"},
                                ""),),
    )
    monkeypatch.setattr(youtube, "_collect_home_feed", browser)
    monkeypatch.setattr(youtube, "collect_ytdlp_home", AsyncMock(
        return_value=result,
    ))
    saved: list[RawPost] = []

    with pytest.raises(youtube.YouTubeFeedIncomplete):
        async for batch in youtube._scrape_feed(  # type: ignore
            page, HOME, SETTINGS, set(),
        ):
            saved.extend(batch.posts)

    assert {post.post_id for post in saved} == {
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    }


async def test_parallel_cancellation_closes_both_collectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = setup_parallel(monkeypatch)
    started = [asyncio.Event(), asyncio.Event()]
    closed = [asyncio.Event(), asyncio.Event()]

    async def method(index: int, *args: Any, **kwargs: Any) -> Any:
        started[index].set()

        try:
            await asyncio.Event().wait()
        finally:
            closed[index].set()

    monkeypatch.setattr(youtube, "_collect_home_feed",
                        partial(method, 0))
    monkeypatch.setattr(youtube, "collect_ytdlp_home",
                        partial(method, 1))
    task = asyncio.create_task(youtube._collect_parallel_home(  # type: ignore
        page, HOME, SETTINGS, [],
    ))
    await asyncio.wait_for(asyncio.gather(*(x.wait() for x in started)), 1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert all(event.is_set() for event in closed)


async def test_deadline_keeps_browser_buffer_and_completed_ytdlp_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = setup_parallel(monkeypatch)
    monkeypatch.setattr(youtube, "HOME_BUDGET_SECONDS", 0.05)
    network = AsyncMock()
    network.drain.return_value = []
    monkeypatch.setattr(youtube, "NetworkCapture", Mock(return_value=network))

    async def delayed_scroll(*args: Any) -> None:
        await asyncio.sleep(10)

    page.evaluate = AsyncMock(side_effect=delayed_scroll)
    result = YtDlpHomeResult((), 0.01, 0.001, "completed", renderers=(
        HomeRenderer("videoRenderer", {"videoId": "bbbbbbbbbbb"}, ""),
    ))
    monkeypatch.setattr(youtube, "collect_ytdlp_home", AsyncMock(
        return_value=result,
    ))
    saved: list[RawPost] = []
    started = asyncio.get_running_loop().time()

    with pytest.raises(youtube.YouTubeFeedIncomplete):
        async for batch in youtube._scrape_feed(  # type: ignore
            page, HOME, SETTINGS, set(),
        ):
            saved.extend(batch.posts)

    assert asyncio.get_running_loop().time() - started < 0.5
    assert {post.post_id for post in saved} == {
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    }
    network.stop.assert_awaited_once()


def test_excluded_appearance_does_not_suppress_allowed_duplicate() -> None:
    emitted: list[dict[str, Any]] = []
    capture = HomeResponseCapture(emitted.append, 100)
    item = video("aaaaaaaaaaa")
    capture.observe([
        {"richShelfRenderer": {
            "title": {"simpleText": "Top News"}, "contents": [item],
        }},
        {"richShelfRenderer": {
            "endpoint": {"browseEndpoint": {
                "browseId": "FEgaming_playables",
            }}, "contents": [item],
        }},
        item,
    ])
    assert len(emitted) == 1
    assert emitted[0]["shelf"] == ""
    assert emitted[0]["raw"]["videoId"] == "aaaaaaaaaaa"


async def test_bad_ytdlp_renderer_preserves_browser_and_valid_ytdlp_posts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = setup_parallel(monkeypatch)

    async def browser(
        page: Any, feed: Any, settings: Any, batches: list[ScrapeBatch],
        **kwargs: Any,
    ) -> None:
        batches.append(ScrapeBatch([raw_post("aaaaaaaaaaa")], []))

    result = YtDlpHomeResult((), 0.1, 0.01, "completed", renderers=(
        HomeRenderer("videoRenderer", {
            "videoId": "ccccccccccc", "thumbnail": {"thumbnails": [{
                "url": "https://images.example/thumb.jpg", "width": "invalid",
            }]},
        }, ""),
        HomeRenderer("videoRenderer", {"videoId": "bbbbbbbbbbb"}, ""),
    ))
    monkeypatch.setattr(youtube, "_collect_home_feed", browser)
    monkeypatch.setattr(youtube, "collect_ytdlp_home", AsyncMock(
        return_value=result,
    ))
    saved: list[RawPost] = []

    with pytest.raises(youtube.YouTubeFeedIncomplete):
        async for batch in youtube._scrape_feed(  # type: ignore
            page, HOME, SETTINGS, set(),
        ):
            saved.extend(batch.posts)

    assert {post.post_id for post in saved} == {
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    }
