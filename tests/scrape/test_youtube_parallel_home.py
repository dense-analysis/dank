from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import yt_dlp

from dank.config import YouTubeSettings
from dank.model import RawPost
from dank.scrape import youtube
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


def setup_home() -> Any:
    cookie = SimpleNamespace(domain=".youtube.com", to_json=lambda: {
        "name": "SID", "value": "synthetic-cookie", "domain": ".youtube.com",
    })

    return SimpleNamespace(send=AsyncMock(return_value=[cookie]))


async def test_recommendations_use_only_ytdlp_and_keep_source_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = setup_home()
    browser = AsyncMock(side_effect=AssertionError("Must not scroll home"))
    monkeypatch.setattr(youtube, "collect_browser_home", browser)
    result = YtDlpHomeResult((), 0.1, 0.01, "completed", renderers=(
        HomeRenderer("videoRenderer", {"videoId": "aaaaaaaaaaa"}, ""),
        HomeRenderer("reelItemRenderer", {"videoId": "bbbbbbbbbbb"},
                     "Shorts", ("Shorts",)),
    ))
    collect = AsyncMock(return_value=result)
    monkeypatch.setattr(youtube, "collect_ytdlp_home", collect)
    batches = [batch async for batch in youtube._scrape_feed(  # pyright: ignore[reportPrivateUsage]
        page, HOME, SETTINGS, set(),
    )]
    browser.assert_not_awaited()
    assert collect.await_count == 1
    assert collect.await_args is not None
    assert collect.await_args.kwargs["include_renderers"]
    assert 0 < collect.await_args.kwargs["timeout_seconds"] <= 30
    assert {post.post_id for post in batches[0].posts} == {
        "video:aaaaaaaaaaa", "video:bbbbbbbbbbb",
    }
    assert all(json.loads(post.payload)["collection_methods"] == ["yt_dlp"]
               for post in batches[0].posts)


@pytest.mark.parametrize("outcome", ["timeout", "failed", "item_limit"])
async def test_incomplete_ytdlp_keeps_partial_results_without_browser_fallback(
    monkeypatch: pytest.MonkeyPatch, outcome: Any,
) -> None:
    browser = AsyncMock(side_effect=AssertionError("Must not scroll home"))
    monkeypatch.setattr(youtube, "collect_browser_home", browser)
    result = YtDlpHomeResult((), 0.1, 0.01, outcome, renderers=(
        HomeRenderer("videoRenderer", {"videoId": "aaaaaaaaaaa"}, ""),
    ))
    monkeypatch.setattr(youtube, "collect_ytdlp_home", AsyncMock(
        return_value=result,
    ))
    saved: list[RawPost] = []

    with pytest.raises(youtube.YouTubeFeedIncomplete):
        async for batch in youtube._scrape_feed(  # pyright: ignore[reportPrivateUsage]
            setup_home(), HOME, SETTINGS, set(),
        ):
            saved.extend(batch.posts)

    browser.assert_not_awaited()
    assert [post.post_id for post in saved] == ["video:aaaaaaaaaaa"]


async def test_empty_ytdlp_results_fail_without_browser_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = AsyncMock(side_effect=AssertionError("Must not scroll home"))
    monkeypatch.setattr(youtube, "collect_browser_home", browser)
    monkeypatch.setattr(youtube, "collect_ytdlp_home", AsyncMock(
        return_value=YtDlpHomeResult((), 0.1, None, "failed"),
    ))

    with pytest.raises(youtube.YouTubeFeedError):
        async for _ in youtube._scrape_feed(  # pyright: ignore[reportPrivateUsage]
            setup_home(), HOME, SETTINGS, set(),
        ):
            pytest.fail("Empty failed capture must not produce a batch")

    browser.assert_not_awaited()


async def test_ytdlp_cancellation_without_browser_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started, closed = asyncio.Event(), asyncio.Event()

    async def method(*args: Any, **kwargs: Any) -> Any:
        started.set()

        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    monkeypatch.setattr(youtube, "collect_ytdlp_home", method)
    task = asyncio.create_task(youtube._collect_ytdlp_home(  # pyright: ignore[reportPrivateUsage]
        setup_home(), HOME, SETTINGS, [],
    ))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert closed.is_set()
