from __future__ import annotations

import asyncio
import datetime
import html
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from dank.config import (
    RedditCommentSort,
    RedditSettings,
    ScrapeSettings,
    SourceConfig,
)
from dank.model import RawPost
from dank.scrape.http import HostCooldownError, RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.reddit import (
    _capture_batch_details,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.reddit.details import (
    capture_post_detail,
    defer_post_detail,
    extract_thread,
    thread_url,
)
from dank.scrape.reddit.html import extract_html_posts
from dank.scrape.reddit.payloads import extract_json_posts
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import NetworkResponse

URL = "https://www.reddit.com/r/python/comments/abc/post/"
POST: dict[str, object] = {
    "id": "abc",
    "name": "t3_abc",
    "title": "Post",
    "subreddit": "python",
    "permalink": URL,
    "selftext": "Full post text",
    "num_comments": 4,
}
CARD = (
    '<shreddit-post id="t3_abc" post-title="Post" author="author" '
    'permalink="/r/python/comments/abc/post/" comment-count="4">'
    '<div slot="text-body"><p>Full post text</p></div></shreddit-post>'
)


def _comment(
    identifier: str, parent: str = "t3_abc", **extra: object,
) -> dict[str, object]:
    return {
        "kind": "t1",
        "data": {
            "id": identifier,
            "name": f"t1_{identifier}",
            "parent_id": parent,
            "link_id": "t3_abc",
            "author": "person",
            "body": "A reply",
            "body_html": "<p>A reply</p>",
            "score": 2,
            "created_utc": 1700000000,
            "permalink": f"/r/python/comments/abc/post/{identifier}/",
            **extra,
        },
    }


def _thread(
    *comments: dict[str, object], post: dict[str, object] = POST,
) -> list[object]:
    return [
        {
            "kind": "Listing",
            "data": {"children": [{"kind": "t3", "data": post}]},
        },
        {"kind": "Listing", "data": {"children": list(comments)}},
    ]


def _raw() -> RawPost:
    return RawPost(
        "reddit.com",
        "t3_abc",
        URL,
        None,
        datetime.datetime.now(datetime.UTC),
        "reddit",
        URL,
        json.dumps({**POST, "id": "t3_abc", "selftext": "short"}),
    )


def test_thread_preserves_order_and_caps_total_including_nested_replies() -> (
    None
):
    payload = _thread(
        _comment(
            "first",
            replies={
                "data": {
                    "children": [
                        _comment("reply", "t1_first", is_submitter=True),
                    ],
                },
            },
        ),
        _comment("second"),
        {"kind": "more", "data": {"count": 9}},
    )
    result = extract_thread(
        payload, "t3_abc", "python", RedditSettings(comment_limit=2),
    )
    assert result is not None
    capture = cast(dict[str, Any], result["comment_capture"])
    assert capture["status"] == "captured"
    assert capture["sort"] == "best"
    assert capture["limit"] == 2
    assert capture["has_more"] is True
    assert capture["total_available"] == 4
    assert [item["id"] for item in capture["comments"]] == [
        "t1_first",
        "t1_reply",
    ]
    assert capture["comments"][1]["depth"] == 1
    assert capture["comments"][1]["parent_id"] == "t1_first"
    assert capture["comments"][1]["is_op"] is True
    assert result["raw"] == {"thread": payload}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        [POST],
        _thread(post={**POST, "name": "t3_wrong"}),
        [{"data": {"children": [{"kind": "t3", "data": POST}]}}, {"data": {}}],
    ],
)
def test_malformed_or_wrong_post_is_never_a_successful_empty_thread(
    payload: object,
) -> None:
    assert (
        extract_thread(payload, "t3_abc", "python", RedditSettings()) is None
    )


def test_wrong_comment_owner_parent_and_duplicates_are_excluded() -> None:
    payload = _thread(
        _comment("wrong", link_id="t3_other"),
        _comment("orphan", parent="t1_missing"),
        _comment("good"),
        _comment("good"),
    )
    result = extract_thread(payload, "t3_abc", "python", RedditSettings())
    assert result is not None
    capture = cast(dict[str, Any], result["comment_capture"])
    assert [item["id"] for item in capture["comments"]] == ["t1_good"]
    assert capture["has_more"] is True


def test_successful_empty_thread_and_more_placeholder_are_distinct() -> None:
    for more in (False, True):
        payload = _thread(
            *([cast(dict[str, object], {
                "kind": "more", "data": {"count": 2},
            })] if more else []),
            post={**POST, "num_comments": 0},
        )
        result = extract_thread(payload, "t3_abc", "python", RedditSettings())
        assert result is not None
        capture = cast(dict[str, Any], result["comment_capture"])
        assert capture["status"] == "captured"
        assert capture["has_more"] is more
        assert capture["comments"] == []


@pytest.mark.parametrize("sort", list(RedditCommentSort))
def test_comments_request_uses_selected_reddit_sort(
    sort: RedditCommentSort,
) -> None:
    url = thread_url(POST, RedditSettings(comment_sort=sort, comment_limit=7))
    expected = "confidence" if sort is RedditCommentSort.BEST else sort.value
    assert f"sort={expected}" in url
    assert "limit=7" in url


async def test_browser_detail_uses_thread_response_without_extra_fetch() -> (
    None
):
    payload = _thread(_comment("first"))
    page = AsyncMock()

    async def navigate(*args: Any) -> None:
        assert page.activate.await_count == 1

    page.send.side_effect = navigate
    response = NetworkResponse(
        URL, 200, "application/json", json.dumps(payload), "id", "Fetch",
    )
    capture = AsyncMock()
    capture.drain.return_value = [response]

    with (
        patch(
            "dank.scrape.reddit.details.NetworkCapture", return_value=capture,
        ),
        patch("dank.scrape.reddit.details.asyncio.sleep"),
    ):
        result = json.loads(
            (await capture_post_detail(
                page, _raw(), RedditSettings(),
            )).payload,
        )

    assert result["detail_capture"]["status"] == "captured"
    assert result["selftext"] == "Full post text"
    assert result["comment_capture"]["status"] == "captured"
    assert len(result["comment_capture"]["comments"]) == 1
    page.evaluate.assert_not_called()
    page.activate.assert_awaited_once()
    capture.stop.assert_awaited_once()


@pytest.mark.parametrize("stage", ["document", "json"])
async def test_rate_limited_details_keep_post_and_apply_host_cooldown(
    stage: str, caplog: pytest.LogCaptureFixture,
) -> None:
    page = AsyncMock()
    page.evaluate.return_value = json.dumps({
        "status": 429, "retry_after": "120", "body": "",
    })
    capture = AsyncMock()
    capture.drain.return_value = [NetworkResponse(
        URL, 429, "text/html", "", "limited", "Document", "120",
    )] if stage == "document" else []
    limiter = RequestLimiter(ScrapeSettings())
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    limit_token = current_limiter.set(limiter)
    stats_token = current_source.set(stats)

    try:
        with (
            patch("dank.scrape.reddit.details.NetworkCapture",
                  return_value=capture),
            patch("dank.scrape.reddit.details.asyncio.sleep"),
        ):
            result = json.loads((await capture_post_detail(
                page, _raw(), RedditSettings(),
            )).payload)

        with pytest.raises(HostCooldownError):
            async with limiter.slot(URL):
                pytest.fail("Detail requested during Reddit cooldown")
    finally:
        current_limiter.reset(limit_token)
        current_source.reset(stats_token)

    assert result["selftext"] == "short"
    assert result["detail_capture"]["status"] == "failed"
    assert result["detail_capture"]["reason"] == "rate_limited"
    assert stats.counts["fetch_failures"] == stats.counts["http_429"] == 1
    assert "HTTP 429" in caplog.text
    assert page.evaluate.await_count == (0 if stage == "document" else 1)


@pytest.mark.parametrize("comments", [False, True])
async def test_rate_limit_defers_batch_details_without_losing_listings(
    monkeypatch: pytest.MonkeyPatch, *, comments: bool,
) -> None:
    settings = RedditSettings(capture_comments=comments)
    posts = [_raw()._replace(post_id=f"t3_{i}") for i in range(4)]
    originals = [post.payload for post in posts]
    visit = AsyncMock(return_value=defer_post_detail(posts[0], settings))
    monkeypatch.setattr("dank.scrape.reddit.capture_post_detail", visit)
    result = await _capture_batch_details(
        ScrapeBatch(posts, []), AsyncMock(), settings,
    )

    visit.assert_awaited_once()
    assert [post.post_id for post in result.posts] == [
        post.post_id for post in posts
    ]
    assert [post.payload for post in posts] == originals

    for post in result.posts:
        payload = json.loads(post.payload)
        assert payload["selftext"] == "short"
        assert payload["detail_capture"]["reason"] == "rate_limited"
        assert payload["comment_capture"]["status"] == (
            "failed" if comments else "disabled"
        )


async def test_successful_json_fallback_preserves_thread_extraction() -> None:
    page = AsyncMock()
    page.evaluate.return_value = json.dumps({
        "status": 200, "retry_after": None,
        "body": json.dumps(_thread(_comment("first"))),
    })
    capture = AsyncMock()
    capture.drain.return_value = []

    with (
        patch(
            "dank.scrape.reddit.details.NetworkCapture", return_value=capture,
        ),
        patch("dank.scrape.reddit.details.asyncio.sleep"),
    ):
        result = json.loads((await capture_post_detail(
            page, _raw(), RedditSettings(),
        )).payload)

    assert result["detail_capture"]["status"] == "captured"
    assert result["comment_capture"]["comments"][0]["id"] == "t1_first"
    page.evaluate.assert_awaited_once()


async def test_host_cooldown_does_not_consume_the_detail_page_budget() -> None:
    page = AsyncMock()
    capture = AsyncMock()
    capture.drain.return_value = [NetworkResponse(
        URL, 200, "application/json", json.dumps(_thread(_comment("first"))),
        "id", "Fetch",
    )]

    @asynccontextmanager
    async def slot(url: str) -> AsyncGenerator[None]:
        ready = asyncio.Event()
        timer = asyncio.get_running_loop().call_later(0.1, ready.set)

        try:
            await ready.wait()
            yield
        finally:
            timer.cancel()

    token = current_limiter.set(cast(Any, SimpleNamespace(slot=slot)))

    try:
        with (
            patch("dank.scrape.reddit.details.DETAIL_TIMEOUT_SECONDS", 0.05),
            patch("dank.scrape.reddit.details.NetworkCapture",
                  return_value=capture),
            patch("dank.scrape.reddit.details.asyncio.sleep"),
        ):
            result = json.loads((await capture_post_detail(
                page, _raw(), RedditSettings(),
            )).payload)
    finally:
        current_limiter.reset(token)

    assert result["detail_capture"]["status"] == "captured"


@pytest.mark.parametrize("disabled", [False, True])
async def test_browser_failure_keeps_listing_and_marks_capture_state(
    *, disabled: bool,
) -> None:
    page = AsyncMock()
    page.send.side_effect = TimeoutError
    capture = AsyncMock()

    with patch(
        "dank.scrape.reddit.details.NetworkCapture", return_value=capture,
    ):
        result = json.loads(
            (
                await capture_post_detail(
                    page,
                    _raw(),
                    RedditSettings(capture_comments=not disabled),
                )
            ).payload,
        )

    assert result["selftext"] == "short"
    assert result["detail_capture"]["status"] == "failed"
    assert result["comment_capture"]["status"] == (
        "disabled" if disabled else "failed"
    )
    assert result["comment_capture"]["has_more"] is True
    capture.stop.assert_awaited_once()


async def test_disabled_comments_keep_body_without_comment_request() -> (
    None
):
    page = AsyncMock()
    page.evaluate.return_value = json.dumps(
        {"url": URL, "html": CARD, "comments": []},
    )
    capture = AsyncMock()
    capture.drain.return_value = []

    with (
        patch(
            "dank.scrape.reddit.details.NetworkCapture", return_value=capture,
        ),
        patch("dank.scrape.reddit.details.asyncio.sleep"),
    ):
        result = json.loads(
            (
                await capture_post_detail(
                    page,
                    _raw(),
                    RedditSettings(capture_comments=False),
                )
            ).payload,
        )

    assert result["selftext_html"] == "<p>Full post text</p>"
    assert result["comment_capture"]["status"] == "disabled"
    assert result["comment_capture"]["comments"] == []
    assert all(
        "fetch(" not in str(call) for call in page.evaluate.call_args_list
    )


async def test_json_denied_uses_rendered_comments_and_original_html() -> (
    None
):
    comments = [cast(dict[str, object], _comment("first")["data"])]
    snapshot = json.dumps(
        {
            "url": URL,
            "html": CARD,
            "comments": comments,
            "comments_html": "<shreddit-comment>original</shreddit-comment>",
            "has_more": True,
        },
    )
    page = AsyncMock()
    def evaluate(script: str, **_kwargs: object) -> str:
        if "fetch(" in script:
            return json.dumps({"status": 403, "retry_after": None, "body": ""})

        return snapshot

    page.evaluate.side_effect = evaluate
    capture = AsyncMock()
    capture.drain.return_value = []

    with (
        patch(
            "dank.scrape.reddit.details.NetworkCapture", return_value=capture,
        ),
        patch("dank.scrape.reddit.details.asyncio.sleep"),
    ):
        result = json.loads(
            (
                await capture_post_detail(
                    page,
                    _raw(),
                    RedditSettings(comment_limit=1),
                )
            ).payload,
        )

    assert result["comment_capture"]["status"] == "captured"
    assert result["comment_capture"]["comments"][0]["id"] == "t1_first"
    assert result["comment_capture"]["has_more"] is True
    assert "original" in result["raw"]["detail"]["comments_html"]


def test_native_video_preserves_download_identity_and_playback_metadata() -> (
    None
):
    native = {
        **POST,
        "secure_media": {
            "reddit_video": {
                "hls_url": "https://v.redd.it/video/HLSPlaylist.m3u8",
                "fallback_url": "https://v.redd.it/video/DASH_720.mp4",
            },
        },
        "preview": {
            "images": [
                {"source": {"url": "https://preview.redd.it/poster.jpg"}},
            ],
        },
    }
    post = extract_json_posts(native, "python")[0]
    media = cast(list[dict[str, str]], post["media"])
    video = next(item for item in media if item["asset_type"] == "video")
    assert video["url"] == video["hls_url"]
    assert video["playback_url"].endswith("DASH_720.mp4")
    assert video["poster_url"].endswith("poster.jpg")


def test_live_player_preserves_muxed_mp4_and_poster() -> (
    None
):
    packaged = html.escape(
        json.dumps(
            {
                "playbackMp4s": {
                    "permutations": [
                        {
                            "source": {
                                "url": "https://packaged-media.redd.it/video/play.mp4",
                            },
                        },
                    ],
                },
            },
        ),
        quote=True,
    )
    player = (
        '<shreddit-player src="https://v.redd.it/video/HLSPlaylist.m3u8" '
        'poster="https://preview.redd.it/poster.jpg" '
        f'packaged-media-json="{packaged}">'
        '<source src="https://v.redd.it/video/HLSPlaylist.m3u8">'
        '</shreddit-player>'
    )
    post = extract_html_posts(
        CARD.replace("</shreddit-post>", player + "</shreddit-post>"),
        "python",
    )[0]
    video = cast(list[dict[str, str]], post["media"])[0]
    assert video["url"] == video["hls_url"]
    assert (
        video["playback_url"]
        == "https://packaged-media.redd.it/video/play.mp4"
    )
    assert video["poster_url"] == "https://preview.redd.it/poster.jpg"


def test_crosspost_inherits_video_but_keeps_its_identity() -> None:
    post = extract_json_posts({
        **POST,
        "crosspost_parent_list": [{
            "id": "parent", "title": "Parent title", "num_comments": 999,
            "secure_media": {"reddit_video": {
                "hls_url": "https://v.redd.it/parent/HLSPlaylist.m3u8",
            }},
        }],
    }, "python")[0]
    assert post["id"] == "t3_abc"
    assert post["title"] == "Post"
    assert post["num_comments"] == 4
    assert cast(list[dict[str, str]], post["media"])[0]["hls_url"] == (
        "https://v.redd.it/parent/HLSPlaylist.m3u8"
    )


async def test_missing_detail_tab_preserves_listing() -> None:
    result = json.loads((await capture_post_detail(
        None, _raw(), RedditSettings(),
    )).payload)
    assert result["selftext"] == "short"
    assert result["detail_capture"]["status"] == "failed"
    assert result["comment_capture"]["status"] == "failed"
