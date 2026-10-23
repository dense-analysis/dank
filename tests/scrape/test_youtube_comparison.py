from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import load_settings
from dank.model import RawPost
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube import (
    YouTubeFeedError,
    YouTubeFeedIncomplete,
    comparison,
)
from dank.scrape.youtube.ytdlp_home import YtDlpHomeEntry, YtDlpHomeResult
from dank.scrape.zendriver import BrowserConfig

VIDEO_A = "video:abcdefghijk"
VIDEO_B = "video:lmnopqrstuv"
VIDEO_C = "video:zyxwvutsrqp"


def entry(identity: str, kind: str = "video", **raw: Any) -> dict[str, Any]:
    return {"identity": identity, "kind": kind, "position": 1, "raw": raw}


def test_comparison_counts_unique_ids_and_unions_metadata_across_appearances():
    browser = [
        entry(VIDEO_A, title="A"),
        entry(VIDEO_A, "short", channel_name="Channel"),
        entry(
            VIDEO_B,
            thumbnail_url="https://i.ytimg.com/a.jpg",
            renderer={"videoRenderer": {}},
            discovery={"shelf": "For you"},
        ),
        entry("post:UgkxPost", "post", text="Community update"),
        entry("", "unknown", title="Unidentified recommendation"),
    ]
    ytdlp = [
        entry(VIDEO_A, title="A", channel="Channel"),
        entry(
            VIDEO_C,
            title="C",
            thumbnails=[{"url": "https://i.ytimg.com/c.jpg"}],
        ),
        entry(VIDEO_C, title="C"),
    ]
    result = comparison.compare_entries(browser, ytdlp)
    summary = result["browser"]
    assert summary["raw_entries"] == 5
    assert summary["unique_items"] == 3
    assert summary["unidentified_entries"] == 1
    assert summary["duplicate_appearances"] == 1
    assert summary["counts"] == {"video": 1, "short": 1, "post": 1}
    assert summary["kind_conflict_identities"] == [VIDEO_A]
    assert summary["metadata_coverage"] == {
        "denominator": 3,
        "available_counts": {
            "title": 1,
            "channel": 1,
            "thumbnail": 1,
            "shelf_label": 1,
            "source_renderer": 1,
            "post_text": 1,
        },
    }
    assert result["yt_dlp"]["unique_items"] == 2
    assert (
        result["yt_dlp"]["metadata_coverage"]["available_counts"][
            "source_renderer"
        ]
        == 0
    )
    assert result["video_overlap"] == {
        "common_count": 1,
        "union_count": 3,
        "jaccard": 1 / 3,
        "common": [VIDEO_A],
        "browser_only": [VIDEO_B],
        "yt_dlp_only": [VIDEO_C],
        "browser_only_count": 1,
        "yt_dlp_only_count": 1,
    }
    json.dumps(result, allow_nan=False)


def test_empty_comparison_does_not_claim_perfect_overlap():
    result = comparison.compare_entries([], [])
    assert result["video_overlap"]["jaccard"] is None
    assert result["video_overlap"]["union_count"] == 0


def raw_post() -> RawPost:
    return RawPost(
        "youtube.com",
        VIDEO_A,
        "https://www.youtube.com/watch?v=abcdefghijk",
        None,
        dt.datetime.now(dt.UTC),
        "youtube",
        "https://www.youtube.com/",
        json.dumps(
            {
                "kind": "short",
                "title": "A",
                "video_id": "abcdefghijk",
                "renderer": {"videoRenderer": {"videoId": "abcdefghijk"}},
            },
        ),
    )


def setup_browser(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
    )
    cookie_values = [
        {"domain": ".youtube.com", "name": "SID", "value": "private-cookie"},
        {"domain": ".google.com", "name": "SID", "value": "google-private"},
        {
            "domain": "youtube.com.evil.example",
            "name": "SID",
            "value": "unrelated",
        },
    ]
    cookies = [
        SimpleNamespace(
            domain=value["domain"], to_json=Mock(return_value=value),
        )
        for value in cookie_values
    ]
    auth = SimpleNamespace(
        evaluate=AsyncMock(return_value=True),
        send=AsyncMock(return_value=cookies),
        close=AsyncMock(),
    )
    listing = SimpleNamespace()
    browser = SimpleNamespace(get=AsyncMock(side_effect=[auth, listing]))
    session = SimpleNamespace(get_browser=AsyncMock(return_value=browser))
    closed = Mock()
    configurations: list[BrowserConfig] = []

    class Session:
        def __init__(self, config: Any):
            configurations.append(config)

        async def __aenter__(self):
            return session

        async def __aexit__(self, *args: Any):
            closed()

    monkeypatch.setattr(comparison, "BrowserSession", Session)
    monkeypatch.setattr(comparison, "navigate", AsyncMock())

    return SimpleNamespace(
        settings=settings,
        auth=auth,
        listing=listing,
        browser=browser,
        closed=closed,
        configurations=configurations,
    )


def ytdlp_result(outcome: str = "completed") -> YtDlpHomeResult:
    return YtDlpHomeResult(
        entries=(
            YtDlpHomeEntry(
                VIDEO_A, "video", 1, {"id": "abcdefghijk", "title": "A"},
            ),
        ),
        elapsed_seconds=0.01,
        first_entry_seconds=0.001,
        outcome=outcome,  # pyright: ignore[reportArgumentType]
    )


async def test_pair_shares_auth_budget_and_keeps_partial_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    fixture = setup_browser(monkeypatch, tmp_path)
    browser_started, ytdlp_started = asyncio.Event(), asyncio.Event()

    async def collect_browser(
        page: Any,
        feed: Any,
        settings: Any,
        batches: list[ScrapeBatch],
        **kwargs: Any,
    ):
        assert page is fixture.listing
        assert feed.kind == "home"
        assert (
            settings.authenticated
            and settings.include_posts
            and settings.include_shorts
        )
        assert kwargs == {
            "timeout_seconds": 240,
            "max_items": 10000,
            "max_scrolls": 2000,
        }
        browser_started.set()
        await ytdlp_started.wait()
        batches.append(ScrapeBatch([raw_post()], []))
        raise YouTubeFeedIncomplete(
            "Recommendations incomplete: time budget; retained 1 items",
        )

    async def collect_ytdlp(cookies: Any, **kwargs: Any):
        assert fixture.auth.evaluate.await_count == 1
        assert {cookie["domain"] for cookie in cookies} == {
            ".youtube.com",
            ".google.com",
        }
        assert kwargs == {
            "timeout_seconds": 240,
            "region": fixture.settings.youtube.region,
        }
        ytdlp_started.set()
        await browser_started.wait()

        return ytdlp_result()

    monkeypatch.setattr(comparison, "_collect_home_feed", collect_browser)
    monkeypatch.setattr(comparison, "collect_ytdlp_home", collect_ytdlp)
    report = await asyncio.wait_for(
        comparison.compare_home(fixture.settings), 2,
    )
    assert report["browser"]["outcome"] == "timeout"
    assert report["browser"]["partial"] is True
    assert report["browser"]["first_entry_seconds"] is not None
    assert report["yt_dlp"]["outcome"] == "completed"
    assert report["comparison"]["video_overlap"]["common_count"] == 1
    assert report["method_start_skew_seconds"] < 0.5
    assert report["session"]["authenticated_before_listing"] is True
    assert report["warmup"]["elapsed_seconds"] >= 0
    assert (
        report["total_elapsed_seconds"] >= report["warmup"]["elapsed_seconds"]
    )
    serialized = json.dumps(report, allow_nan=False)
    assert all(
        secret not in serialized
        for secret in ("private-cookie", "google-private", "unrelated")
    )
    assert "cookie_values" not in serialized
    fixture.auth.close.assert_awaited_once()
    fixture.closed.assert_called_once()
    assert (
        fixture.configurations[0].profile_dir
        == tmp_path / "youtube-browser-profile"
    )
    assert fixture.configurations[0].headless is True
    assert fixture.browser.get.call_count == 2


async def test_signed_out_stops_before_collection_and_cookies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    fixture = setup_browser(monkeypatch, tmp_path)
    fixture.auth.evaluate.return_value = False
    browser, ytdlp = AsyncMock(), AsyncMock()
    monkeypatch.setattr(comparison, "_collect_home_feed", browser)
    monkeypatch.setattr(comparison, "collect_ytdlp_home", ytdlp)

    with pytest.raises(YouTubeFeedError, match="Sign in"):
        await comparison.compare_home(fixture.settings)

    browser.assert_not_awaited()
    ytdlp.assert_not_awaited()
    fixture.auth.send.assert_not_awaited()
    fixture.closed.assert_called_once()


async def test_error_keeps_peer_running_and_hides_private_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    fixture = setup_browser(monkeypatch, tmp_path)
    monkeypatch.setattr(
        comparison,
        "_collect_home_feed",
        AsyncMock(
            side_effect=RuntimeError("private-cookie error text"),
        ),
    )
    peer = AsyncMock(return_value=ytdlp_result())
    monkeypatch.setattr(comparison, "collect_ytdlp_home", peer)
    report = await comparison.compare_home(fixture.settings, 30)
    assert report["browser"]["error_type"] == "RuntimeError"
    assert report["yt_dlp"]["outcome"] == "completed"
    assert "private-cookie" not in json.dumps(report)
    peer.assert_awaited_once()
    assert peer.call_args.kwargs["timeout_seconds"] == 30


async def test_cancellation_stops_both_collectors_and_releases_browser_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    fixture = setup_browser(monkeypatch, tmp_path)
    started = [asyncio.Event(), asyncio.Event()]
    stopped = [asyncio.Event(), asyncio.Event()]

    async def wait(index: int):
        started[index].set()

        try:
            await asyncio.Event().wait()
        finally:
            stopped[index].set()

    async def browser_wait(*args: Any, **kwargs: Any):
        await wait(0)

    async def ytdlp_wait(*args: Any, **kwargs: Any):
        await wait(1)

    monkeypatch.setattr(comparison, "_collect_home_feed", browser_wait)
    monkeypatch.setattr(comparison, "collect_ytdlp_home", ytdlp_wait)
    task = asyncio.create_task(comparison.compare_home(fixture.settings))
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in started)), 2,
    )
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert all(event.is_set() for event in stopped)
    fixture.closed.assert_called_once()


@pytest.mark.parametrize("budget", [0, -1, 241, float("inf"), float("nan")])
async def test_invalid_budget_never_opens_profile(
    budget: float, monkeypatch: pytest.MonkeyPatch,
):
    session = Mock()
    monkeypatch.setattr(comparison, "BrowserSession", session)

    with pytest.raises(ValueError, match="at most 240"):
        await comparison.compare_home(
            load_settings("config.native.example.toml"), budget,
        )

    session.assert_not_called()
