from __future__ import annotations

import asyncio
import io
import json
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import yt_dlp

from dank.config import YouTubeSettings
from dank.scrape.youtube import ytdlp_playlists
from dank.scrape.youtube.playlists import snapshot_hash
from dank.scrape.youtube.ytdlp_playlists import (
    PlaylistResponseCapture,
    _worker,  # pyright: ignore[reportPrivateUsage]
    collect_playlist,
    install_playlist_capture,
)
from dank.scrape.youtube.ytdlp_renderers import RendererLimitReached

URL = "https://www.youtube.com/playlist?list=PLexample12345"
IDENTIFIER = "PLexample12345"


def video(
    identifier: str = "aaaaaaaaaaa",
    occurrence: str = "one",
) -> dict[str, Any]:
    return {
        "videoId": identifier,
        "setVideoId": occurrence,
        "title": {"simpleText": "A video"},
        "isPlayable": True,
    }


def data(entries: list[dict[str, Any]], token: str = "") -> dict[str, Any]:
    contents: list[dict[str, Any]] = [
        {"playlistVideoRenderer": entry} for entry in entries
    ]

    if token:
        contents.append(
            {
                "continuationItemRenderer": {
                    "continuationEndpoint": {
                        "continuationCommand": {"token": token},
                    },
                },
            },
        )

    listing = {
        "playlistVideoListRenderer": {
            "playlistId": IDENTIFIER,
            "contents": contents,
            "sortFilterMenu": {
                "sortFilterSubMenuRenderer": {
                    "subMenuItems": [{"title": "Manual", "selected": True}],
                },
            },
        },
    }
    section = {
        "sectionListRenderer": {
            "contents": [
                {
                    "itemSectionRenderer": {"contents": [listing]},
                },
            ],
        },
    }

    return {
        "metadata": {"playlistMetadataRenderer": {"title": "Examples"}},
        "header": {"playlistHeaderRenderer": {"playlistId": IDENTIFIER}},
        "contents": {
            "twoColumnBrowseResultsRenderer": {
                "tabs": [
                    {
                        "tabRenderer": {"selected": True, "content": section},
                    },
                ],
            },
        },
    }


def continuation(
    entries: list[dict[str, Any]],
    token: str = "",
) -> dict[str, Any]:
    listing = data(entries, token)["contents"][
        "twoColumnBrowseResultsRenderer"
    ]["tabs"][0]["tabRenderer"]["content"]["sectionListRenderer"]["contents"][
        0
    ]["itemSectionRenderer"]["contents"][0]["playlistVideoListRenderer"]

    return {
        "onResponseReceivedActions": [
            {
                "appendContinuationItemsAction": {
                    "targetId": "playlist-video-list",
                    "continuationItems": listing["contents"],
                },
            },
        ],
    }


def test_response_capture_keeps_duplicate_positions_and_original_metadata():
    events: list[dict[str, Any]] = []
    capture = PlaylistResponseCapture(IDENTIFIER, events.append, 100)
    first = video()
    duplicate = video(occurrence="two")
    capture.initial(data([first], "next"))
    capture.page(continuation([duplicate]))
    assert capture.started and not capture.token
    assert events[0]["raw"]["sort_order"] == "Manual"
    assert (
        json.loads(events[0]["raw"]["payload"])["collection_method"]
        == "yt_dlp"
    )
    assert [event["raw"] for event in events if event["event"] == "entry"] == [
        first,
        duplicate,
    ]


def test_repeated_and_unknown_pages_cannot_claim_completion():
    capture = PlaylistResponseCapture(IDENTIFIER, lambda _: None, 100)
    capture.initial(data([video()], "next"))

    with pytest.raises(ValueError, match="Repeated"):
        capture.page(continuation([], "next"))

    with pytest.raises(ValueError, match="Unrecognized"):
        capture.page({"unexpected": []})


def test_raw_manifest_limit_is_bounded():
    events: list[dict[str, Any]] = []
    capture = PlaylistResponseCapture(IDENTIFIER, events.append, 1)

    with pytest.raises(RendererLimitReached):
        capture.initial(data([video(), video(occurrence="two")]))

    assert len([e for e in events if e["event"] == "entry"]) == 1


def test_pinned_ytdlp_extractor_drives_selected_playlist_without_video_visits(
    monkeypatch: pytest.MonkeyPatch,
):
    from yt_dlp.extractor.youtube import YoutubeTabIE  # type: ignore

    original = data([video(), video(occurrence="two")])
    section = original["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][
        0
    ]["tabRenderer"]["content"]["sectionListRenderer"]
    section["contents"].insert(0, {"itemSectionRenderer": {"contents": [{
        "playlistVideoListRenderer": {
            "playlistId": "PLunrelated", "contents": [
                {"playlistVideoRenderer": video("ccccccccccc")},
            ],
        },
    }]}})
    section["contents"].append(
        {
            "itemSectionRenderer": {
                "contents": [
                    {"videoRenderer": video("bbbbbbbbbbb")},
                    {
                        "continuationItemRenderer": {
                            "continuationEndpoint": {
                                "continuationCommand": {
                                    "token": "suggested-videos",
                                },
                            },
                        },
                    },
                ],
            },
        },
    )
    unexpected = Mock(side_effect=AssertionError("Suggested video request"))
    monkeypatch.setattr(YoutubeTabIE, "_extract_response", unexpected)
    extract = Mock(return_value=(original, {"LOGGED_IN": True}))
    monkeypatch.setattr(YoutubeTabIE, "_extract_data", extract)
    events: list[dict[str, Any]] = []
    capture = PlaylistResponseCapture(IDENTIFIER, events.append, 100)
    options: Any = {
        "quiet": True,
        "extract_flat": True,
        "compat_opts": ["no-youtube-unavailable-videos"],
    }

    with yt_dlp.YoutubeDL(options) as provider:
        install_playlist_capture(provider, capture)
        result: Any = provider.extract_info(URL, download=False, process=False)
        assert result["id"] == IDENTIFIER
        flattened = list(result["entries"])
        assert [entry["id"] for entry in flattened] == ["aaaaaaaaaaa"] * 2
        unexpected.assert_not_called()

    assert extract.call_count == 1
    assert capture.count == 2 and not capture.token
    assert events[0]["raw"]["sort_order"] == "Manual"


async def test_public_playlist_uses_no_browser_navigation_or_cookie_read(
    monkeypatch: pytest.MonkeyPatch,
):
    page = AsyncMock()

    async def worker(
        module: str,
        request: dict[str, Any],
        budget_seconds: float,
        consume: Any,
    ) -> int:
        assert request["cookies"] == []
        assert request["url"] == URL
        assert budget_seconds <= 120
        capture = PlaylistResponseCapture(IDENTIFIER, consume, 100)
        capture.initial(data([video(), video(occurrence="two")]))
        consume({"event": "done", "outcome": "completed"})

        return 0

    monkeypatch.setattr(ytdlp_playlists, "run_worker", worker)
    first = await collect_playlist(page, URL, YouTubeSettings())
    second = await collect_playlist(page, URL, YouTubeSettings())
    assert first.status == "complete" and first.exhausted
    assert [entry["position"] for entry in first.entries] == [1, 2]
    assert [entry["entry_id"] for entry in first.entries] == ["one", "two"]
    assert snapshot_hash(first.metadata, first.entries) == snapshot_hash(
        second.metadata,
        second.entries,
    )
    page.send.assert_not_awaited()
    page.evaluate.assert_not_awaited()


@pytest.mark.parametrize("failure", ["timeout", "incomplete", "warning"])
async def test_interrupted_manifest_retains_data_but_is_not_exhausted(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
):
    async def worker(
        module: str,
        request: Any,
        budget_seconds: float,
        consume: Any,
    ) -> int:
        capture = PlaylistResponseCapture(IDENTIFIER, consume, 100)
        capture.initial(data([video()], "next"))

        if failure == "timeout":
            raise TimeoutError

        if failure == "warning":
            consume({"event": "warning", "category": "repeated_continuation"})

        consume({"event": "done", "outcome": "incomplete"})

        return 0

    monkeypatch.setattr(ytdlp_playlists, "run_worker", worker)
    result = await collect_playlist(AsyncMock(), URL, YouTubeSettings())
    assert result.status == "partial" and not result.exhausted
    assert len(result.entries) == 1


async def test_cancelled_manifest_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
):
    started, closed = asyncio.Event(), asyncio.Event()

    async def worker(*args: Any) -> int:
        started.set()

        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

        return 0

    monkeypatch.setattr(ytdlp_playlists, "run_worker", worker)
    task = asyncio.create_task(
        collect_playlist(AsyncMock(), URL, YouTubeSettings()),
    )
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert closed.is_set()


def test_worker_requests_flat_metadata_only_and_redacts_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    provider = Mock()
    provider.__enter__ = Mock(return_value=provider)
    provider.__exit__ = Mock(return_value=False)
    provider.extract_info.side_effect = RuntimeError("private-session-value")
    constructor = Mock(return_value=provider)
    monkeypatch.setattr(yt_dlp, "YoutubeDL", constructor)
    monkeypatch.setattr(ytdlp_playlists, "install_playlist_capture", Mock())
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "url": URL,
                    "cookies": [],
                    "max_entries": 10000,
                    "region": "GB",
                },
            ),
        ),
    )
    _worker()
    output = capsys.readouterr().out
    assert "private-session-value" not in output
    assert json.loads(output)["error_type"] == "RuntimeError"
    options = constructor.call_args.args[0]
    assert options["extract_flat"] and options["skip_download"]
    assert options["cachedir"] is False
    assert options["compat_opts"] == ["no-youtube-unavailable-videos"]
    assert provider.extract_info.call_args.kwargs == {
        "download": False,
        "process": False,
    }


async def test_authenticated_playlist_never_degrades_to_empty_cookie_capture(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        ytdlp_playlists, "session_cookies", AsyncMock(return_value=[]),
    )
    worker = AsyncMock()
    monkeypatch.setattr(ytdlp_playlists, "run_worker", worker)
    result = await collect_playlist(
        AsyncMock(), URL, YouTubeSettings(authenticated=True),
    )
    assert result.status == "failed" and not result.exhausted
    worker.assert_not_awaited()


@pytest.mark.parametrize("with_metadata", [True, False])
async def test_only_recognized_empty_playlist_can_publish_empty_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_metadata: bool,
):
    async def worker(
        module: str, request: Any, budget_seconds: float, consume: Any,
    ) -> int:
        if with_metadata:
            PlaylistResponseCapture(IDENTIFIER, consume, 100).initial(data([]))

        consume({"event": "done", "outcome": "completed"})

        return 0

    monkeypatch.setattr(ytdlp_playlists, "run_worker", worker)
    result = await collect_playlist(AsyncMock(), URL, YouTubeSettings())
    assert result.entries == ()
    assert result.exhausted is with_metadata
    assert result.status == ("complete" if with_metadata else "failed")
