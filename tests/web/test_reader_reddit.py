from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
from aiohttp import web
from test_reader import FakeClickHouse, request, row

from dank.config import load_settings
from dank.web.app import create_app
from dank.web.reader_reddit import comment_capture, videos


@pytest.fixture
def app(tmp_path: pathlib.Path) -> web.Application:
    settings = load_settings("config.example.toml")._replace(data_dir=tmp_path)

    return create_app(settings, page_size=50)


PERMALINK = "https://www.reddit.com/r/python/comments/abc/title/"
VIDEO = "https://v.redd.it/abc/HLSPlaylist.m3u8"
POSTER = "https://preview.redd.it/poster.jpg"


@pytest.mark.parametrize("total", [None, 0, 25])
def test_historical_capture_is_not_mistaken_for_empty(total: int | None):
    capture = comment_capture({"num_comments": total}, PERMALINK)
    assert capture["status"] == "not_captured"
    assert capture["total_available"] == total
    assert capture["comments"] == []


@pytest.mark.parametrize("status", ["disabled", "failed"])
def test_failed_and_disabled_captures_do_not_expose_partial_comments(
    status: str,
):
    capture = comment_capture(
        {
            "comment_capture": {
                "status": status,
                "comments": [{"id": "a", "body": "Partial"}],
            },
        },
        PERMALINK,
    )
    assert capture["status"] == status
    assert capture["comments"] == []


def test_comment_html_is_sanitized_and_partial_archives_are_labelled():
    capture = comment_capture(
        {
            "num_comments": 40,
            "comment_capture": {
                "status": "captured",
                "sort": "new",
                "limit": 50,
                "comments": [
                    {
                        "id": "t1_a",
                        "parent_id": "t3_abc",
                        "author": "Alice",
                        "body_html": '&lt;p&gt;Hi '
                        '&lt;a href="/r/python"&gt;there&lt;/a&gt;'
                        '&lt;img src="/a.jpg" onerror="alert(1)"&gt;'
                        "&lt;script&gt;alert(1)&lt;/script&gt;&lt;/p&gt;",
                        "score": -2,
                        "depth": 0,
                        "created_utc": 1700000000,
                        "permalink": "/r/python/comments/abc/title/a/",
                        "is_op": True,
                    },
                    {
                        "id": "t1_b",
                        "parent_id": "t1_a",
                        "body": "<literal>\nReply",
                        "depth": 1,
                        "score": True,
                        "created_utc": float("inf"),
                        "permalink": "javascript:alert(1)",
                    },
                ],
            },
        },
        PERMALINK,
    )
    assert capture["has_more"] is True
    assert capture["sort"] == "new"
    first, reply = capture["comments"]
    assert 'href="https://www.reddit.com/r/python"' in first["body_html"]
    assert all(
        value not in first["body_html"] for value in ("script", "onerror")
    )
    assert first["score"] == -2
    assert first["is_op"] is True
    assert reply["body_html"] == "<p>&lt;literal&gt;<br>Reply</p>"
    assert reply["score"] is None and reply["created_utc"] is None
    assert reply["permalink"] == PERMALINK
    assert reply["depth"] == 1


def test_capture_cap_duplicate_ids_and_untrusted_options():
    capture = comment_capture(
        {
            "comment_capture": {
                "status": "captured",
                "sort": "injected",
                "limit": 9000,
                "comments": [{"id": "a", "depth": 100}] * 510,
            },
        },
        PERMALINK,
    )
    assert capture["limit"] == 500
    assert capture["sort"] == "best"
    assert len(capture["comments"]) == 1
    assert capture["has_more"] is True
    assert capture["comments"][0]["depth"] == 20


def test_video_keeps_audio_capable_hls_and_uses_verified_local_asset():
    snapshot = {
        "media": [
            {
                "asset_type": "video",
                "url": VIDEO,
                "hls_url": VIDEO,
                "playback_url": "https://v.redd.it/abc/DASH_720.mp4",
                "poster_url": POSTER,
            },
        ],
    }
    remote = videos(snapshot, {})[0]
    assert remote["remote"] is True
    assert remote["url"] == VIDEO
    assert remote["hls_url"] == VIDEO
    archived = videos(
        snapshot, {VIDEO: "/assets/clip.mp4", POSTER: "/assets/poster.jpg"},
    )[0]
    assert archived == {
        "url": "/assets/clip.mp4",
        "poster_url": "/assets/poster.jpg",
        "remote": False,
        "hls_url": None,
        "fallback_url": None,
    }


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "https://[broken",
        "https://x:bad/a",
        "//v.redd.it/a",
    ],
)
def test_video_rejects_unsafe_sources(url: str):
    assert videos({"media": [{"url": url, "asset_type": "video"}]}, {}) == []


async def test_archive_metadata_only_on_detail_and_posters_not_repeated(
    app: web.Application,
):
    post = row("reddit.com", "t3_abc")
    post.update(
        source="reddit",
        url=PERMALINK,
        html=f'<p><img src="{POSTER}"></p><p>Context.</p>',
    )
    snapshot = {
        "num_comments": 10,
        "media": [
            {
                "url": VIDEO,
                "asset_type": "video",
                "poster_url": POSTER,
            },
        ],
        "comment_capture": {
            "status": "captured",
            "sort": "best",
            "limit": 50,
            "comments": [{"id": "a", "body": "Hello"}],
        },
    }
    results: list[list[dict[str, Any]]] = [
        [post],
        [],
        [{"post_id": "t3_abc", "payload": json.dumps(snapshot)}],
        [],
    ]
    app["clickhouse"] = FakeClickHouse(*results)
    response = await request(
        app, "/api/reader/post?domain=reddit.com&id=t3_abc",
    )
    assert response.status == 200
    detail = json.loads(response.text or "")
    assert detail["reddit"]["comment_capture"]["status"] == "captured"
    assert detail["reddit"]["video"]["url"] == VIDEO
    assert detail["thumbnail"] == POSTER
    assert "<img" not in detail["html"] and "Context." in detail["html"]
    app["clickhouse"] = FakeClickHouse(*results)
    response = await request(app, "/api/reader/posts?kind=reddit")
    listing = json.loads(response.text or "")["posts"][0]
    assert "comment_capture" not in listing["reddit"]
    assert "video" not in listing["reddit"]
    assert listing["thumbnail"] == POSTER
