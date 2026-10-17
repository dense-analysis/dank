from __future__ import annotations

import datetime as dt
import json

import pytest

from dank.model import RawPost
from dank.process.youtube import convert_raw_youtube_post
from dank.youtube import (
    canonical_video_id,
    canonical_youtube_url,
    discovery_from_post,
    youtube_post_id,
)


@pytest.mark.parametrize("url", [
    "https://www.youtube.com/watch?v=AbCdEfGhIjK&t=30",
    "https://youtube.com/watch?feature=share&v=AbCdEfGhIjK",
    "https://m.youtube.com/watch?feature=share&amp;v=AbCdEfGhIjK",
    "https://youtu.be/AbCdEfGhIjK?si=example",
    "https://www.youtube.com/shorts/AbCdEfGhIjK",
    "https://www.youtube-nocookie.com/embed/AbCdEfGhIjK",
    "http://music.youtube.com/watch?v=AbCdEfGhIjK",
])
def test_canonical_video_identity(url: str) -> None:
    assert canonical_video_id(url) == "AbCdEfGhIjK"
    assert canonical_youtube_url(url) == (
        "https://www.youtube.com/watch?v=AbCdEfGhIjK"
    )


@pytest.mark.parametrize("url", [
    "https://youtube.com.evil.example/watch?v=AbCdEfGhIjK",
    "https://youtube.com@evil.example/watch?v=AbCdEfGhIjK",
    "https://evil@youtube.com/watch?v=AbCdEfGhIjK",
    "javascript:https://youtube.com/watch?v=AbCdEfGhIjK",
    "https://youtube.com:9000/watch?v=AbCdEfGhIjK",
    "https://youtube.com/watch?v=AbCdEfGhIjKl",
    "https://youtube.com/watch?v=AbCdEfGhIjK&v=other",
    "https://youtu.be/AbCdEfGhIjK/extra",
    "https://youtube.com/playlist?list=AbCdEfGhIjK",
    "https://youtube.com/live/AbCdEfGhIjK",
])
def test_unsupported_urls_are_not_video_identities(url: str) -> None:
    assert canonical_video_id(url) is None


def test_community_post_identity() -> None:
    assert youtube_post_id("https://www.youtube.com/post/Ugkx1234567890") == (
        "Ugkx1234567890"
    )
    assert youtube_post_id("https://evil.example/post/Ugkx1234567890") is None


def youtube_raw() -> RawPost:
    return RawPost(
        domain="youtube.com", post_id="video:AbCdEfGhIjK",
        url="https://www.youtube.com/watch?v=AbCdEfGhIjK",
        post_created_at=None,
        scraped_at=dt.datetime(2026, 10, 17, tzinfo=dt.UTC),
        source="youtube", request_url="https://www.youtube.com/playlist?list=x",
        payload=json.dumps({
            "schema_version": 1, "kind": "video", "title": "Test story",
            "text": "<script>unsafe</script>\nSecond paragraph",
            "channel_name": "Channel 4 News",
            "discovery": {
                "feed_url": "https://www.youtube.com/feed/hype",
                "feed_name": "Top 100 · United Kingdom", "feed_kind": "hype",
                "shelf": "Top 100", "rank": 7, "region": "GB",
                "playlist_id": "OLJexample",
            },
            "renderer": {"videoRenderer": {"videoId": "AbCdEfGhIjK"}},
        }),
    )


def test_observation_preserves_run_rank_region_and_original_payload() -> None:
    raw = youtube_raw()
    first = discovery_from_post(raw, "first-run")
    second = discovery_from_post(raw, "second-run")
    assert first.item_id == second.item_id == raw.post_id
    assert first.run_id != second.run_id
    assert first.feed_url == "https://www.youtube.com/feed/hype"
    assert first.feed_name == "Top 100 · United Kingdom"
    assert first.rank == 7
    assert first.region == "GB"
    assert first.playlist_id == "OLJexample"
    assert first.payload == raw.payload


def test_feed_processing_keeps_text_safe_and_dates_honest() -> None:
    raw = youtube_raw()
    post = convert_raw_youtube_post(raw)
    assert post is not None
    assert post.created_at == raw.scraped_at
    assert post.source == "youtube"
    assert post.author == "Channel 4 News"
    assert post.html == (
        "<p>&lt;script&gt;unsafe&lt;/script&gt;</p><p>Second paragraph</p>"
    )
    assert convert_raw_youtube_post(raw._replace(payload="[]")) is None
