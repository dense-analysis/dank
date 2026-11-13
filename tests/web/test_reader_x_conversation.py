from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pytest
from test_reader import row

from dank.api.posts.model import PostRow
from dank.web.reader_x_conversation import (
    MAX_CONVERSATION_DEPTH,
    MAX_CONVERSATION_POSTS,
    conversation_capture,
)

DATE = "2026-10-08T12:30:45.123000+00:00"
OLDER = "2026-10-07T10:00:00+00:00"
ICON = "https://pbs.twimg.com/profile_images/123/account_normal.png"


def tweet(
    post_id: str, parent: str | None = None, *, root: str = "100",
    handle: str = "author", text: str = "Post text", replies: int = 0,
) -> dict[str, Any]:
    return {
        "__typename": "Tweet", "rest_id": post_id,
        "core": {"user_results": {"result": {
            "core": {"screen_name": handle, "name": handle.title()},
            "avatar": {"image_url": ICON},
        }}},
        "legacy": {
            "id_str": post_id, "conversation_id_str": root,
            "in_reply_to_status_id_str": parent, "full_text": text,
            "created_at": "Wed Oct 07 10:00:00 +0000 2026",
            "reply_count": replies,
        },
    }


def saved_row(post_id: str = "100", domain: str = "x.com") -> dict[str, Any]:
    return {
        **row(domain, post_id), "source": "x",
        "url": f"https://x.com/author/status/{post_id}",
    }


def selected(post_id: str = "100") -> PostRow:
    timestamp = dt.datetime.fromisoformat(DATE)

    return PostRow("x.com", post_id, f"https://x.com/author/status/{post_id}",
                   "author", "", "<p>Processed</p>", timestamp, timestamp, "x")


def archive(
    posts: list[dict[str, Any]], *, target: str = "100", root: str = "100",
    status: str = "captured", captured_at: str = DATE,
) -> dict[str, Any]:
    return {
        "status": status, "target_id": target, "conversation_id": root,
        "captured_at": captured_at, "limit": 50,
        "has_more": status == "partial",
        "reason": "cursor" if status == "partial" else None, "posts": posts,
    }


def raw_row(
    payload: dict[str, Any], *, scraped_at: str = DATE,
) -> dict[str, Any]:
    return {
        "domain": "x.com", "post_id": payload["rest_id"],
        "payload": json.dumps(payload), "scraped_at": scraped_at,
    }


def project(payload: dict[str, Any], post_id: str = "100") -> dict[str, Any]:
    return conversation_capture(
        selected(post_id), payload, {}, include_posts=True,
    )


def test_missing_parents_do_not_invent_nested_ancestry() -> None:
    root = tweet("100")
    root["conversation_capture"] = archive([
        tweet("102", "101"), tweet("103", "102"),
    ])
    capture = project(root)
    assert capture["status"] == "partial"
    assert capture["reason"] == "missing_context"
    assert [node["id"] for node in capture["replies"]] == ["102", "103"]
    assert capture["replies"][0]["missing_parent"]
    assert not capture["replies"][1]["missing_parent"]
    nested = tweet("110", "100")
    nested["conversation_capture"] = archive([
        tweet("100"), tweet("102", "101"), tweet("111", "110"),
    ], target="110")
    capture = project(nested, "110")
    assert capture["status"] == "partial"
    assert [node["id"] for node in capture["context"]] == ["100"]
    assert [node["id"] for node in capture["replies"]] == ["111"]


def test_cycles_duplicates_ads_and_quotes_are_excluded() -> None:
    focal = tweet("100")
    advertisement = tweet("105", "100")
    advertisement["promotedMetadata"] = {"advertiser": "Ad"}
    hostile = tweet("106", "100", handle="evil.example/path")
    focal["conversation_capture"] = archive([
        tweet("101", "100"), tweet("101", "100", text="Duplicate"),
        tweet("102", "103"), tweet("103", "102"),
        tweet("104", root="999"), advertisement, hostile,
        {"quoted_status_result": {"result": tweet("107", "100")}},
    ])
    capture = project(focal)
    assert [node["id"] for node in capture["replies"]] == ["101"]
    assert capture["status"] == "partial"
    assert "Duplicate" not in capture["replies"][0]["html"]


@pytest.mark.parametrize("field,value", [
    ("target_id", "999"), ("conversation_id", "999"), ("captured_at", "bad"),
    ("posts", {}),
])
def test_invalid_capture_cannot_claim_success(
    field: str, value: object,
) -> None:
    focal = tweet("100")
    focal["conversation_capture"] = archive([tweet("101", "100")])
    focal["conversation_capture"][field] = value
    capture = project(focal)
    assert capture["status"] == "failed"
    assert capture["reason"] == "invalid_capture"
    assert capture["replies"] == capture["context"] == []


def test_reply_arrays_and_depth_are_bounded() -> None:
    focal = tweet("100")
    focal["conversation_capture"] = archive([
        tweet(str(1000 + index), "100")
        for index in range(MAX_CONVERSATION_POSTS + 20)
    ])
    capture = project(focal)
    assert len(capture["replies"]) == MAX_CONVERSATION_POSTS
    assert capture["status"] == "partial" and capture["reason"] == "limit"
    focal["conversation_capture"] = archive([
        tweet(str(100 + index), str(99 + index))
        for index in range(1, MAX_CONVERSATION_DEPTH + 20)
    ])
    capture = project(focal)
    assert len(capture["replies"]) == MAX_CONVERSATION_DEPTH
    assert max(node["depth"] for node in capture["replies"]) < (
        MAX_CONVERSATION_DEPTH
    )
    assert capture["status"] == "partial"


@pytest.mark.parametrize("status", ["failed", "disabled", [], {}, 42, None])
def test_bad_attempts_have_no_fabricated_replies(status: object) -> None:
    focal = tweet("100")
    focal["conversation_capture"] = {
        **archive([tweet("101", "100")]), "status": status,
    }
    capture = project(focal)
    assert capture["status"] in {"failed", "disabled", "not_collected"}
    assert capture["replies"] == capture["context"] == []


def test_context_missing_parent_and_remote_media_are_honest() -> None:
    focal = tweet("102", "101")
    reply = tweet("103", "102")
    reply["legacy"]["extended_entities"] = {"media": [
        {"type": "photo", "media_url_https": "https://evil.example/image.jpg"},
        {"type": "photo", "media_url_https": "https://pbs.twimg.com/media/a.jpg"},
    ]}
    reply["legacy"]["entities"] = {"urls": [
        {"url": "javascript:alert(1)", "expanded_url": "javascript:alert(1)"},
    ]}
    focal["conversation_capture"] = archive([reply], target="102")
    capture = project(focal, "102")
    assert capture["status"] == "partial"
    assert capture["reason"] == "missing_context"
    assert capture["context"] == []
    saved = capture["replies"][0]
    assert saved["x"]["links"] == []
    assert len(saved["x"]["media"]) == 1
    assert saved["x"]["media"][0]["remote"]
    assert saved["x"]["media"][0]["url"] == "https://pbs.twimg.com/media/a.jpg"


def test_unknown_reply_count_is_not_a_claim_of_zero() -> None:
    focal = tweet("100")
    focal["legacy"].pop("reply_count")
    focal["conversation_capture"] = archive([], status="partial")
    focal["conversation_capture"]["reason"] = "missing_source"
    capture = project(focal)
    assert capture["reported_replies"] is None
    assert capture["status"] == "partial"


@pytest.mark.parametrize("url", [
    "https://x.com.evil.example/author/status/100",
    "https://x.com/author/status/999",
    "https://x.com@evil.example/author/status/100",
])
def test_noncanonical_posts_cannot_project_archives(url: str) -> None:
    focal = tweet("100")
    focal["conversation_capture"] = archive([tweet("101", "100")])
    capture = conversation_capture(
        selected()._replace(url=url), focal, {}, include_posts=True,
    )
    assert capture["status"] == "failed"
    assert capture["replies"] == []


@pytest.mark.parametrize("order", [
    ["101", "102", "103", "104", "105"],
    ["103", "101", "102", "105", "104"],
])
def test_reply_branches_stay_together_across_interleaved_pages(
    order: list[str],
) -> None:
    nodes = {
        "101": tweet("101", "100"), "102": tweet("102", "100"),
        "103": tweet("103", "101"), "104": tweet("104", "103"),
        "105": tweet("105", "102"),
    }
    focal = tweet("100", replies=2)
    focal["conversation_capture"] = archive(
        [nodes[node_id] for node_id in order],
    )
    capture = project(focal)
    assert [(node["id"], node["depth"]) for node in capture["replies"]] == [
        ("101", 0), ("103", 1), ("104", 2), ("102", 0), ("105", 1),
    ]


def test_feed_conversation_summary_does_not_render_reply_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_render(_: dict[str, Any]) -> str:
        pytest.fail("Feed summaries must not serialize entire conversations")

    monkeypatch.setattr(
        "dank.web.reader_x_post.post_html", unexpected_render,
    )
    focal = tweet("100", replies=1)
    focal["conversation_capture"] = archive([tweet("101", "100")])
    summary = conversation_capture(selected(), focal, {}, include_posts=False)
    assert summary["status"] == "captured"
    assert summary["captured_count"] == 1
    assert "replies" not in summary and "context" not in summary


def test_reply_video_keeps_layout_and_source_metadata() -> None:
    focal = tweet("100")
    reply = tweet("101", "100")
    source_url = "https://x.com/author/status/101/video/1"
    reply["legacy"]["extended_entities"] = {"media": [{
        "type": "video", "expanded_url": source_url,
        "video_info": {
            "aspect_ratio": [9, 16],
            "variants": [{
                "url": "https://video.twimg.com/video.mp4",
                "content_type": "video/mp4",
            }],
        },
    }]}
    focal["conversation_capture"] = archive([reply])
    capture = project(focal)
    media = capture["replies"][0]["x"]["media"][0]
    assert media["aspect_ratio"] == 9 / 16
    assert media["source_url"] == source_url
