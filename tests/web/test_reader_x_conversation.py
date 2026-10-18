from __future__ import annotations

import copy
import datetime as dt
import json
import pathlib
from typing import Any

import pytest
from aiohttp import web
from test_reader import FakeClickHouse, request, row

from dank.config import SourceConfig, load_settings
from dank.web.app import PostRow, create_app
from dank.web.reader_x_conversation import (
    MAX_CONVERSATION_DEPTH,
    MAX_CONVERSATION_POSTS,
    conversation_capture,
)

DATE = "2026-10-08T12:30:45.123000+00:00"
OLDER = "2026-10-07T10:00:00+00:00"
ICON = "https://pbs.twimg.com/profile_images/123/account_normal.png"


@pytest.fixture
def app(tmp_path: pathlib.Path) -> web.Application:
    settings = load_settings("config.example.toml")._replace(
        data_dir=tmp_path, sources=(SourceConfig("x.com", ("author",)),),
    )

    return create_app(settings, page_size=50)


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


async def test_detail_orders_context_and_replies_and_reuses_saved_media(
    app: web.Application, tmp_path: pathlib.Path,
) -> None:
    image = "https://pbs.twimg.com/media/reply.jpg"
    local = tmp_path / "assets" / "reply.jpg"
    local.touch()
    focal = tweet("102", "101", replies=1)
    reply = tweet("103", "102", handle="visitor", text=(
        '<script>alert(1)</script> Hello @author https://t.co/link'
    ))
    reply["legacy"].update({
        "entities": {"urls": [{"url": "https://t.co/link",
                                  "expanded_url": "https://youtu.be/video"}]},
        "extended_entities": {"media": [{"type": "photo",
                                          "media_url_https": image}]},
    })
    focal["conversation_capture"] = archive([
        tweet("104", "103"), reply, tweet("101", "100"), tweet("100"),
        tweet("105", "100", handle="sibling"),
        tweet("200", root="200", handle="unrelated"),
        {"quoted_status_result": {"result": tweet("201", "102")}},
    ], target="102")
    fake = FakeClickHouse([saved_row("102")], [], [raw_row(focal)], [{
        "domain": "x.com", "post_id": "103", "url": image,
        "local_path": str(local), "content_type": "image/jpeg",
        "size_bytes": 0,
    }])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=102")
    assert response.status == 200
    post = json.loads(response.text or "")
    assert post["html"] == "<p>Article body</p>"
    capture = post["x"]["conversation"]
    assert capture["status"] == "captured"
    assert capture["captured_count"] == 2
    assert [node["id"] for node in capture["context"]] == ["100", "101"]
    assert [node["id"] for node in capture["replies"]] == ["103", "104"]
    first, second = capture["replies"]
    assert (first["depth"], second["depth"]) == (0, 1)
    assert not first["is_author"] and second["is_author"]
    assert first["parent_id"] == "102" and not first["missing_parent"]
    assert first["url"] == "https://x.com/visitor/status/103"
    assert first["x"]["name"] == "Visitor" and first["x"]["icon_url"] == ICON
    assert "<script>" not in first["html"]
    assert "&lt;script&gt;" in first["html"]
    assert 'href="https://youtu.be/video"' in first["html"]
    assert first["x"]["media"][0]["url"] == "/assets/reply.jpg"
    assert not first["x"]["media"][0]["remote"]
    assert "conversation" not in first["x"]
    assert first["created_at"] == "2026-10-07T10:00:00+00:00"
    assert len(fake.calls) == 7
    assert "x_related_keys" in fake.calls[-2][1]
    assert "link_post_keys" in fake.calls[-1][1]


async def test_feed_has_validated_summary_without_conversation_posts(
    app: web.Application,
) -> None:
    focal = tweet("100", replies=2)
    focal["conversation_capture"] = archive([tweet("101", "100")])
    fake = FakeClickHouse([saved_row()], [], [raw_row(focal)])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=all")
    assert response.status == 200
    post = json.loads(response.text or "")["posts"][0]
    assert post["x"]["replies_status"] == "partial"
    assert post["x"]["replies_captured_count"] == 1
    assert "conversation" not in post["x"]
    assert "reply.jpg" not in (response.text or "")
    assert len(fake.calls) == 4


@pytest.mark.parametrize("latest", ["failed", "disabled", "not_collected"])
@pytest.mark.parametrize("prior", ["captured", "partial"])
async def test_previous_archive_survives_later_attempt_without_stale_identity(
    app: web.Application, latest: str, prior: str,
) -> None:
    old = tweet("100", handle="oldname", replies=24)
    old["conversation_capture"] = archive([
        *(tweet(str(101 + index), "100", handle="visitor")
          for index in range(24)),
    ], status=prior, captured_at=OLDER)
    current = tweet("100", handle="author", text="New body", replies=99)

    if latest != "not_collected":
        current["conversation_capture"] = archive([], status=latest)

    fake = FakeClickHouse([saved_row()], [], [raw_row(current)], [
        raw_row(old, scraped_at=OLDER),
    ], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=100")
    assert response.status == 200
    post = json.loads(response.text or "")
    assert post["html"] == "<p>Article body</p>"
    assert post["x"]["handle"] == "author"
    assert post["x"]["name"] == "Author"
    assert post["x"]["captured_at"] == DATE
    capture = post["x"]["conversation"]
    assert capture["status"] == prior
    assert post["x"]["reply_count"] == 99
    assert capture["captured_at"] == OLDER
    assert capture["latest_attempt_status"] == latest
    assert capture["latest_attempt_at"] == DATE
    assert capture["reported_replies"] == 24
    assert [reply["id"] for reply in capture["replies"]] == [
        str(101 + index) for index in range(24)
    ]
    query, params = fake.calls[3]
    assert "LIMIT 1 BY domain, post_id" in query
    assert "toUnixTimestamp64Milli(scraped_at) <" in query
    assert params["x_before_0"] == 1791462645123


@pytest.mark.parametrize("snapshot", ["newer", "same", "wrong_target"])
async def test_history_rejects_unprocessed_or_mismatched_archives(
    app: web.Application, snapshot: str,
) -> None:
    focal = tweet("100")
    old = copy.deepcopy(focal)
    old["conversation_capture"] = archive(
        [tweet("101", "100")], captured_at=OLDER,
    )
    timestamp = "2026-10-09T12:00:00+00:00" if snapshot == "newer" else DATE

    if snapshot == "wrong_target":
        old["conversation_capture"]["target_id"] = "900"
        timestamp = OLDER

    fake = FakeClickHouse([saved_row()], [], [raw_row(focal)], [
        raw_row(old, scraped_at=timestamp),
    ])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=100")
    assert response.status == 200
    capture = json.loads(response.text or "")["x"]["conversation"]
    assert capture["status"] == "not_collected"
    assert capture["replies"] == []


async def test_latest_partial_archive_is_not_replaced_by_older_capture(
    app: web.Application,
) -> None:
    focal = tweet("100", replies=2)
    focal["conversation_capture"] = archive(
        [tweet("102", "100")], status="partial",
    )
    fake = FakeClickHouse([saved_row()], [], [raw_row(focal)], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=100")
    assert response.status == 200
    capture = json.loads(response.text or "")["x"]["conversation"]
    assert capture["captured_at"] == DATE
    assert [node["id"] for node in capture["replies"]] == ["102"]
    assert "latest_attempt_status" not in capture
    assert len(fake.calls) == 6
    assert "x_related_keys" in fake.calls[-2][1]
    assert "link_post_keys" in fake.calls[-1][1]


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


async def test_history_queries_are_batched_and_bound_to_each_displayed_version(
    app: web.Application,
) -> None:
    focal = tweet("100")
    second = tweet("200", root="200")
    old = copy.deepcopy(focal)
    old["conversation_capture"] = archive([], captured_at=OLDER)
    wrong_domain = {**raw_row(old, scraped_at=OLDER), "domain": "twitter.com"}
    fake = FakeClickHouse([saved_row(), saved_row("200")], [], [
        raw_row(focal), raw_row(second),
    ], [raw_row(old, scraped_at=OLDER), wrong_domain])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=x")
    assert response.status == 200
    first, second_post = json.loads(response.text or "")["posts"]
    assert first["x"]["replies_status"] == "captured"
    assert second_post["x"]["replies_status"] == "not_collected"
    assert len(fake.calls) == 5
    assert fake.calls[2][1]["x_versions"] == [
        ("x.com", "100", 1791462645123), ("x.com", "200", 1791462645123),
    ]
    assert fake.calls[3][1] == {
        "x_domain_0": "x.com", "x_post_0": "100",
        "x_before_0": 1791462645123,
        "x_domain_1": "x.com", "x_post_1": "200",
        "x_before_1": 1791462645123,
    }


async def test_missing_exact_snapshot_can_recover_archive_without_old_profile(
    app: web.Application,
) -> None:
    old = tweet("100", handle="oldname")
    old["conversation_capture"] = archive([], captured_at=OLDER)
    fake = FakeClickHouse(
        [saved_row()], [], [], [raw_row(old, scraped_at=OLDER)],
    )
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=100")
    assert response.status == 200
    post = json.loads(response.text or "")
    assert post["x"]["handle"] == "author"
    assert post["x"]["name"] == "author"
    assert post["x"]["icon_url"] is None
    assert post["x"]["captured_at"] is None
    assert post["x"]["conversation"]["status"] == "captured"
    assert post["x"]["conversation"]["captured_at"] == OLDER
    assert post["x"]["conversation"]["latest_attempt_status"] == (
        "not_collected"
    )


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
        "dank.web.reader_x_conversation.post_html", unexpected_render,
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
