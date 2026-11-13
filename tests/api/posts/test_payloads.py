"""What each kind of post looks like over HTTP, from real rows.

The real store runs over a throwaway ClickHouse database, so these cover the
projection and the SQL that picks each post's displayed version together.
"""

from __future__ import annotations

import copy
import datetime as dt
import pathlib
from typing import Any, cast
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.clickhouse import ClickHousePostStore
from dank.api.posts.deps import post_store
from dank.config import Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.assets import assets_path
from tests.api.clickhouse import COLLECTED_AT, asset, insert, post, raw_post

pytestmark = pytest.mark.clickhouse

DATE = COLLECTED_AT.isoformat()
OLDER = COLLECTED_AT - dt.timedelta(days=1)
LATER = COLLECTED_AT + dt.timedelta(days=1)
BODY = "<p>Article body</p>"
ICON = "https://pbs.twimg.com/profile_images/123/account_normal.png"
PERMALINK = "https://www.reddit.com/r/python/comments/abc/title/"
EMPTY_X: dict[str, Any] = {
    "links": [], "reply_count": None, "repost_count": None,
    "like_count": None, "view_count": None,
    "replies_status": "not_collected", "captured_at": None,
    "replies_captured_count": 0,
}
CAPTURED_X: dict[str, Any] = {**EMPTY_X, "media": [], "captured_at": DATE}


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    path = assets_path(settings)
    path.mkdir(parents=True, exist_ok=True)

    return path


@pytest.fixture
def app(
    app: FastAPI, clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> FastAPI:
    app.dependency_overrides[post_store] = lambda: ClickHousePostStore(
        clickhouse, assets,
    )

    return app


def x_url(post_id: str, handle: str = "author") -> str:
    return f"https://x.com/{handle}/status/{post_id}"


def x_post(
    post_id: str,
    handle: str = "author",
    *,
    html: str = BODY,
    author: str = "",
) -> dict[str, Any]:
    return post(
        "x.com", post_id, url=x_url(post_id, handle), source="x", html=html,
        author=author,
    )


def x_raw(
    payload: dict[str, Any] | str,
    post_id: str = "",
    *,
    scraped_at: dt.datetime = COLLECTED_AT,
    domain: str = "x.com",
) -> dict[str, Any]:
    identity = post_id or str(cast(dict[str, Any], payload)["rest_id"])

    return raw_post(
        domain, identity, payload, url=x_url(identity), source="x",
        scraped_at=scraped_at,
    )


def tweet(
    post_id: str,
    parent: str | None = None,
    *,
    root: str = "100",
    handle: str = "author",
    text: str = "Post text",
    replies: int = 0,
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


def archive(
    posts: list[dict[str, Any]],
    *,
    target: str = "100",
    root: str = "100",
    status: str = "captured",
    captured_at: str = DATE,
) -> dict[str, Any]:
    return {
        "status": status, "target_id": target, "conversation_id": root,
        "captured_at": captured_at, "limit": 50,
        "has_more": status == "partial",
        "reason": "cursor" if status == "partial" else None, "posts": posts,
    }


def reddit_post(html: str = "") -> dict[str, Any]:
    return post(
        "reddit.com", "t3_abc", url=PERMALINK, source="reddit", html=html,
    )


def reddit_raw(
    payload: dict[str, Any] | str, *, scraped_at: dt.datetime = COLLECTED_AT,
) -> dict[str, Any]:
    return raw_post(
        "reddit.com", "t3_abc", payload, url=PERMALINK, source="reddit",
        scraped_at=scraped_at,
    )


async def get_post(
    client: AsyncClient, domain: str, post_id: str,
) -> dict[str, Any]:
    response = await client.get(
        f"/api/reader/post?domain={domain}&id={post_id}",
    )

    assert response.status_code == 200

    return response.json()


async def test_posts_paginate_without_cross_domain_asset_collisions(
    client: AsyncClient, clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> None:
    image = assets / "b.jpg"
    image.touch()
    source = "https://example.com/b.jpg"
    await insert(clickhouse, "posts", [
        post("c.example", "same", html=BODY),
        post("b.example", "same", html=f'<img src="{source}"><p>Body</p>'),
        post("a.example", "same", html=BODY),
    ])
    await insert(clickhouse, "assets", [
        asset("b.example", "same", source, image),
    ])
    await insert(clickhouse, "raw_posts", [
        raw_post("b.example", "same", {"page_fetch_status": "failed"}),
    ])

    response = await client.get("/api/reader/posts?limit=2")
    body = response.json()
    first, second = body["posts"]

    assert (first["domain"], second["domain"]) == ("c.example", "b.example")
    assert (first["thumbnail"], first["media"]) == (None, [])
    assert second["thumbnail"] == "/assets/b.jpg"
    assert second["media"] == [
        {"url": "/assets/b.jpg", "content_type": "image/jpeg"},
    ]
    assert first["collection_incomplete"] is False
    assert second["collection_incomplete"] is True
    assert body["limited"] is False

    following = await client.get(
        "/api/reader/posts?limit=2&cursor=" + quote(body["next_cursor"]),
    )

    assert [item["domain"] for item in following.json()["posts"]] == [
        "a.example",
    ]
    assert following.json()["next_cursor"] is None


@pytest.mark.parametrize("failed", [True, False])
async def test_article_with_text_reports_its_page_capture_failure(
    client: AsyncClient, clickhouse: ClickHouseClient, *, failed: bool,
) -> None:
    await insert(clickhouse, "posts", [post("a.example", "same", html=BODY)])
    await insert(clickhouse, "raw_posts", [raw_post(
        "a.example", "same",
        {"page_fetch_status": "failed" if failed else "ok"},
    )])

    detail = await get_post(client, "a.example", "same")

    assert detail["html"] == BODY
    assert detail["collection_incomplete"] is failed


async def test_a_retry_awaiting_processing_does_not_change_the_capture_shown(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [post("a.example", "same", html=BODY)])
    await insert(clickhouse, "raw_posts", [raw_post(
        "a.example", "same", {"page_fetch_status": "failed"},
        scraped_at=LATER,
    )])

    detail = await get_post(client, "a.example", "same")

    assert detail["collection_incomplete"] is False


async def test_social_post_does_not_need_rss_page_capture(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [x_post("1")])
    await insert(clickhouse, "raw_posts", [
        x_raw({"page_fetch_status": "failed"}, "1"),
    ])

    detail = await get_post(client, "x.com", "1")

    assert detail["collection_incomplete"] is False


async def test_reddit_metadata_uses_the_processed_capture_and_safe_html(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [
        reddit_post(html="<p>Body</p><script>alert(1)</script>"),
    ])
    await insert(clickhouse, "raw_posts", [
        reddit_raw({
            "subreddit": "other", "score": 123, "num_comments": 42,
            "link_flair_text": "Discussion",
        }),
        # A newer capture nothing has processed yet.
        reddit_raw({"score": 999}, scraped_at=LATER),
    ])

    response = await client.get("/api/reader/posts?kind=reddit")
    [item] = response.json()["posts"]

    assert item["reddit"] == {
        "community": "python", "score": 123, "comments": 42,
        "flair": "Discussion", "icon_url": None, "video": None, "videos": [],
    }
    assert "<script>" not in item["html"]


@pytest.mark.parametrize("payload", [
    "broken", "[]", '{"score":true}',
    '{"score":9007199254740992,"num_comments":-1}',
])
async def test_reddit_posts_remain_readable_without_valid_metadata(
    client: AsyncClient, clickhouse: ClickHouseClient, payload: str,
) -> None:
    await insert(clickhouse, "posts", [reddit_post()])
    await insert(clickhouse, "raw_posts", [reddit_raw(payload)])

    response = await client.get("/api/reader/posts?kind=reddit")
    metadata = response.json()["posts"][0]["reddit"]

    assert (metadata["score"], metadata["comments"]) == (None, None)
    assert metadata["community"] == "python"


@pytest.mark.parametrize("kind", ["all", "reddit"])
async def test_feed_videos_match_detail_without_loading_comments(
    client: AsyncClient, clickhouse: ClickHouseClient, kind: str,
) -> None:
    video = "https://v.redd.it/abc/HLSPlaylist.m3u8"
    poster = "https://preview.redd.it/abc.jpg"
    await insert(clickhouse, "posts", [reddit_post(
        html=f'<p><img src="{poster}"></p><p>Context.</p>',
    )])
    await insert(clickhouse, "raw_posts", [reddit_raw({
        "num_comments": 10,
        "media": [
            {"url": video, "asset_type": "video", "poster_url": poster},
        ],
        "comment_capture": {
            "status": "captured", "sort": "best", "limit": 50,
            "comments": [{"id": "a", "body": "Hello"}],
        },
    })])

    detail = await get_post(client, "reddit.com", "t3_abc")

    assert detail["reddit"]["comment_capture"]["status"] == "captured"
    assert detail["reddit"]["video"]["hls_url"] == video
    assert detail["thumbnail"] == poster
    assert "<img" not in detail["html"]
    assert "Context." in detail["html"]

    response = await client.get(f"/api/reader/posts?kind={kind}")
    [listing] = response.json()["posts"]

    assert "comment_capture" not in listing["reddit"]
    assert listing["reddit"]["video"] == detail["reddit"]["video"]
    assert listing["reddit"]["videos"] == detail["reddit"]["videos"]
    assert listing["thumbnail"] == poster


async def test_image_references_reach_feeds_and_opened_posts(
    client: AsyncClient, clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> None:
    source = "https://i.redd.it/image.jpg"
    image = assets / "image.jpg"
    image.touch()
    await insert(clickhouse, "posts", [
        reddit_post(html=f'<p><img src="{source}"></p>'),
    ])
    await insert(clickhouse, "assets", [
        asset("reddit.com", "t3_abc", source, image),
    ])
    await insert(clickhouse, "raw_posts", [
        reddit_raw({"media": [{"asset_type": "photo", "url": source}]}),
    ])
    expected = [{"url": "/assets/image.jpg", "source_url": source}]

    detail = await get_post(client, "reddit.com", "t3_abc")
    listing = await client.get("/api/reader/posts?kind=reddit")

    assert detail["reddit"]["images"] == expected
    assert listing.json()["posts"][0]["reddit"]["images"] == expected


@pytest.mark.parametrize("path", [
    "/api/reader/posts", "/api/reader/posts?kind=all",
])
async def test_mixed_feed_keeps_each_domains_x_profile_apart(
    client: AsyncClient, clickhouse: ClickHouseClient, path: str,
) -> None:
    profile = {
        "core": {"screen_name": "Archived", "name": "Archived account"},
        "avatar": {"image_url": ICON},
    }
    old_profile = {"legacy": {
        "screen_name": "LEGACY", "name": "Old",
        "profile_image_url_https": ICON,
    }}
    legacy_url = "https://www.twitter.com/Legacy/status/123"
    await insert(clickhouse, "posts", [
        x_post("same", "Archived", author="Edited display name"),
        x_post("456", "Archived"),
        post("twitter.com", "same", url=legacy_url, source="x"),
        reddit_post(),
        post("blog.example", "same", html=BODY),
    ])
    await insert(clickhouse, "raw_posts", [
        x_raw({"core": {"user_results": {"result": profile}}}, post_id)
        for post_id in ("same", "456")
    ] + [
        raw_post(
            "twitter.com", "same",
            {"core": {"user_results": {"result": old_profile}}},
            url=legacy_url, source="x",
        ),
    ])

    response = await client.get(path)
    posts = {
        (item["domain"], item["id"]): item for item in response.json()["posts"]
    }
    archived: dict[str, Any] = {
        "handle": "archived", "name": "Archived account", "icon_url": ICON,
        **CAPTURED_X,
    }

    assert posts["x.com", "same"]["x"] == archived
    assert posts["x.com", "456"]["x"] == archived
    assert posts["x.com", "same"]["author"] == "Edited display name"
    assert posts["twitter.com", "same"]["x"] == {
        "handle": "legacy", "name": "Old", "icon_url": ICON, **CAPTURED_X,
    }
    assert posts["reddit.com", "t3_abc"]["reddit"]["community"] == "python"
    assert "x" not in posts["reddit.com", "t3_abc"]
    assert "x" not in posts["blog.example", "same"]


async def test_x_cards_and_details_use_a_captured_profile_or_their_handle(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    fallback = {"handle": "example", "name": "example", "icon_url": None}
    cases: dict[str, tuple[Any, dict[str, Any]]] = {
        "1": (
            {"legacy": {"screen_name": "Example", "name": "Legacy name",
                        "profile_image_url_https": ICON}},
            {"handle": "example", "name": "Legacy name", "icon_url": ICON},
        ),
        "2": (
            {"core": {"screen_name": "other", "name": "Wrong account"},
             "avatar": {"image_url": ICON}},
            fallback,
        ),
        "3": (
            {"core": {"screen_name": "Example", "name": "Example"},
             "avatar": {"image_url": "https://evil.example/avatar.png"}},
            {"handle": "example", "name": "Example", "icon_url": None},
        ),
        "4": ({"core": None, "legacy": []}, fallback),
        "5": ([], fallback),
    }
    await insert(clickhouse, "posts", [
        x_post(post_id, "EXAMPLE") for post_id in (*cases, "6", "7")
    ])
    await insert(clickhouse, "raw_posts", [
        x_raw({"core": {"user_results": {"result": profile}}}, post_id)
        for post_id, (profile, _) in cases.items()
    ] + [x_raw("broken", "6")])

    response = await client.get("/api/reader/posts?kind=x")
    feed = {item["id"]: item["x"] for item in response.json()["posts"]}

    for post_id, (_, expected) in cases.items():
        assert feed[post_id] == {**expected, **CAPTURED_X}

    for post_id in ("6", "7"):
        assert feed[post_id] == {**fallback, **EMPTY_X}

    detail = (await get_post(client, "x.com", "1"))["x"]
    conversation = detail.pop("conversation")

    assert (conversation["status"], conversation["context"]) == (
        "not_collected", [],
    )
    assert conversation["replies"] == []
    assert detail == {**cases["1"][1], **CAPTURED_X}


@pytest.mark.parametrize("url", [
    "https://x.com.evil.example/example/status/1",
    "https://example.com/example/status/1",
    "https://x.com/a-hyphen/status/1",
    "https://x.com/handleistoolong16/status/1",
    "https://x.com/i/web/status/1", "https://x.com/i/status/1",
])
async def test_x_posts_without_canonical_handles_do_not_guess_from_author(
    client: AsyncClient, clickhouse: ClickHouseClient, url: str,
) -> None:
    await insert(clickhouse, "posts", [
        post("x.com", "1", url=url, source="x", author="@Example"),
    ])

    response = await client.get("/api/reader/posts?kind=x")

    assert response.json()["posts"][0]["x"] == {
        "handle": "", "name": "", "icon_url": None, **EMPTY_X,
    }


async def test_non_x_posts_have_no_x_content_even_with_x_links(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [
        post("blog.example", "1", url="https://x.com/example/status/1"),
    ])

    response = await client.get("/api/reader/posts?kind=stories")

    assert "x" not in response.json()["posts"][0]


@pytest.mark.parametrize("file_exists", [False, True])
async def test_x_content_uses_saved_capture_and_existing_local_media(
    client: AsyncClient,
    clickhouse: ClickHouseClient,
    assets: pathlib.Path,
    *,
    file_exists: bool,
) -> None:
    image = "https://pbs.twimg.com/media/photo.jpg"
    image_file = assets / "photo.jpg"

    if file_exists:
        image_file.touch()

    payload = {
        "core": {"user_results": {"result": {
            "core": {"screen_name": "Example", "name": "Example Person"},
            "avatar": {"image_url": ICON},
        }}},
        "legacy": {
            "reply_count": 24, "retweet_count": 50, "favorite_count": 573,
            "full_text": "Same capture",
            "extended_entities": {"media": [{
                "type": "photo", "media_url_https": image,
                "expanded_url": "https://x.com/Example/status/1/photo/1",
            }, {
                "type": "video",
                "expanded_url": "https://x.com/Example/status/1/video/1",
                "video_info": {
                    "aspect_ratio": [9, 16],
                    "variants": [{
                        "url": "https://video.twimg.com/video.mp4",
                        "content_type": "video/mp4",
                    }],
                },
            }]},
            "entities": {"urls": [{
                "url": "https://t.co/abc",
                "expanded_url": "https://youtu.be/id",
            }]},
        },
        "views": {"count": "56000"},
    }
    await insert(clickhouse, "posts", [
        x_post("1", "Example", html="<p>Processed body</p>"),
    ])
    await insert(clickhouse, "assets", [
        asset("x.com", "1", image, image_file),
    ])
    await insert(clickhouse, "raw_posts", [x_raw(payload, "1")])
    expected = {
        "handle": "example", "name": "Example Person", "icon_url": ICON,
        "media": [{"type": "image",
                   "url": "/assets/photo.jpg" if file_exists else image,
                   "poster_url": None, "alt": "", "remote": not file_exists,
                   "source_url": "https://x.com/Example/status/1/photo/1"},
                  {"type": "video",
                   "url": "https://video.twimg.com/video.mp4",
                   "poster_url": None, "alt": "", "remote": True,
                   "source_url": "https://x.com/Example/status/1/video/1",
                   "aspect_ratio": 9 / 16}],
        "links": [{"url": "https://youtu.be/id", "title": "",
                   "description": "", "image_url": None}],
        "reply_count": 24, "repost_count": 50,
        "like_count": 573, "view_count": 56000,
        "replies_status": "not_collected",
        "replies_captured_count": 0,
        "captured_at": DATE,
    }

    listing = await client.get("/api/reader/posts?kind=x")
    detail = await get_post(client, "x.com", "1")
    conversation = detail["x"].pop("conversation")

    assert listing.json()["posts"][0]["x"] == expected
    assert detail["html"] == "<p>Processed body</p>"
    assert detail["x"] == expected
    assert conversation["reported_replies"] == 24
    assert conversation["status"] == "not_collected"


async def test_x_engagement_counts_preserve_zero_and_unknown_values(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    cases: list[tuple[object, int | None, int | None]] = [
        (0, 0, 0), (24, 24, 24),
        (2**53 - 1, 2**53 - 1, 2**53 - 1),
        ("0", None, 0), ("56000", None, 56000), ("00024", None, 24),
        ("9007199254740991", None, 2**53 - 1),
        (None, None, None), (True, None, None), (False, None, None),
        (-1, None, None), (2**53, None, None), (24.0, None, None),
        ([], None, None), ({}, None, None),
        ("", None, None), ("-1", None, None), ("+1", None, None),
        (" 24", None, None), ("24 ", None, None), ("24\n", None, None),
        ("24.0", None, None), ("1e3", None, None), ("1,000", None, None),
        ("0x10", None, None), ("\uff12\uff14", None, None),
        ("9007199254740992", None, None), ("9" * 5000, None, None),
    ]
    ids = [str(index + 1) for index in range(len(cases))]
    await insert(clickhouse, "posts", [x_post(post_id) for post_id in ids])
    await insert(clickhouse, "raw_posts", [
        x_raw({
            "legacy": {
                "reply_count": count, "retweet_count": count,
                "favorite_count": count,
            },
            "views": {"count": count},
        }, post_id)
        for post_id, (count, _, _) in zip(ids, cases, strict=True)
    ])

    response = await client.get("/api/reader/posts?kind=x&limit=100")
    found = {item["id"]: item["x"] for item in response.json()["posts"]}

    for post_id, (_, numeric, views) in zip(ids, cases, strict=True):
        x = found[post_id]

        assert (x["reply_count"], x["repost_count"], x["like_count"]) == (
            numeric, numeric, numeric,
        )
        assert x["view_count"] == views


@pytest.mark.parametrize("payload", [None, "broken", "[]", "{}"])
async def test_missing_x_snapshot_keeps_downloaded_media_available(
    client: AsyncClient,
    clickhouse: ClickHouseClient,
    assets: pathlib.Path,
    payload: str | None,
) -> None:
    image_file = assets / "archived.jpg"
    image_file.touch()
    await insert(clickhouse, "posts", [x_post("1", "Example")])
    await insert(clickhouse, "assets", [asset(
        "x.com", "1", "https://pbs.twimg.com/media/archived.jpg", image_file,
    )])

    if payload is not None:
        await insert(clickhouse, "raw_posts", [x_raw(payload, "1")])

    detail = await get_post(client, "x.com", "1")

    assert "media" not in detail["x"]
    assert detail["x"]["captured_at"] is None
    assert detail["media"] == [
        {"url": "/assets/archived.jpg", "content_type": "image/jpeg"},
    ]

    for field in ("reply_count", "repost_count", "like_count", "view_count"):
        assert detail["x"][field] is None


async def test_detail_orders_context_and_replies_and_reuses_saved_media(
    client: AsyncClient, clickhouse: ClickHouseClient, assets: pathlib.Path,
) -> None:
    image = "https://pbs.twimg.com/media/reply.jpg"
    local = assets / "reply.jpg"
    local.touch()
    focal = tweet("102", "101", replies=1)
    reply = tweet("103", "102", handle="visitor", text=(
        "<script>alert(1)</script> Hello @author https://t.co/link"
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
    await insert(clickhouse, "posts", [x_post("102")])
    await insert(clickhouse, "assets", [asset("x.com", "103", image, local)])
    await insert(clickhouse, "raw_posts", [x_raw(focal)])

    detail = await get_post(client, "x.com", "102")
    capture = detail["x"]["conversation"]
    first, second = capture["replies"]

    assert detail["html"] == BODY
    assert (capture["status"], capture["captured_count"]) == ("captured", 2)
    assert [node["id"] for node in capture["context"]] == ["100", "101"]
    assert [node["id"] for node in capture["replies"]] == ["103", "104"]
    assert (first["depth"], second["depth"]) == (0, 1)
    assert not first["is_author"]
    assert second["is_author"]
    assert first["parent_id"] == "102"
    assert not first["missing_parent"]
    assert first["url"] == "https://x.com/visitor/status/103"
    assert (first["x"]["name"], first["x"]["icon_url"]) == ("Visitor", ICON)
    assert "<script>" not in first["html"]
    assert "&lt;script&gt;" in first["html"]
    assert 'href="https://youtu.be/video"' in first["html"]
    assert first["x"]["media"][0]["url"] == "/assets/reply.jpg"
    assert not first["x"]["media"][0]["remote"]
    assert "conversation" not in first["x"]
    assert first["created_at"] == "2026-10-07T10:00:00+00:00"


async def test_feed_has_a_validated_summary_without_conversation_posts(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    focal = tweet("100", replies=2)
    focal["conversation_capture"] = archive([tweet("101", "100")])
    await insert(clickhouse, "posts", [x_post("100")])
    await insert(clickhouse, "raw_posts", [x_raw(focal)])

    response = await client.get("/api/reader/posts?kind=all")
    x = response.json()["posts"][0]["x"]

    assert x["replies_status"] == "partial"
    assert x["replies_captured_count"] == 1
    assert "conversation" not in x


@pytest.mark.parametrize("latest", ["failed", "disabled", "not_collected"])
@pytest.mark.parametrize("prior", ["captured", "partial"])
async def test_previous_archive_survives_a_later_attempt(
    client: AsyncClient,
    clickhouse: ClickHouseClient,
    latest: str,
    prior: str,
) -> None:
    old = tweet("100", handle="oldname", replies=24)
    old["conversation_capture"] = archive([
        tweet(str(101 + index), "100", handle="visitor")
        for index in range(24)
    ], status=prior, captured_at=OLDER.isoformat())
    current = tweet("100", text="New body", replies=99)

    if latest != "not_collected":
        current["conversation_capture"] = archive([], status=latest)

    await insert(clickhouse, "posts", [x_post("100")])
    await insert(clickhouse, "raw_posts", [
        x_raw(current), x_raw(old, scraped_at=OLDER),
    ])

    detail = await get_post(client, "x.com", "100")
    capture = detail["x"]["conversation"]

    assert detail["html"] == BODY
    assert (detail["x"]["handle"], detail["x"]["name"]) == ("author", "Author")
    assert detail["x"]["captured_at"] == DATE
    assert detail["x"]["reply_count"] == 99
    assert capture["status"] == prior
    assert capture["captured_at"] == OLDER.isoformat()
    assert capture["latest_attempt_status"] == latest
    assert capture["latest_attempt_at"] == DATE
    assert capture["reported_replies"] == 24
    assert [reply["id"] for reply in capture["replies"]] == [
        str(101 + index) for index in range(24)
    ]


@pytest.mark.parametrize("snapshot", ["newer", "wrong_target"])
async def test_history_rejects_unprocessed_or_mismatched_archives(
    client: AsyncClient, clickhouse: ClickHouseClient, snapshot: str,
) -> None:
    focal = tweet("100")
    old = copy.deepcopy(focal)
    old["conversation_capture"] = archive(
        [tweet("101", "100")], captured_at=OLDER.isoformat(),
    )

    if snapshot == "wrong_target":
        old["conversation_capture"]["target_id"] = "900"

    await insert(clickhouse, "posts", [x_post("100")])
    await insert(clickhouse, "raw_posts", [
        x_raw(focal),
        x_raw(old, scraped_at=LATER if snapshot == "newer" else OLDER),
    ])

    capture = (await get_post(client, "x.com", "100"))["x"]["conversation"]

    assert capture["status"] == "not_collected"
    assert capture["replies"] == []


async def test_latest_partial_archive_is_not_replaced_by_an_older_capture(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    focal = tweet("100", replies=2)
    focal["conversation_capture"] = archive(
        [tweet("102", "100")], status="partial",
    )
    older = tweet("100", replies=2)
    older["conversation_capture"] = archive(
        [tweet("101", "100")], captured_at=OLDER.isoformat(),
    )
    await insert(clickhouse, "posts", [x_post("100")])
    await insert(clickhouse, "raw_posts", [
        x_raw(focal), x_raw(older, scraped_at=OLDER),
    ])

    capture = (await get_post(client, "x.com", "100"))["x"]["conversation"]

    assert capture["captured_at"] == DATE
    assert [node["id"] for node in capture["replies"]] == ["102"]
    assert "latest_attempt_status" not in capture


async def test_archives_stay_with_their_own_post_and_domain(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    old = tweet("100")
    old["conversation_capture"] = archive(
        [], captured_at=OLDER.isoformat(),
    )
    await insert(clickhouse, "posts", [x_post("100"), x_post("200")])
    await insert(clickhouse, "raw_posts", [
        x_raw(tweet("100")),
        x_raw(tweet("200", root="200")),
        x_raw(old, scraped_at=OLDER),
        # The same post ID on another domain is a different post.
        x_raw(
            {**old, "rest_id": "200"}, scraped_at=OLDER, domain="twitter.com",
        ),
    ])

    response = await client.get("/api/reader/posts?kind=x")
    found = {item["id"]: item["x"] for item in response.json()["posts"]}

    assert found["100"]["replies_status"] == "captured"
    assert found["200"]["replies_status"] == "not_collected"


async def test_missing_exact_snapshot_can_recover_archive_without_old_profile(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    old = tweet("100", handle="oldname")
    old["conversation_capture"] = archive([], captured_at=OLDER.isoformat())
    await insert(clickhouse, "posts", [x_post("100")])
    await insert(clickhouse, "raw_posts", [x_raw(old, scraped_at=OLDER)])

    x = (await get_post(client, "x.com", "100"))["x"]

    assert (x["handle"], x["name"], x["icon_url"]) == (
        "author", "author", None,
    )
    assert x["captured_at"] is None
    assert x["conversation"]["status"] == "captured"
    assert x["conversation"]["captured_at"] == OLDER.isoformat()
    assert x["conversation"]["latest_attempt_status"] == "not_collected"


async def test_detail_connects_a_saved_parent_without_a_discussion(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [x_post("100"), x_post("101")])
    await insert(clickhouse, "raw_posts", [
        x_raw(tweet("100")), x_raw(tweet("101", "100")),
    ])

    x = (await get_post(client, "x.com", "101"))["x"]

    assert x["reply_to"]["collected"]
    assert x["reply_to"]["id"] == "100"
    assert x["reply_to"]["url"] == x_url("100")
    assert x["conversation"]["status"] == "not_collected"
    assert x["conversation"]["context"][0]["id"] == "100"
    assert x["conversation"]["context"][0]["collected"] is True


async def test_hacker_news_detail_adds_a_discussion_the_feed_leaves_out(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [post(
        "news.ycombinator.com", "1", source="hacker_news",
        url="https://news.ycombinator.com/item?id=1",
    )])
    await insert(clickhouse, "raw_posts", [raw_post(
        "news.ycombinator.com", "1", {
            "hckrnews": {"points": "120", "comments": 2,
                         "link": "https://example.com/story", "time": 1},
            "hacker_news": {"day": "2026-10-30", "rank": 3,
                            "filters": ["top10"]},
            "comments": [
                {"id": 2, "by": "alice", "text": "<p>Hello</p>", "time": 2},
                {"id": 3, "parent": 2, "by": "bob", "text": "Reply"},
                {"id": 4, "dead": True, "text": "gone"},
            ],
            "comment_capture": {"status": "captured", "total": 3},
        },
        source="hacker_news",
    )])

    listing = await client.get(
        "/api/reader/posts?kind=hacker_news&hn_filter=all",
    )
    feed = listing.json()["posts"][0]["hacker_news"]
    opened = await get_post(client, "news.ycombinator.com", "1")
    detail = opened["hacker_news"]

    assert feed["points"] == 120
    assert feed["external_url"] == "https://example.com/story"
    assert (feed["day"], feed["rank"]) == ("2026-10-30", 3)
    assert "comments_items" not in feed
    assert detail["comment_capture"] == {
        "status": "captured", "captured": 2, "total": 3, "reason": None,
    }
    assert [
        (item["id"], item["parent_id"], item["depth"])
        for item in detail["comments_items"]
    ] == [("2", "1", 0), ("3", "2", 1)]


async def test_youtube_posts_carry_their_saved_preview(
    client: AsyncClient, clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "posts", [post(
        "youtube.com", "video:abc", source="youtube", author="Channel",
        url="https://www.youtube.com/watch?v=abc",
    )])
    await insert(clickhouse, "raw_posts", [raw_post(
        "youtube.com", "video:abc",
        {"channel_id": "c1", "duration_text": "3:21", "views_text": "9 views"},
        source="youtube",
    )])

    response = await client.get("/api/reader/posts?kind=youtube")

    assert response.json()["posts"][0]["youtube"] == {
        "kind": "video", "video_id": "abc",
        "channel": {"id": "c1", "name": "Channel", "url": ""},
        "duration_text": "3:21", "views_text": "9 views",
        "thumbnail_url": None, "video_url": None,
    }
