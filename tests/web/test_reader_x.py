from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
from aiohttp import web
from test_reader import FakeClickHouse, request, row

from dank.config import SourceConfig, load_settings
from dank.web.app import create_app

ICON = "https://pbs.twimg.com/profile_images/123/account_normal.png"
EMPTY_CONTENT: dict[str, Any] = {
    "links": [], "reply_count": None,
    "replies_status": "not_collected", "captured_at": None,
    "replies_captured_count": 0,
}
CAPTURED_CONTENT: dict[str, Any] = {
    **EMPTY_CONTENT, "media": [],
    "captured_at": "2026-10-08T12:30:45.123000+00:00",
}


@pytest.fixture
def app(tmp_path: pathlib.Path) -> web.Application:
    settings = load_settings("config.example.toml")._replace(
        data_dir=tmp_path, sources=(SourceConfig("x.com", ("configured",)),),
    )

    return create_app(settings, page_size=50)


@pytest.mark.parametrize("path", [
    "/api/reader/posts", "/api/reader/posts?kind=all",
])
async def test_mixed_feed_batches_x_profiles_without_cross_domain_collisions(
    app: web.Application, path: str,
) -> None:
    x_post = {
        **row("x.com"), "source": "x", "author": "Edited display name",
        "url": "https://x.com/Archived/status/123",
    }
    other_x_post = {**x_post, "post_id": "456"}
    twitter_post = {
        **row("twitter.com"), "source": "x",
        "url": "https://www.twitter.com/Legacy/status/123",
    }
    reddit_post = {
        **row("reddit.com"), "source": "reddit",
        "url": "https://www.reddit.com/r/python/comments/abc/title/",
    }
    profile = {
        "core": {"screen_name": "Archived", "name": "Archived account"},
        "avatar": {"image_url": ICON},
    }
    old_profile = {"legacy": {"screen_name": "LEGACY", "name": "Old",
                              "profile_image_url_https": ICON}}
    fake = FakeClickHouse(
        [x_post, other_x_post, twitter_post, reddit_post, row("blog.example")],
        [], [],
        [{"domain": "x.com", "post_id": post_id, "payload": json.dumps({
            "core": {"user_results": {"result": profile}},
        })} for post_id in ("same", "456")] + [{
            "domain": "twitter.com", "post_id": "same", "payload": json.dumps({
                "core": {"user_results": {"result": old_profile}},
            }),
        }],
        [], [],
    )
    app["clickhouse"] = fake
    response = await request(app, path)
    assert response.status == 200
    posts = json.loads(response.text or "")["posts"]
    assert posts[0]["x"] == posts[1]["x"] == {
        "handle": "archived", "name": "Archived account", "icon_url": ICON,
        **CAPTURED_CONTENT,
    }
    assert posts[0]["author"] == "Edited display name"
    assert posts[2]["x"] == {
        "handle": "legacy", "name": "Old", "icon_url": ICON,
        **CAPTURED_CONTENT,
    }
    assert posts[3]["reddit"]["community"] == "python"
    assert all("x" not in post for post in posts[3:])
    assert len(fake.calls) == 6
    query, params = fake.calls[3]
    assert "toUnixTimestamp64Milli(scraped_at)" in query
    assert "LIMIT 1 BY domain, post_id" in query
    assert params == {
        "x_versions": [("x.com", "same", 1791462645123),
                       ("x.com", "456", 1791462645123),
                       ("twitter.com", "same", 1791462645123)],
    }


@pytest.mark.parametrize("path", [
    "/api/reader/posts?kind=x", "/api/reader/post?domain=x.com&id=same",
])
@pytest.mark.parametrize("profile,expected", [
    ({"legacy": {"screen_name": "Example", "name": "Legacy name",
                 "profile_image_url_https": ICON}},
     {"handle": "example", "name": "Legacy name", "icon_url": ICON}),
    ({"core": {"screen_name": "other", "name": "Wrong account"},
      "avatar": {"image_url": ICON}},
     {"handle": "example", "name": "example", "icon_url": None}),
    ({"core": {"screen_name": "Example", "name": "Example"},
      "avatar": {"image_url": "https://evil.example/avatar.png"}},
     {"handle": "example", "name": "Example", "icon_url": None}),
    ({"core": None, "legacy": []},
     {"handle": "example", "name": "example", "icon_url": None}),
    ([], {"handle": "example", "name": "example", "icon_url": None}),
    ("broken", {"handle": "example", "name": "example", "icon_url": None}),
    (None, {"handle": "example", "name": "example", "icon_url": None}),
])
async def test_x_cards_and_details_use_captured_profile_or_handle_fallback(
    app: web.Application, path: str, profile: Any, expected: dict[str, Any],
) -> None:
    post = {
        **row("x.com"), "source": "x",
        "url": "https://x.com/EXAMPLE/status/123",
    }
    profiles = [] if profile is None else [{
        "domain": "x.com", "post_id": "same",
        "payload": (
            profile if isinstance(profile, str) else json.dumps({
                "core": {"user_results": {"result": profile}},
            })
        ),
    }]
    fake = FakeClickHouse([post], [], profiles)
    app["clickhouse"] = fake
    response = await request(app, path)
    assert response.status == 200
    data = json.loads(response.text or "")
    result = data["posts"][0] if "posts" in data else data
    captured = CAPTURED_CONTENT if profile is not None and not isinstance(
        profile, str,
    ) else EMPTY_CONTENT
    if "/post?" in path:
        conversation = result["x"].pop("conversation")
        assert conversation["status"] == "not_collected"
        assert conversation["context"] == conversation["replies"] == []

    assert result["x"] == {**expected, **captured}
    assert len(fake.calls) == 3
    assert fake.calls[2][1] == {
        "x_versions": [("x.com", "same", 1791462645123)],
    }


@pytest.mark.parametrize("url", [
    "https://x.com.evil.example/example/status/1",
    "https://example.com/example/status/1",
    "https://x.com/a-hyphen/status/1",
    "https://x.com/handleistoolong16/status/1",
    "https://x.com/i/web/status/1", "https://x.com/i/status/1",
])
async def test_x_posts_without_canonical_handles_do_not_guess_from_author(
    app: web.Application, url: str,
) -> None:
    fake = FakeClickHouse([
        {**row("x.com"), "source": "x", "author": "@Example", "url": url},
    ], [], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=x")
    assert response.status == 200
    assert json.loads(response.text or "")["posts"][0]["x"] == {
        "handle": "", "name": "", "icon_url": None, **EMPTY_CONTENT,
    }
    assert len(fake.calls) == 3


async def test_non_x_posts_do_not_query_profiles_even_with_x_links(
    app: web.Application,
) -> None:
    fake = FakeClickHouse([
        {**row("blog.example"), "url": "https://x.com/example/status/1"},
    ], [], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=stories")
    assert response.status == 200
    assert "x" not in json.loads(response.text or "")["posts"][0]
    assert len(fake.calls) == 3
    assert all("AS profile" not in query for query, _ in fake.calls)


@pytest.mark.parametrize("path", [
    "/api/reader/posts?kind=all", "/api/reader/posts?kind=x",
    "/api/reader/post?domain=x.com&id=same",
])
@pytest.mark.parametrize("file_exists", [False, True])
async def test_x_content_uses_saved_capture_and_existing_local_media(
    app: web.Application, tmp_path: pathlib.Path, path: str,
    *, file_exists: bool,
) -> None:
    image = "https://pbs.twimg.com/media/photo.jpg"
    image_file = tmp_path / "assets" / "photo.jpg"

    if file_exists:
        image_file.touch()

    payload = {
        "core": {"user_results": {"result": {
            "core": {"screen_name": "Example", "name": "Example Person"},
            "avatar": {"image_url": ICON},
        }}},
        "legacy": {
            "reply_count": 24, "full_text": "Same capture",
            "extended_entities": {"media": [{
                "type": "photo", "media_url_https": image,
            }]},
            "entities": {"urls": [{
                "url": "https://t.co/abc", "expanded_url": "https://youtu.be/id",
            }]},
        },
    }
    fake = FakeClickHouse([
        {**row("x.com"), "source": "x", "url": "https://x.com/Example/status/1",
         "html": "<p>Processed body</p>"},
    ], [{"domain": "x.com", "post_id": "same", "url": image,
         "local_path": str(image_file), "content_type": "image/jpeg",
         "size_bytes": 0}], [{
        "domain": "x.com", "post_id": "same", "payload": json.dumps(payload),
    }])
    app["clickhouse"] = fake
    response = await request(app, path)
    assert response.status == 200
    result = json.loads(response.text or "")
    post = result["posts"][0] if "posts" in result else result
    assert post["html"] == "<p>Processed body</p>"
    if "/post?" in path:
        conversation = post["x"].pop("conversation")
        assert conversation["reported_replies"] == 24
        assert conversation["status"] == "not_collected"

    assert post["x"] == {
        "handle": "example", "name": "Example Person", "icon_url": ICON,
        "media": [{"type": "image",
                   "url": "/assets/photo.jpg" if file_exists else image,
                   "poster_url": None, "alt": "", "remote": not file_exists}],
        "links": [{"url": "https://youtu.be/id", "title": "",
                   "description": "", "image_url": None}],
        "reply_count": 24, "replies_status": "not_collected",
        "replies_captured_count": 0,
        "captured_at": "2026-10-08T12:30:45.123000+00:00",
    }
    assert fake.calls[2][1] == {
        "x_versions": [("x.com", "same", 1791462645123)],
    }
    assert "(domain, post_id, toUnixTimestamp64Milli(scraped_at))" in (
        fake.calls[2][0]
    )


@pytest.mark.parametrize("count", [True, -1, 2 ** 53, "24", None])
async def test_x_unknown_reply_counts_never_claim_zero(
    app: web.Application, count: object,
) -> None:
    fake = FakeClickHouse([
        {**row("x.com"), "source": "x", "url": "https://x.com/Example/status/1"},
    ], [], [{"domain": "x.com", "post_id": "same", "payload": json.dumps({
        "legacy": {"reply_count": count},
    })}])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=same")
    assert response.status == 200
    post = json.loads(response.text or "")
    assert post["x"]["reply_count"] is None
    assert post["x"]["replies_status"] == "not_collected"
    assert post["html"] == "<p>Article body</p>"


@pytest.mark.parametrize("payload", [None, "broken", "[]", "{}"])
async def test_missing_x_snapshot_keeps_legacy_downloaded_media_available(
    app: web.Application, tmp_path: pathlib.Path, payload: str | None,
) -> None:
    image_file = tmp_path / "assets" / "archived.jpg"
    image_file.touch()
    metadata = [] if payload is None else [{
        "domain": "x.com", "post_id": "same", "payload": payload,
    }]
    fake = FakeClickHouse([
        {**row("x.com"), "source": "x", "url": "https://x.com/Example/status/1"},
    ], [{"domain": "x.com", "post_id": "same",
         "url": "https://pbs.twimg.com/media/archived.jpg",
         "local_path": str(image_file), "content_type": "image/jpeg",
         "size_bytes": 0}], metadata)
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=same")
    assert response.status == 200
    post = json.loads(response.text or "")
    assert "media" not in post["x"]
    assert post["x"]["captured_at"] is None
    assert post["media"] == [{
        "url": "/assets/archived.jpg", "content_type": "image/jpeg",
    }]
