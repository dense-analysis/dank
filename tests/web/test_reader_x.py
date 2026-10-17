from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
from aiohttp import web
from test_reader import FakeClickHouse, request, row

from dank.config import SourceConfig, load_settings
from dank.web.app import create_app
from dank.web.reader_accounts import X_ACCOUNT_SQL

ICON = "https://pbs.twimg.com/profile_images/123/account_normal.png"


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
    fake = FakeClickHouse(
        [x_post, other_x_post, twitter_post, reddit_post, row("blog.example")],
        [], [],
        [{"domain": "x.com", "handle": "archived", "profile": json.dumps({
            "core": {"screen_name": "Archived", "name": "Archived account"},
            "avatar": {"image_url": ICON},
        })}, {"domain": "twitter.com", "handle": "legacy", "profile":
              json.dumps({"legacy": {"screen_name": "LEGACY", "name": "Old",
                                      "profile_image_url_https": ICON}})}],
        [], [],
    )
    app["clickhouse"] = fake
    response = await request(app, path)
    assert response.status == 200
    posts = json.loads(response.text or "")["posts"]
    assert posts[0]["x"] == posts[1]["x"] == {
        "handle": "archived", "name": "Archived account", "icon_url": ICON,
    }
    assert posts[0]["author"] == "Edited display name"
    assert posts[2]["x"] == {
        "handle": "legacy", "name": "Old", "icon_url": ICON,
    }
    assert posts[3]["reddit"]["community"] == "python"
    assert all("x" not in post for post in posts[3:])
    assert len(fake.calls) == 6
    query, params = fake.calls[3]
    assert X_ACCOUNT_SQL in query
    assert "GROUP BY domain, handle" in query
    assert params == {
        "accounts": [("twitter.com", "legacy"), ("x.com", "archived")],
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
        "domain": "x.com", "handle": "example",
        "profile": (
            profile if isinstance(profile, str) else json.dumps(profile)
        ),
    }]
    fake = FakeClickHouse([post], [], profiles)
    app["clickhouse"] = fake
    response = await request(app, path)
    assert response.status == 200
    data = json.loads(response.text or "")
    result = data["posts"][0] if "posts" in data else data
    assert result["x"] == expected
    assert len(fake.calls) == 3
    assert fake.calls[2][1] == {"accounts": [("x.com", "example")]}


@pytest.mark.parametrize("url", [
    "https://x.com.evil.example/example/status/1",
    "https://example.com/example/status/1",
    "https://x.com/a-hyphen/status/1",
    "https://x.com/handleistoolong16/status/1",
    "https://x.com/i/web/status/1",
])
async def test_x_posts_without_canonical_handles_do_not_guess_from_author(
    app: web.Application, url: str,
) -> None:
    fake = FakeClickHouse([
        {**row("x.com"), "source": "x", "author": "@Example", "url": url},
    ], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=x")
    assert response.status == 200
    assert "x" not in json.loads(response.text or "")["posts"][0]
    assert len(fake.calls) == 2


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
