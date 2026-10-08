from __future__ import annotations

import datetime as dt
import json
import pathlib
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.config import SourceConfig, load_settings
from dank.storage.clickhouse import QueryResult
from dank.web import reader_api
from dank.web.app import AssetRow, PostRow, create_app
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)


class FakeClickHouse:
    def __init__(self, *results: list[dict[str, Any]]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        self.calls.append((query, params or {}))

        return QueryResult(self.results.pop(0))


@pytest.fixture
def app(tmp_path: pathlib.Path) -> web.Application:
    settings = load_settings("config.example.toml")._replace(
        data_dir=tmp_path,
        sources=(
            SourceConfig("politics.example", (), tags=("politics",)),
            SourceConfig("empty.example", (), tags=("culture",)),
        ),
    )

    return create_app(settings, page_size=50)


async def request(app: web.Application, path: str) -> web.Response:
    req = make_mocked_request("GET", path, app=app)
    match = await app.router.resolve(req)
    match.add_app(app)
    req._match_info = match  # pyright: ignore[reportPrivateUsage]
    result = await reader_api.reader_errors(req, match.handler)
    assert isinstance(result, web.Response)

    return result


def row(domain: str, post_id: str = "same") -> dict[str, Any]:
    return {
        "domain": domain, "post_id": post_id,
        "url": "https://example.com/story", "author": "Reporter",
        "title": "An article", "html": "<p>Article body</p>",
        "created_at": "2026-10-08T12:30:45.123000+00:00",
        "updated_at": "2026-10-08T12:30:45.123000+00:00", "source": "rss",
    }


@pytest.mark.parametrize("query", [
    {"limit": "0"}, {"limit": "101"}, {"limit": "oops"},
    {"mode": "typo"}, {"sort": "random"}, {"sort": "relevance"},
    {"after": "2026-02-30"}, {"after": "20261008"},
    {"after": "2026-10-09", "before": "2026-10-08"},
    {"cursor": "bad"}, {"tag": ""}, {"unknown": "value"},
])
def test_invalid_filters_are_rejected(query: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


def test_filter_groups_are_intersected_and_all_words_are_literal() -> None:
    filters = parse_filters(MultiDict([
        ("domain", "news.example"), ("domain", "politics.example"),
        ("tag", "Politics"), ("author", "O'Reilly"),
        ("q", "50% x_y 'OR'"), ("after", "2026-10-01"),
        ("before", "2026-10-08"),
    ]))
    query, params = build_query(filters, (
        SourceConfig("politics.example", (), tags=("politics",)),
    ))
    assert "domain IN %(domains)s AND domain IN %(tag_domains)s" in query
    assert params["tag_domains"] == ["politics.example"]
    assert params["domains"] == ["news.example", "politics.example"]
    assert params["author"] == "O'Reilly"
    assert params["term_0"] == "50%"
    assert params["term_1"] == "x_y"
    assert params["term_2"] == "'OR'"
    assert "50%" not in query and "O'Reilly" not in query
    assert "extractTextFromHTML(html)" in query
    assert "decodeHTMLComponent" in query
    assert "created_at < toDateTime64(%(before)s, 3, 'UTC')" in query
    assert params["before"] == "2026-10-09 00:00:00"


def test_unknown_tag_does_not_fall_back_to_unfiltered_collection() -> None:
    query, _ = build_query(ReaderFilters(tags=("missing",)), ())
    assert "WHERE 0" in query


@pytest.mark.parametrize("sort,operator", [("newest", "<"), ("oldest", ">")])
def test_cursor_uses_domain_id_and_preserves_timestamp_precision(
    sort: str, operator: str,
) -> None:
    filters = ReaderFilters(sort=sort)
    created_at = dt.datetime(2026, 10, 8, 12, 30, 45, 123000, dt.UTC)
    token = encode_cursor(filters, created_at, "b.example", "same")
    paged = filters._replace(cursor=token)
    query, params = build_query(paged, ())
    assert f"(created_at, domain, post_id) {operator}" in query
    assert params["cursor_time"] == "2026-10-08 12:30:45.123000"
    assert params["cursor_domain"] == "b.example"
    assert params["cursor_id"] == "same"
    assert decode_cursor(paged) == (created_at, "b.example", "same")

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(paged._replace(author="different"))


def test_meaning_filters_before_ranking_and_requires_embedding() -> None:
    filters = ReaderFilters(q="transport", mode="meaning", sort="relevance")
    query, params = build_query(filters, (), (0.1, 0.2))
    assert "cosineDistance" in query
    assert "length(title_embedding) = length(%(embedding)s)" in query
    assert (
        "reader_score ASC, created_at DESC, domain DESC, post_id DESC" in query
    )
    assert params["embedding"] == [0.1, 0.2]
    assert "extractTextFromHTML" not in query

    with pytest.raises(RuntimeError, match="unavailable"):
        build_query(filters, ())


async def test_sources_include_empty_configured_and_collected_sources(
    app: web.Application,
) -> None:
    app["clickhouse"] = FakeClickHouse([
        {"domain": "politics.example", "count": 3},
        {"domain": "old.example", "count": 7},
    ])
    response = await request(app, "/api/reader/sources")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert data["total_posts"] == 10
    assert data["sources"][0] == {
        "domain": "empty.example", "name": "empty.example",
        "tags": ["culture"], "count": 0,
    }
    assert data["sources"][1]["tags"] == []
    assert data["sources"][2]["tags"] == ["politics"]


async def test_posts_paginate_without_cross_domain_asset_collisions(
    app: web.Application, tmp_path: pathlib.Path,
) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "b.jpg").touch()
    article = row("b.example")
    article["html"] = '<img src="https://example.com/b.jpg"><p>Body</p>'
    fake = FakeClickHouse(
        [row("c.example"), article, row("a.example")],
        [{"domain": "b.example", "post_id": "same",
          "url": "https://example.com/b.jpg",
          "local_path": str(assets / "b.jpg"), "content_type": "image/jpeg",
          "size_bytes": 0}],
    )
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?limit=2")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert len(data["posts"]) == 2
    assert data["posts"][0]["thumbnail"] is None
    assert data["posts"][1]["thumbnail"] == "/assets/b.jpg"
    assert data["posts"][1]["media"][0]["content_type"] == "image/jpeg"
    assert fake.calls[1][1]["post_keys"] == [
        ("c.example", "same"), ("b.example", "same"),
    ]
    assert data["next_cursor"]
    assert data["limited"] is False
    filters = ReaderFilters(limit=2, cursor=data["next_cursor"])
    assert decode_cursor(filters)[1:] == ("b.example", "same")


async def test_relevance_reports_capped_results(app: web.Application) -> None:
    app["clickhouse"] = FakeClickHouse(
        [row("b.example"), row("a.example")], [],
    )
    response = await request(
        app, "/api/reader/posts?q=article&sort=relevance&limit=1",
    )
    data = json.loads(response.text or "")
    assert len(data["posts"]) == 1
    assert data["limited"] is True
    assert data["next_cursor"] is None


async def test_empty_page_has_no_cursor(app: web.Application) -> None:
    app["clickhouse"] = FakeClickHouse([])
    response = await request(app, "/api/reader/posts")
    assert json.loads(response.text or "") == {
        "posts": [], "next_cursor": None, "limited": False,
    }


async def test_post_detail_returns_direct_object_and_not_found(
    app: web.Application,
) -> None:
    fake = FakeClickHouse([row("a.example")], [], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=a.example&id=same")
    assert json.loads(response.text or "")["id"] == "same"
    assert fake.calls[0][1] == {"domain": "a.example", "post_id": "same"}
    missing = await request(app, "/api/reader/post?domain=missing&id=missing")
    assert missing.status == 404


async def test_invalid_requests_are_json_errors(app: web.Application) -> None:
    response = await request(app, "/api/reader/posts?limit=broken")
    assert response.status == 400
    assert "error" in json.loads(response.text or "")
    response = await request(app, "/api/reader/post")
    assert response.status == 400


async def test_backend_failure_is_explicit_503(app: web.Application) -> None:
    app["clickhouse"] = FakeClickHouse()
    response = await request(app, "/api/reader/posts")
    assert response.status == 503
    assert "error" in json.loads(response.text or "")
    assert response.headers["Cache-Control"] == "no-store"


def test_article_sanitization_safe_urls_and_thumbnail(
    tmp_path: pathlib.Path,
) -> None:
    now = dt.datetime.now(dt.UTC)
    post = PostRow("example.com", "1", "https://example.com/story", "", "",
                   '<script>alert(1)</script><img src="/logo.svg">'
                   '<a href="javascript:alert(1)">unsafe</a>'
                   '<img src="https://[broken"><img src="/article.jpg" '
                   'onerror="alert(1)"><p>Readable &amp; clear</p>',
                   now, now, "rss")
    payload = reader_api.post_payload(post, [], tmp_path)
    assert "script" not in payload["html"]
    assert "javascript:" not in payload["html"]
    assert "onerror" not in payload["html"]
    assert payload["thumbnail"] == "https://example.com/article.jpg"
    assert "Readable & clear" in payload["excerpt"]
    unsafe = reader_api.post_payload(
        post._replace(url="javascript:alert(1)"), [], tmp_path,
    )
    assert unsafe["url"] == ""


def test_body_lead_image_wins_and_downloaded_images_are_reused(
    tmp_path: pathlib.Path,
) -> None:
    for name in ("unrelated.jpg", "portrait.jpg", "lead.jpg"):
        (tmp_path / name).touch()

    assets = [
        AssetRow("1", f"https://example.com/{name}",
                 str(tmp_path / name), "image/jpeg", 0)
        for name in ("unrelated.jpg", "portrait.jpg", "lead.jpg")
    ]
    now = dt.datetime.now(dt.UTC)
    post = PostRow("example.com", "1", "https://example.com/story", "", "",
                   '<img src="/logo.svg"><img src="/author-photo.jpg">'
                   '<img src="/lead.jpg" alt="A &amp; B"><p>Story</p>',
                   now, now, "rss")
    payload = reader_api.post_payload(post, assets, tmp_path)
    assert payload["thumbnail"] == "/assets/lead.jpg"
    assert '<img src="/assets/lead.jpg" alt="A &amp; B">' in payload["html"]
    assert "https://example.com/lead.jpg" not in payload["html"]
    assert "unrelated.jpg" not in payload["html"]
    no_image = reader_api.post_payload(
        post._replace(html="<p>No lead image</p>"), assets, tmp_path,
    )
    assert no_image["thumbnail"] is None


async def test_built_reader_routes_and_existing_viewer_are_independent(
    app: web.Application, tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "index.html").write_text("reader")
    (tmp_path / "app.js").write_text("// reader")
    monkeypatch.setattr(reader_api, "READER_DIST", tmp_path)
    req = make_mocked_request("GET", "/reader/", app=app)
    req._match_info = await app.router.resolve(req)  # pyright: ignore[reportPrivateUsage]
    response = await reader_api.handle_reader(req)
    assert isinstance(response, web.FileResponse)
    assert response.headers["Cache-Control"] == "no-store"
    root = await app.router.resolve(make_mocked_request("GET", "/", app=app))
    assert root.handler.__name__ == "handle_index"
    assert reader_api._reader_file("../outside") is None  # pyright: ignore[reportPrivateUsage]
    assert reader_api._reader_file("missing.js") is None  # pyright: ignore[reportPrivateUsage]
    assert reader_api._reader_file("saved/politics") == tmp_path / "index.html"  # pyright: ignore[reportPrivateUsage]
