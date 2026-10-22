from __future__ import annotations

import datetime as dt
import json
import pathlib
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.config import SourceConfig, load_settings
from dank.model import RawPost
from dank.process.rss import convert_raw_post
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

        if params and any(key.startswith("provenance_") for key in params):
            return QueryResult([])

        if params and ({"link_post_keys", "hn_link_keys",
                        "x_parent_keys", "x_thread_domain"} & params.keys()):
            return QueryResult([])

        if "AS blog_external_count" in query:
            return QueryResult([{"blog_external_count": 4}])

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
    {"author_match": "typo"},
])
def test_invalid_filters_are_rejected(query: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


@pytest.mark.parametrize("mode", ["words", "combined"])
def test_filter_groups_are_intersected_and_all_words_are_literal(
    mode: str,
) -> None:
    filters = parse_filters(MultiDict([
        ("domain", "news.example"), ("domain", "politics.example"),
        ("tag", "Politics"), ("author", "O'Reilly"),
        ("q", "50% x_y 'OR'"), ("mode", mode), ("after", "2026-10-01"),
        ("before", "2026-10-08"),
    ]))
    query, params = build_query(filters, (
        SourceConfig("politics.example", (), tags=("politics",)),
    ), (0.1, 0.2))
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


def test_exact_author_name_is_parameterized() -> None:
    filters = parse_filters(MultiDict({
        "author": " O'Reilly ", "author_match": "exact",
        "domain": "news.example", "after": "2026-10-01",
    }))
    query, params = build_query(filters, ())
    assert "lowerUTF8(trimBoth(author)) = lowerUTF8(%(author)s)" in query
    assert "positionCaseInsensitiveUTF8(author" not in query
    assert params["author"] == "O'Reilly"
    assert "O'Reilly" not in query
    assert params["domains"] == ["news.example"]
    assert "created_at >=" in query
    partial, _ = build_query(filters._replace(author_match="contains"), ())
    assert "positionCaseInsensitiveUTF8(author, %(author)s) > 0" in partial


def test_author_match_mode_is_part_of_cursor_identity() -> None:
    filters = ReaderFilters(author="Ada", author_match="exact")
    created = dt.datetime(2026, 10, 8, tzinfo=dt.UTC)
    cursor = encode_cursor(filters, created, "example.com", "1")
    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(filters._replace(author_match="contains", cursor=cursor))


@pytest.mark.parametrize("sort,operator", [("newest", "<"), ("oldest", ">")])
@pytest.mark.parametrize("mode", ["words", "meaning", "combined"])
def test_cursor_uses_domain_id_and_preserves_timestamp_precision(
    sort: str, operator: str, mode: str,
) -> None:
    filters = ReaderFilters(q="article", mode=mode, sort=sort)
    created_at = dt.datetime(2026, 10, 8, 12, 30, 45, 123000, dt.UTC)
    token = encode_cursor(filters, created_at, "b.example", "same")
    paged = filters._replace(cursor=token)
    query, params = build_query(paged, (), (0.1, 0.2))
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


def test_combined_search_prioritizes_literal_then_title_matches() -> None:
    filters = parse_filters(MultiDict({
        "q": "domain driven design", "mode": "combined", "sort": "relevance",
    }))
    query, params = build_query(filters, (), (1.0, 0.0))
    assert query.startswith("WITH ")
    assert ") > 0 AND positionCaseInsensitiveUTF8" in query
    assert "if(reader_literal_match, -toInt32(" in query
    assert "(reader_literal_match OR reader_distance <= 1.0)" in query
    assert (
        "ORDER BY reader_literal_match DESC, reader_title_score ASC, "
        "reader_score ASC, created_at DESC, domain DESC, post_id DESC" in query
    )
    assert "FROM posts FINAL" in query
    assert "UNION" not in query
    assert [params[f"term_{index}"] for index in range(3)] == [
        "domain", "driven", "design",
    ]
    # Missing or incompatible vectors cannot disqualify a literal match.
    assert "WHERE (reader_literal_match OR reader_distance <= 1.0)" in query
    assert query.count("cosineDistance(if(length(title_embedding)") == 2
    assert ", 2.0) AS reader_distance" in query

    with pytest.raises(RuntimeError, match="unavailable"):
        build_query(filters, ())


async def test_combined_posts_requests_embedding_and_reports_relevance_cap(
    app: web.Application, monkeypatch: pytest.MonkeyPatch,
) -> None:
    embed = AsyncMock(return_value=(1.0, 0.0))
    monkeypatch.setattr(reader_api, "_search_embedding", embed)
    fake = FakeClickHouse([row("b.example"), row("a.example")], [], [])
    app["clickhouse"] = fake
    response = await request(
        app,
        "/api/reader/posts?q=article&mode=combined&sort=relevance&limit=1",
    )
    data = json.loads(response.text or "")
    embed.assert_awaited_once_with(fake, search_text="article")
    assert response.status == 200
    assert len(data["posts"]) == 1
    assert data["limited"] is True
    assert data["next_cursor"] is None
    assert fake.calls[0][1]["embedding"] == [1.0, 0.0]
    assert "reader_literal_match DESC" in fake.calls[0][0]


@pytest.mark.parametrize("mode", ["words", "meaning", "combined"])
async def test_browsing_needs_no_embedding(
    mode: str, app: web.Application, monkeypatch: pytest.MonkeyPatch,
) -> None:
    embed = AsyncMock(side_effect=RuntimeError("Embedding unavailable"))
    monkeypatch.setattr(reader_api, "_search_embedding", embed)
    fake = FakeClickHouse([])
    app["clickhouse"] = fake
    response = await request(app, f"/api/reader/posts?mode={mode}")
    assert response.status == 200
    embed.assert_not_awaited()
    assert "cosineDistance" not in fake.calls[0][0]


@pytest.mark.parametrize("embedding", [None, ()])
async def test_combined_embedding_unavailable_is_an_explicit_error(
    embedding: tuple[float, ...] | None, app: web.Application,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    embed = AsyncMock(return_value=embedding)
    monkeypatch.setattr(reader_api, "_search_embedding", embed)
    fake = FakeClickHouse([])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?q=article&mode=combined")
    assert response.status == 503
    assert "error" in json.loads(response.text or "")
    assert fake.calls == []


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
    assert data["blog_external_count"] == 4
    assert data["sources"][0] == {
        "domain": "empty.example", "name": "empty.example",
        "tags": ["culture"], "count": 0, "kind": "rss", "configured": True,
    }
    assert data["sources"][1]["configured"] is False
    assert data["sources"][2]["configured"] is True
    assert data["sources"][1]["tags"] == []
    assert data["sources"][2]["tags"] == ["politics"]


async def test_x_sources_include_configured_accounts_and_exclude_other_authors(
    app: web.Application,
) -> None:
    state = app["state"]
    app["state"] = state._replace(settings=state.settings._replace(sources=(
        SourceConfig("x.com", ("@WatcherGuru", "Empty_Account")),
    )))
    icon = "https://pbs.twimg.com/profile_images/123/profile_normal.png"
    app["clickhouse"] = FakeClickHouse(
        [{"domain": "x.com", "count": 9},
         {"domain": "reddit.com", "count": 3}],
        [{"handle": "watcherguru", "count": 7},
         {"handle": "oldaccount", "count": 2}],
        [{"domain": "x.com", "handle": "watcherguru", "profile": json.dumps({
            "core": {"screen_name": "WatcherGuru", "name": "Watcher.Guru"},
            "avatar": {"image_url": icon},
        })}],
    )
    response = await request(app, "/api/reader/sources")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert data["total_posts"] == 12
    assert data["sources"][0]["kind"] == "reddit"
    assert data["sources"][1]["kind"] == "x"
    assert data["sources"][1]["accounts"] == [
        {"handle": "empty_account", "name": "empty_account", "count": 0,
         "icon_url": None, "total_posts": None},
        {"handle": "watcherguru", "name": "Watcher.Guru", "count": 7,
         "icon_url": icon, "total_posts": None},
    ]


async def test_x_account_api_keeps_search_and_returns_a_bound_cursor(
    app: web.Application,
) -> None:
    first = row("x.com", "2")
    first.update(url="https://x.com/Example/status/2", source="x")
    second = row("x.com", "1")
    second.update(url="https://x.com/Example/status/1", source="x")
    fake = FakeClickHouse([first, second], [], [], [])
    app["clickhouse"] = fake
    path = "/api/reader/posts?kind=x&account=Example&q=article&limit=1"
    response = await request(app, path)
    data = json.loads(response.text or "")
    assert response.status == 200
    assert len(data["posts"]) == 1
    assert data["posts"][0]["domain"] == "x.com"
    assert fake.calls[0][1]["account"] == "example"
    assert fake.calls[0][1]["term_0"] == "article"
    filters = parse_filters(MultiDict({
        "kind": "x", "account": "EXAMPLE", "q": "article", "limit": "1",
        "cursor": data["next_cursor"],
    }))
    assert decode_cursor(filters)[1:] == ("x.com", "2")

    response = await request(
        app, path.replace("account=Example", "account=other")
        + "&cursor=" + data["next_cursor"].replace("=", "%3D"),
    )
    assert response.status == 400


async def test_reddit_sources_include_configured_empty_communities(
    app: web.Application,
) -> None:
    state = app["state"]
    app["state"] = state._replace(settings=state.settings._replace(sources=(
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/LocalLLaMA/",
            "https://www.reddit.com/r/accelerate/",
        )),
    )))
    fake = FakeClickHouse(
        [{"domain": "reddit.com", "count": 7}],
        [{"community": "localllama", "count": 7}], [], [], [],
    )
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/sources")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert data["sources"][0]["kind"] == "reddit"
    assert data["sources"][0]["communities"] == [
        {"name": "accelerate", "count": 0, "icon_url": None,
         "flairs": [], "post_total": None},
        {"name": "LocalLLaMA", "count": 7, "icon_url": None,
         "flairs": [], "post_total": None},
    ]


def test_reddit_filters_are_separate_parameterized_and_case_insensitive():
    filters = parse_filters(MultiDict({
        "kind": "reddit", "community": "LocalLLaMA", "q": "model",
    }))
    query, params = build_query(filters, ())
    assert "domain = 'reddit.com'" in query
    assert "lowerUTF8(extract(url, '/r/([^/]+)/comments/'))" in query
    assert params["community"] == "localllama"
    assert "LocalLLaMA" not in query
    stories, _ = build_query(ReaderFilters(kind="stories"), ())
    assert "source IN ('rss', 'linked_article', 'hacker_news')" in stories


@pytest.mark.parametrize("query", [
    {"kind": "unknown"}, {"community": "python"},
    {"kind": "stories", "community": "python"},
    {"kind": "reddit", "community": "python' OR 1"},
    {"kind": "reddit", "community": "r/python"},
])
def test_invalid_reddit_filters(query: dict[str, str]):
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


def test_reddit_cursor_cannot_leak_across_communities():
    filters = ReaderFilters(kind="reddit", community="python")
    token = encode_cursor(
        filters, dt.datetime.now(dt.UTC), "reddit.com", "t3_a",
    )

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(filters._replace(community="localllama", cursor=token))


async def test_reddit_metadata_uses_processed_snapshot_and_retains_safe_html(
    app: web.Application,
) -> None:
    post = row("reddit.com", "t3_abc")
    post.update(
        source="reddit",
        url="https://www.reddit.com/r/LocalLLaMA/comments/abc/model/",
        html='<p>Body</p><script>alert(1)</script>',
    )
    fake = FakeClickHouse([post], [], [{
        "post_id": "t3_abc", "payload": json.dumps({
            "subreddit": "other", "score": 123, "num_comments": 42,
            "link_flair_text": "Discussion",
        }),
    }], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?kind=reddit")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert data["posts"][0]["reddit"] == {
        "community": "LocalLLaMA", "score": 123, "comments": 42,
        "flair": "Discussion", "icon_url": None,
        "video": None, "videos": [],
    }
    assert "<script>" not in data["posts"][0]["html"]
    assert "ORDER BY scraped_at DESC LIMIT 1 BY post_id" in fake.calls[2][0]
    assert fake.calls[2][1] == {
        "reddit_versions": [("t3_abc", 1791462645123)],
    }
    assert "toUnixTimestamp64Milli(scraped_at)" in fake.calls[2][0]


@pytest.mark.parametrize("payload", [
    "broken", "[]", '{"score":true}',
    '{"score":9007199254740992,"num_comments":-1}',
])
async def test_reddit_posts_remain_readable_without_valid_metadata(
    app: web.Application, payload: str,
) -> None:
    post = row("reddit.com", "t3_abc")
    post["url"] = "https://www.reddit.com/r/python/comments/abc/title/"
    post["source"] = "reddit"
    app["clickhouse"] = FakeClickHouse([post], [], [{
        "post_id": "t3_abc", "payload": payload,
    }], [])
    response = await request(app, "/api/reader/posts?kind=reddit")
    assert response.status == 200
    metadata = json.loads(response.text or "")["posts"][0]["reddit"]
    assert metadata["score"] is None
    assert metadata["comments"] is None
    assert metadata["community"] == "python"


async def test_posts_paginate_without_cross_domain_asset_collisions(
    app: web.Application, tmp_path: pathlib.Path,
) -> None:
    assets = tmp_path / "assets"
    assets.mkdir(exist_ok=True)
    (assets / "b.jpg").touch()
    article = row("b.example")
    article["html"] = '<img src="https://example.com/b.jpg"><p>Body</p>'
    fake = FakeClickHouse(
        [row("c.example"), article, row("a.example")],
        [{"domain": "b.example", "post_id": "same",
          "url": "https://example.com/b.jpg",
          "local_path": str(assets / "b.jpg"), "content_type": "image/jpeg",
          "size_bytes": 0}],
        [{"domain": "b.example", "post_id": "same"}],
    )
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/posts?limit=2")
    data = json.loads(response.text or "")
    assert response.status == 200
    assert len(data["posts"]) == 2
    assert data["posts"][0]["thumbnail"] is None
    assert data["posts"][1]["thumbnail"] == "/assets/b.jpg"
    assert data["posts"][1]["media"][0]["content_type"] == "image/jpeg"
    assert data["posts"][0]["collection_incomplete"] is False
    assert data["posts"][1]["collection_incomplete"] is True
    assert fake.calls[1][1]["post_keys"] == [
        ("c.example", "same"), ("b.example", "same"),
    ]
    assert fake.calls[2][1]["post_versions"] == [
        (domain, "same", 1791462645123)
        for domain in ("c.example", "b.example")
    ]
    assert data["next_cursor"]
    assert data["limited"] is False
    filters = ReaderFilters(limit=2, cursor=data["next_cursor"])
    assert decode_cursor(filters)[1:] == ("b.example", "same")


async def test_relevance_reports_capped_results(app: web.Application) -> None:
    app["clickhouse"] = FakeClickHouse(
        [row("b.example"), row("a.example")], [], [],
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
    fake = FakeClickHouse([row("a.example")], [], [], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=a.example&id=same")
    assert json.loads(response.text or "")["id"] == "same"
    assert fake.calls[0][1] == {"domain": "a.example", "post_id": "same"}
    missing = await request(app, "/api/reader/post?domain=missing&id=missing")
    assert missing.status == 404


@pytest.mark.parametrize("failed", [True, False])
async def test_article_with_text_reports_its_page_capture_failure(
    app: web.Application, *, failed: bool,
) -> None:
    article = row("a.example")
    failure = [{"domain": "a.example", "post_id": "same"}] if failed else []
    fake = FakeClickHouse([article], [], failure)
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=a.example&id=same")
    data = json.loads(response.text or "")

    assert response.status == 200
    assert data["html"] == article["html"]
    assert data["collection_incomplete"] is failed
    assert fake.calls[2][1]["post_versions"] == [
        ("a.example", "same", 1791462645123),
    ]


async def test_social_post_does_not_need_rss_page_capture(
    app: web.Application,
) -> None:
    article = {**row("x.com"), "source": "x"}
    fake = FakeClickHouse([article], [], [])
    app["clickhouse"] = fake
    response = await request(app, "/api/reader/post?domain=x.com&id=same")

    assert response.status == 200
    assert json.loads(response.text or "")["collection_incomplete"] is False
    assert len(fake.calls) == 4
    assert "link_post_keys" in fake.calls[-1][1]
    assert all("page_fetch_status" not in query for query, _ in fake.calls)


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


def test_reader_payload_retains_table_groups_and_merged_cells(
    tmp_path: pathlib.Path,
) -> None:
    body = (pathlib.Path(__file__).parents[1] / "fixtures"
            / "reader-table.html").read_text()
    now = dt.datetime.now(dt.UTC)
    post = PostRow(
        "example.com", "table", "https://example.com/table", "", "Table",
        body, now, now, "rss",
    )
    payload = reader_api.post_payload(post, [], tmp_path)
    assert '<th colspan="2" scope="colgroup">' in payload["html"]
    assert '<th rowspan="2" scope="rowgroup">' in payload["html"]
    assert '<caption>Benchmark comparison</caption>' in payload["html"]
    assert '<tfoot>' in payload["html"]
    assert "onclick" not in payload["html"]
    assert "style=" not in payload["html"]


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


def test_stored_wordpress_caption_reaches_reader_without_reprocessing(
    tmp_path: pathlib.Path,
) -> None:
    now = dt.datetime.now(dt.UTC)
    post = PostRow(
        "example.com", "caption", "https://example.com/story", "", "Caption",
        '<p>Before.</p><div class="wp-caption aligncenter"'
        ' style="width:600px">'
        '<img src="/image.png" alt="Diagram" aria-describedby="caption-1">'
        '<p class="wp-caption-text" id="caption-1">Credit: '
        '<a href="/credit">Example Lab</a></p></div><p>After.</p>',
        now, now, "rss",
    )
    image = tmp_path / "image.png"
    image.touch()
    payload = reader_api.post_payload(post, [AssetRow(
        "caption", "https://example.com/image.png", str(image), "image/png", 0,
    )], tmp_path)

    assert payload["html"] == (
        '<p>Before.</p><figure><img src="/assets/image.png" alt="Diagram">'
        '<figcaption>Credit: <a href="https://example.com/credit">Example Lab'
        '</a></figcaption></figure><p>After.</p>'
    )


def test_recovered_figure_reaches_reader_with_caption_and_local_image(
    tmp_path: pathlib.Path,
) -> None:
    now = dt.datetime.now(dt.UTC)
    raw = RawPost(
        domain="example.com", post_id="figure", url="https://example.com/1",
        post_created_at=now, scraped_at=now, source="rss", request_url="",
        payload=json.dumps({
            "feed_xml": '<item xmlns:c="http://purl.org/rss/1.0/modules/'
            'content/"><title>Diagram</title><c:encoded>Before. After.'
            '</c:encoded></item>',
            "page_html": '<article><p>Before.</p><figure onclick="attack()">'
            '<picture><source srcset="/large.png 2x">'
            '<img src="/diagram.png" alt="The diagram" onerror="attack()">'
            '</picture><figcaption style="position:fixed">Credit: Lab'
            '</figcaption></figure><p>After.</p></article>',
        }),
    )
    processed = convert_raw_post(raw)
    assert processed is not None
    image = tmp_path / "diagram.png"
    image.touch()
    post = PostRow(
        processed.domain, processed.post_id, processed.url, processed.author,
        processed.title, processed.html, now, now, processed.source,
    )
    payload = reader_api.post_payload(post, [AssetRow(
        "figure", "https://example.com/diagram.png", str(image),
        "image/png", 0,
    )], tmp_path)

    assert '<figure>' in payload["html"]
    assert '<img src="/assets/diagram.png" alt="The diagram">' in (
        payload["html"]
    )
    assert '<figcaption>Credit: Lab</figcaption>' in payload["html"]
    assert payload["html"].index("Before.") < payload["html"].index("<figure>")
    assert payload["html"].index("</figure>") < payload["html"].index("After.")
    assert all(unsafe not in payload["html"] for unsafe in (
        "onclick", "onerror", "position:fixed", "<source", "srcset",
    ))


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


async def test_community_identity_and_flair_choices_are_shared_with_posts(
    app: web.Application, tmp_path: pathlib.Path,
) -> None:
    assets = tmp_path / "assets"
    assets.mkdir(exist_ok=True)
    icon = assets / "community.png"
    icon.write_bytes(b"\x89PNG\r\n\x1a\n")
    state = app["state"]
    app["state"] = state._replace(settings=state.settings._replace(sources=(
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/LocalLLaMA/",
            "https://www.reddit.com/r/accelerate/",
        )),
    )))
    icon_row = {
        "community": "localllama", "icon_url": "https://styles.redditmedia.com/icon.png",
        "local_path": str(icon), "content_type": "image/png",
    }
    app["clickhouse"] = FakeClickHouse(
        [{"domain": "reddit.com", "count": 9}],
        [{"community": "localllama", "count": 7},
         {"community": "accelerate", "count": 2}],
        [icon_row], [],
        [{"community": "localllama", "name": "Discussion", "count": 4},
         {"community": "localllama", "name": "New Model", "count": 3},
         {"community": "accelerate", "name": "Discussion", "count": 2}],
    )
    response = await request(app, "/api/reader/sources")
    assert response.status == 200
    source = json.loads(response.text or "")["sources"][0]
    assert source["flairs"] == [
        {"name": "Discussion", "count": 6}, {"name": "New Model", "count": 3},
    ]
    assert source["communities"][0]["flairs"] == [
        {"name": "Discussion", "count": 2},
    ]
    assert source["communities"][1]["icon_url"] == "/assets/community.png"
    post = row("reddit.com", "t3_icon")
    post.update(source="reddit", url="https://www.reddit.com/r/LocalLLaMA/comments/icon/title/")
    app["clickhouse"] = FakeClickHouse([post], [], [{
        "post_id": "t3_icon", "payload": '{"link_flair_text":"  New Model  "}',
    }], [icon_row])
    response = await request(
        app, "/api/reader/post?domain=reddit.com&id=t3_icon",
    )
    assert response.status == 200
    metadata = json.loads(response.text or "")["reddit"]
    assert metadata["icon_url"] == "/assets/community.png"
    assert metadata["flair"] == "New Model"


@pytest.mark.parametrize('naive_dates', [False, True])
async def test_community_totals_include_archive_provenance_and_both_dates(
    app: web.Application, *, naive_dates: bool,
) -> None:
    state = app['state']
    app['state'] = state._replace(settings=state.settings._replace(sources=(
        SourceConfig('reddit.com', (), feed_urls=(
            'https://www.reddit.com/r/Conspiracy/',
            'https://www.reddit.com/r/empty/',
        )),
    )))
    app['clickhouse'] = FakeClickHouse(
        [{'domain': 'reddit.com', 'count': 238}],
        [{'community': 'conspiracy', 'count': 238}], [],
        [{'community': 'conspiracy', 'total_posts': 1866004,
          'source': 'arctic_shift',
          'updated_at': (
              dt.datetime(2026, 10, 11, 9, 26, 38) if naive_dates
              else '2026-10-11T09:26:38+00:00'
          ),
          'scraped_at': (
              dt.datetime(2026, 10, 17, 15) if naive_dates
              else '2026-10-17T15:00:00+00:00'
          )}], [],
    )
    response = await request(app, '/api/reader/sources')
    assert response.status == 200
    communities = json.loads(response.text or '')['sources'][0]['communities']
    assert communities[0]['count'] == 238
    assert communities[0]['post_total'] == {
        'count': 1866004, 'source': 'arctic_shift',
        'updated_at': '2026-10-11T09:26:38+00:00',
        'captured_at': '2026-10-17T15:00:00+00:00',
    }
    assert communities[1]['post_total'] is None
