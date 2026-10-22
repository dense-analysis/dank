from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest
from multidict import MultiDict

from dank.config import SourceConfig, load_settings
from dank.storage.clickhouse import QueryResult
from dank.web.reader_blogs import EXTERNAL_BLOG_CONDITION, external_blog_count
from dank.web.reader_links import RESOLVED_HN_POSTS_SQL
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)
from dank.web.reader_refresh_scope import resolve_scope


@pytest.mark.parametrize("query", [
    {"blog_feed": "external"}, {"kind": "all", "blog_feed": "sources"},
    {"kind": "stories", "blog_feed": ""},
    {"kind": "stories", "blog_feed": "unconfigured"},
    {"kind": "hacker_news", "blog_feed": "sources"},
])
def test_blog_scope_requires_valid_blogs_context(query: dict[str, str]):
    with pytest.raises(ValueError, match="blog_feed"):
        parse_filters(MultiDict(query))


def test_repeated_blog_scope_is_rejected():
    with pytest.raises(ValueError, match="only once"):
        parse_filters(MultiDict([
            ("kind", "stories"), ("blog_feed", "external"),
            ("blog_feed", "sources"),
        ]))


def test_blog_source_scope_keeps_feed_entries_and_hacker_news():
    filters = parse_filters(MultiDict({
        "kind": "stories", "blog_feed": "sources", "q": "interesting",
    }))
    query, params = build_query(filters, (
        SourceConfig("blog.test", ()),
        SourceConfig("news.ycombinator.com", ()),
        SourceConfig("reddit.com", ()), SourceConfig("youtube.com", ()),
        SourceConfig("x.com", ()),
    ))
    assert params["blog_domains"] == ["blog.test", "news.ycombinator.com"]
    assert params["term_0"] == "interesting"
    assert "domain IN %(blog_domains)s" in query
    assert RESOLVED_HN_POSTS_SQL not in query
    assert "hn_filter" not in params
    empty, _ = build_query(filters, ())
    assert "WHERE 0" in empty


def test_selected_feed_preserves_aggregator_records_with_saved_destinations():
    query, params = build_query(ReaderFilters(
        kind="stories", domains=("lobste.rs",),
    ), (SourceConfig("lobste.rs", ()),))
    assert params["domains"] == ["lobste.rs"]
    assert RESOLVED_HN_POSTS_SQL not in query


def test_external_articles_require_saved_provenance_and_publisher_identity():
    filters = parse_filters(MultiDict({
        "kind": "stories", "blog_feed": "external", "q": "specific",
        "after": "2026-10-01", "domain": "configured.test",
    }))
    query, params = build_query(
        filters, (SourceConfig("configured.test", ()),),
    )
    assert EXTERNAL_BLOG_CONDITION in query
    assert "source IN ('rss', 'linked_article')" in query
    assert "= domainWithoutWWW(url)" in query
    assert "post_reference_evidence" in query
    assert "evidence.observed_at <= origin.updated_at" in query
    assert "alias.observed_at <= target.updated_at" in query
    assert "capture.scraped_at <= target.updated_at" in query
    assert "collection_origin" in query
    assert "blog_domains" not in params
    assert params["domains"] == ["configured.test"]
    assert params["term_0"] == "specific"
    assert "created_at >=" in query


def test_blog_scope_is_part_of_cursor_identity():
    filters = ReaderFilters(kind="stories", blog_feed="external")
    token = encode_cursor(filters, dt.datetime(2026, 10, 22, tzinfo=dt.UTC),
                          "blog.test", "1")

    for scope in ("", "sources"):
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor(filters._replace(blog_feed=scope, cursor=token))


async def test_external_count_matches_the_external_article_query():
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{"blog_external_count": 17}])
    assert await external_blog_count(client) == 17
    query = client.fetch_json.call_args.args[0]
    assert query.endswith(EXTERNAL_BLOG_CONDITION)


@pytest.mark.parametrize("scope", [
    {"kind": "rss"}, {"kind": "rss", "blog_feed": "external"},
])
async def test_blogs_refreshes_discovery_sources_without_adding_publishers(
    scope: dict[str, str],
):
    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("blog.test", ()),
        SourceConfig("news.ycombinator.com", ()),
        SourceConfig("x.com", ("alice",)), SourceConfig("reddit.com", ()),
        SourceConfig("youtube.com", ()),
    ))
    client = AsyncMock()
    plan = await resolve_scope(settings, client, scope)
    assert plan.settings.sources == settings.sources[:4]
    client.fetch_json.assert_not_called()


async def test_selected_blog_refresh_does_not_collect_other_discovery_feeds():
    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("blog.test", ()), SourceConfig("x.com", ("alice",)),
    ))
    plan = await resolve_scope(settings, AsyncMock(), {
        "kind": "rss", "domains": ["blog.test"],
    })
    assert plan.settings.sources == settings.sources[:1]
    assert plan.label == "blog.test"


@pytest.mark.parametrize("scope", [
    {"kind": "rss", "blog_feed": "unknown"},
    {"kind": "rss", "blog_feed": ""},
    {"kind": "all", "blog_feed": "external"},
])
async def test_invalid_blog_refresh_context_fails_before_storage(
    scope: object,
):
    client = AsyncMock()

    with pytest.raises(ValueError):
        await resolve_scope(
            load_settings("config.example.toml"), client, scope,
        )

    client.fetch_json.assert_not_called()
