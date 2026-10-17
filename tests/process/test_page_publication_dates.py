import datetime as dt
import json

import pytest

from dank.process.page import extract_page_metadata


def _jsonld(value: object) -> str:
    return ('<script type="application/ld+json">'
            + json.dumps(value) + '</script>')


@pytest.mark.parametrize("kind", [
    "Article", "NewsArticle", "BlogPosting", "ScholarlyArticle",
    "https://schema.org/NewsArticle", ["Thing", "NewsArticle"],
])
def test_publication_from_typed_jsonld_article(kind: object) -> None:
    page = _jsonld({
        "@context": "https://schema.org", "@type": kind,
        "datePublished": "2026-10-16T16:05:35-00:00",
        "dateModified": "2026-10-17T17:30:00Z",
        "publisher": {"@type": "Organization", "datePublished": "1990-01-01"},
    })

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 16, 16, 5, 35, tzinfo=dt.UTC,
    )


@pytest.mark.parametrize("attribute, key", [
    ("property", "article:published_time"),
    ("property", "og:article:published_time"),
    ("property", "og:published_time"),
    ("name", "published_time"),
    ("name", "datePublished"),
    ("itemprop", "datePublished"),
    ("name", "dcterms.issued"),
    ("name", "DC.date.issued"),
])
def test_explicit_publication_meta_fields(attribute: str, key: str) -> None:
    page = (f'<meta {attribute}="{key}" '
            'content="2026-10-17T15:56:46+00:00">')

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 17, 15, 56, 46, tzinfo=dt.UTC,
    )


def test_valid_publication_meta_is_preferred_to_jsonld() -> None:
    page = ('<meta property="og:article:published_time" '
            'content="2026-10-17T15:56:46+00:00">') + _jsonld({
                "@type": "NewsArticle",
                "datePublished": "2026-10-18T15:00:00Z",
            })

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 17, 15, 56, 46, tzinfo=dt.UTC,
    )


def test_invalid_meta_does_not_hide_valid_jsonld() -> None:
    page = ('<meta property="article:published_time" content="invalid">'
            + _jsonld({
                "@type": "NewsArticle",
                "datePublished": "2026-10-17T17:56:46+02:00",
            }))

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 17, 15, 56, 46, tzinfo=dt.UTC,
    )


def test_graph_ignores_organization_and_related_article_dates() -> None:
    page = _jsonld({"@graph": [
        {"@type": "Organization", "datePublished": "1990-01-01"},
        {"@type": "WebPage", "datePublished": "2026-10-17", "mainEntity": {
            "@type": "Article", "datePublished": "2026-10-16T16:05:35Z",
            "relatedLink": {
                "@type": "Article", "datePublished": "2010-01-01",
            },
        }},
    ]})

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 16, 16, 5, 35, tzinfo=dt.UTC,
    )


def test_multiple_conflicting_article_dates_remain_unknown() -> None:
    page = _jsonld([
        {"@type": "Article", "datePublished": "2026-10-16T12:00:00Z"},
        {"@type": "NewsArticle", "datePublished": "2026-10-17T12:00:00Z"},
    ])

    assert extract_page_metadata(page).published_at is None


def test_duplicate_article_dates_in_different_timezones_agree() -> None:
    page = _jsonld([
        {"@type": "Article", "datePublished": "2026-10-17T15:56:46Z"},
        {"@type": "NewsArticle", "datePublished": "2026-10-17T17:56:46+02:00"},
    ])

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 17, 15, 56, 46, tzinfo=dt.UTC,
    )


@pytest.mark.parametrize("value", [
    {"datePublished": "2026-10-17T12:00:00Z"},
    {"@type": "Organization", "datePublished": "2026-10-17T12:00:00Z"},
    {"@type": "WebPage", "datePublished": "2026-10-17T12:00:00Z"},
    {"@type": "NewsArticle", "dateModified": "2026-10-17T12:00:00Z"},
    {"@type": "NewsArticle", "datePublished": 12345678},
    {"@type": "NewsArticle", "datePublished": "invalid"},
    {"@type": "https://unrelated.test/Article",
     "datePublished": "2026-10-17T12:00:00Z"},
])
def test_non_publication_metadata_is_not_promoted_to_publication(
    value: object,
) -> None:
    assert extract_page_metadata(_jsonld(value)).published_at is None


def test_modified_meta_and_url_dates_remain_unknown() -> None:
    page = ('<meta property="article:modified_time" '
            'content="2026-10-17T15:56:46+00:00">'
            '<meta itemprop="dateModified" content="2026-10-17T16:00:00Z">'
            '<link rel="canonical" href="https://publisher.test/2026/10/16/a">'
            '<article><p>Article without publication metadata.</p></article>')

    assert extract_page_metadata(page).published_at is None


def test_malformed_jsonld_does_not_hide_later_valid_article() -> None:
    page = '<script type="application/ld+json">{broken}</script>' + _jsonld({
        "@type": "Article", "datePublished": "2026-10-16T16:05:35Z",
    })

    assert extract_page_metadata(page).published_at == dt.datetime(
        2026, 10, 16, 16, 5, 35, tzinfo=dt.UTC,
    )
