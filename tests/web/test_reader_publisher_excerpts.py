from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock

import pytest

from dank.storage.clickhouse import QueryResult
from dank.web.reader_links import add_post_links

URL = "https://publisher.test/story?edition=1"
BODY = "<p>Saved publisher research &amp; evidence.</p>"


def post(source: str = "reddit", **overrides: Any) -> dict[str, Any]:
    return {
        "domain": "reddit.com" if source == "reddit" else "x.com",
        "id": "123", "source": source,
        "url": "https://reddit.com/comments/123",
        "html": f'<p><img src="https://image.test/preview.png"></p>'
                f'<p><a href="{URL}">{URL}</a></p>',
        "excerpt": URL[:25] + "...", **overrides,
    }


def publisher(**overrides: Any) -> dict[str, Any]:
    return {
        "lookup_url": URL, "domain": "publisher.test", "id": "article",
        "url": URL, "title": "Original title", "source": "rss",
        "html": BODY, **overrides,
    }


async def test_reddit_bare_link_with_thumbnail_gets_saved_publisher_excerpt():
    item = post()
    original = deepcopy(item)
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([publisher()]),
    ]
    await add_post_links(client, [item], "research")
    assert item["linked_post"]["excerpt"] == (
        "Saved publisher research & evidence."
    )
    assert item["linked_post"]["id"] == "article"
    assert all(item[key] == value for key, value in original.items())
    query, params = client.fetch_json.call_args.args
    assert params == {"publisher_excerpt_urls": [URL]}
    assert "u.observed_at <= target.updated_at" in query
    assert "domainWithoutWWW(target.url)" in query
    assert "source IN ('rss', 'linked_article')" in query
    assert "target.html != ''" in query


async def test_shared_destination_uses_one_batched_lookup_and_observed_alias():
    items = [post(), post(id="456")]
    client = AsyncMock()
    client.fetch_json.side_effect = [QueryResult([]), QueryResult([
        publisher(url="https://publisher.test/redirected"),
    ])]
    await add_post_links(client, items)
    assert client.fetch_json.await_count == 2
    params = client.fetch_json.call_args.args[1]
    assert params["publisher_excerpt_urls"] == [URL]
    assert all(
        item["linked_post"]["url"].endswith("/redirected") for item in items
    )


@pytest.mark.parametrize("html", [
    f"<p>My explanation: {URL}</p>",
    f'<p><a href="{URL}">My descriptive link</a></p>',
    f"<p>{URL} https://other.test/</p>",
    "<p>[removed]</p>",
])
async def test_own_text_and_ambiguous_links_never_borrow_a_cited_article(
    html: str,
):
    item = post(html=html)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    await add_post_links(client, [item])
    assert client.fetch_json.await_count == 1
    assert "linked_post" not in item


@pytest.mark.parametrize("rows", [[], [publisher(html="")], [publisher(
    domain="reddit.com", id="123",
)]])
async def test_missing_empty_or_self_destination_has_no_fallback(
    rows: list[dict[str, Any]],
):
    item = post()
    client = AsyncMock()
    client.fetch_json.side_effect = [QueryResult([]), QueryResult(rows)]
    await add_post_links(client, [item])
    assert "linked_post" not in item


async def test_empty_rss_aggregator_uses_its_article_url():
    item = post("rss", domain="lobste.rs", url=URL, html="", excerpt="")
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([publisher()]),
    ]
    await add_post_links(client, [item])
    assert item["linked_post"]["domain"] == "publisher.test"


@pytest.mark.parametrize("description", ["", "Captured X card description."])
async def test_x_fills_missing_preview_but_keeps_captured_description(
    description: str,
):
    link = {"url": URL, "title": "X card", "description": description,
            "image_url": None}
    item = post("x", x={"links": [link]})
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([publisher()]),
    ]
    await add_post_links(client, [item])
    assert link["description"] == (
        description or "Saved publisher research & evidence."
    )
    assert link["title"] == "X card"
    assert len(item["x"]["links"]) == 1


async def test_x_link_only_post_without_card_gains_a_saved_preview():
    item = post("x", x={"links": []})
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([publisher()]),
    ]
    await add_post_links(client, [item])
    assert item["x"]["links"] == [{
        "url": URL, "title": "Original title", "image_url": None,
        "description": "Saved publisher research & evidence.",
    }]


async def test_x_empty_body_can_use_one_source_owned_card_url():
    item = post("x", html="", x={"links": [{"url": URL}]})
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), QueryResult([publisher()]),
    ]
    await add_post_links(client, [item])
    assert item["x"]["links"][0]["description"].startswith("Saved publisher")


@pytest.mark.parametrize("content", [
    {"links": [{"url": URL}], "quote": {"id": "quote"}},
    {"links": [{"url": URL}, {"url": "https://other.test/story"}]},
])
async def test_x_quote_or_ambiguous_empty_post_keeps_its_own_presentation(
    content: dict[str, Any],
):
    item = post("x", html="", x=content)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    await add_post_links(client, [item])
    assert client.fetch_json.await_count == 1
    assert "linked_post" not in item
