import datetime as dt
import json
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.model import RawPost
from dank.process.hacker_news import (
    convert_raw_hacker_news_post,
    preserve_hacker_news_story,
)
from dank.process.runner import process_source_posts
from dank.storage.clickhouse import QueryResult

NOW = dt.datetime(2026, 10, 17, 15, tzinfo=dt.UTC)


def raw(payload: object) -> RawPost:
    return RawPost(
        "news.ycombinator.com", "123", "https://news.ycombinator.com/item?id=123",
        NOW - dt.timedelta(days=1), NOW, "hacker_news", "https://hckrnews.com/",
        json.dumps(payload),
    )


def test_listing_keeps_frontpage_date_and_escapes_link():
    post = convert_raw_hacker_news_post(raw({"hckrnews": {
        "link_text": "News &amp; ideas", "submitter": "Ada",
        "link": 'https://example.test/path?a=1&b="quote"',
    }}))
    assert post is not None
    assert post.title == "News & ideas"
    assert post.created_at == NOW - dt.timedelta(days=1)
    assert post.updated_at == NOW
    assert post.author == "Ada"
    assert post.source == "hacker_news"
    assert post.url == "https://news.ycombinator.com/item?id=123"
    assert '&amp;b=&quot;quote&quot;' in post.html


def test_detailed_story_uses_story_text_without_comment_search_pollution():
    post = convert_raw_hacker_news_post(raw({
        "hckrnews": {"link_text": "Ask HN: the question"},
        "story": {"title": "Earlier title", "text": "<p>Story body</p>",
                  "by": "author", "time": 1},
        "comments": [{"text": "Unrelated comment text"}],
    }))
    assert post is not None
    assert post.title == "Ask HN: the question"
    assert post.html == "<p>Story body</p>"
    assert post.author == "author"
    assert post.created_at == NOW - dt.timedelta(days=1)


@pytest.mark.parametrize("payload", [None, [], {}, {"hckrnews": []}, {
    "hckrnews": {"link_text": 12}, "story": [],
}])
def test_malformed_post_has_no_processed_story(payload: object):
    assert convert_raw_hacker_news_post(raw(payload)) is None


@pytest.mark.parametrize("url", [
    "javascript:alert(1)", "file:///tmp/story", "https://[", "//example.com",
    "https://user:secret@example.com/",
])
def test_unsafe_external_urls_not_in_processed_body(url: str):
    post = convert_raw_hacker_news_post(raw({
        "hckrnews": {"link_text": "News", "link": url},
    }))
    assert post is not None
    assert post.html == ""


def test_listing_refresh_preserves_detail_body_and_current_title():
    latest = raw({"hckrnews": {"link_text": "Current title"}})
    original = latest.payload
    previous = json.dumps({"story": {"title": "Old", "text": "<p>Body</p>"}})
    projected = preserve_hacker_news_story(latest, previous)
    post = convert_raw_hacker_news_post(projected)
    assert latest.payload == original
    assert post is not None
    assert post.title == "Current title"
    assert post.html == "<p>Body</p>"
    assert post.updated_at == NOW


def test_new_story_capture_does_not_revive_old_body():
    latest = raw({"story": {"title": "New", "text": ""}})
    assert preserve_hacker_news_story(
        latest, json.dumps({"story": {"text": "Old body"}}),
    ) == latest


class Embedder:
    def embed_texts(self, texts: list[str]) -> list[tuple[float, ...]]:
        return [() for _ in texts]


async def test_processing_recovers_body_from_an_older_detailed_scrape():
    latest = raw({"hckrnews": {"link_text": "Current"}})
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([latest._asdict()]),
        QueryResult([{"post_id": "123", "payload": json.dumps({
            "story": {"text": "<p>Archived story</p>", "by": "Ada"},
        })}]),
    ]
    assert await process_source_posts(
        client, latest.domain, convert_raw_hacker_news_post,
        since=NOW - dt.timedelta(hours=1), embedder=cast(Any, Embedder()),
    ) == 1
    query, params = client.fetch_json.call_args_list[1].args
    assert "toUnixTimestamp64Milli(scraped_at) < %(before_0)s" in query
    assert params == {
        "post_0": "123", "before_0": round(NOW.timestamp() * 1000),
    }
    _, rows = client.insert_rows.call_args.args
    assert rows[0]["html"] == "<p>Archived story</p>"
    assert rows[0]["updated_at"] == NOW


def test_self_post_cannot_supply_internal_destination_evidence() -> None:
    post = convert_raw_hacker_news_post(raw({
        "hckrnews": {"link_text": "Ask HN", "link":
                     "https://news.ycombinator.com/item?id=123"},
        "story": {"text": '<a data-dank-hn-destination="1" '
                  'href="https://example.test/">A cited page</a>'},
    }))
    assert post is not None
    assert "data-dank-hn-destination" not in post.html
    assert "A cited page" in post.html
