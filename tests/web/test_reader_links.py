from __future__ import annotations

import datetime as dt
import pathlib
from typing import Any, cast

from dank.storage.clickhouse import QueryResult
from dank.web.app import PostRow
from dank.web.reader_api import post_payload
from dank.web.reader_links import (
    RESOLVED_HN_POSTS_SQL,
    add_post_links,
    ensure_link_storage,
)

NOW = dt.datetime(2026, 10, 17, 19, 30, tzinfo=dt.UTC)


class _Client:
    def __init__(self, responses: list[list[dict[str, Any]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.executed: list[str] = []
        self.inserted: list[tuple[str, list[dict[str, Any]]]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        self.calls.append((query, params))

        return QueryResult(self.responses.pop(0))

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        self.inserted.append((table, rows))


def _reference(prefix: str, domain: str, post_id: str) -> dict[str, str]:
    return {
        prefix + "domain": domain, prefix + "id": post_id,
        prefix + "url": f"https://{domain}/{post_id}",
        prefix + "title": "An article", prefix + "source": "rss",
    }


async def test_empty_posts_do_not_access_storage() -> None:
    client = _Client([])
    await add_post_links(cast(Any, client), [])

    assert not client.calls


async def test_backlinks_are_batched_and_counts_preserve_truncated_total(
) -> None:
    rows: list[dict[str, Any]] = [
        {"target_domain": "publisher.test", "target_id": "a",
         "link_count": 63, **_reference("origin_", "news.test", str(index))}
        for index in range(50)
    ]
    rows.append({
        "target_domain": "other.test", "target_id": "b", "link_count": 1,
        **_reference("origin_", "citing.test", "single"),
    })
    client = _Client([rows])
    posts: list[dict[str, Any]] = [
        {"domain": "publisher.test", "id": "a"},
        {"domain": "other.test", "id": "b"},
        {"domain": "empty.test", "id": "c"},
    ]
    await add_post_links(cast(Any, client), posts)

    assert len(client.calls) == 1
    query, params = client.calls[0]
    assert params == {"link_post_keys": [
        ("publisher.test", "a"), ("other.test", "b"), ("empty.test", "c"),
    ]}
    assert "LIMIT 50 BY target_domain, target_id" in query
    assert "count() OVER (PARTITION BY target_domain, target_id)" in query
    assert len(posts[0]["links"]) == 50
    assert posts[0]["links_count"] == 63
    assert posts[0]["links"][0] == {
        "domain": "news.test", "id": "0", "url": "https://news.test/0",
        "title": "An article", "source": "rss",
    }
    assert posts[1]["links_count"] == 1
    assert "links" not in posts[2]


async def test_hn_destinations_use_internal_identity_and_one_batched_query(
) -> None:
    client = _Client([[], [
        {"origin_domain": "news.ycombinator.com", "origin_id": "123",
         **_reference("target_", "publisher.test", "canonical-id")},
        {"origin_domain": "news.ycombinator.com", "origin_id": "unrequested",
         **_reference("target_", "unrelated.test", "ignored")},
    ]])
    posts: list[dict[str, Any]] = [
        {"domain": "news.ycombinator.com", "id": "123"},
        {"domain": "news.ycombinator.com", "id": "456'"},
        {"domain": "publisher.test", "id": "canonical-id"},
    ]
    await add_post_links(cast(Any, client), posts)

    assert len(client.calls) == 2
    query, params = client.calls[1]
    assert params == {"hn_link_keys": [
        ("news.ycombinator.com", "123"), ("news.ycombinator.com", "456'"),
    ]}
    assert "456'" not in query
    assert "LIMIT 1 BY l.domain, l.post_id" in query
    assert "l.kind = 'primary'" in query
    assert posts[0]["linked_post"] == {
        "domain": "publisher.test", "id": "canonical-id",
        "url": "https://publisher.test/canonical-id", "title": "An article",
        "source": "rss",
    }
    assert "linked_post" not in posts[1]
    assert "linked_post" not in posts[2]


async def test_all_reader_link_queries_require_current_content_evidence(
) -> None:
    client = _Client([[], []])
    await add_post_links(cast(Any, client), [
        {"domain": "news.ycombinator.com", "id": "123"},
    ])

    # Database execution is covered separately; guard the query contracts here.
    for query in [RESOLVED_HN_POSTS_SQL, *(call[0] for call in client.calls)]:
        assert "origin.updated_at = l.observed_at" in query
        assert "origin.content_hash = l.content_hash" in query
        assert "hex(SHA256(html)) AS content_hash" in query
        assert "u.observed_at <= target.updated_at" in query


async def test_completed_link_index_does_not_repeat_backfill() -> None:
    client = _Client([[{"version": 1}]])
    await ensure_link_storage(cast(Any, client))

    assert len(client.executed) == 3
    assert len(client.calls) == 1
    assert not client.inserted


async def test_backfill_pages_existing_posts_and_marks_completion_last(
) -> None:
    first = {
        "domain": "one.test", "post_id": "a", "url": "https://one.test/a",
        "html": '<a href="https://two.test/b">Link</a>', "source": "rss",
        "updated_at": NOW.isoformat(),
    }
    second = {
        "domain": "two.test", "post_id": "b", "url": "https://two.test/b",
        "html": "Body", "source": "rss", "updated_at": NOW,
    }
    client = _Client([[], [first], [second], []])
    await ensure_link_storage(cast(Any, client))

    assert [params for _, params in client.calls[1:]] == [
        {"cursor": ("", "")}, {"cursor": ("one.test", "a")},
        {"cursor": ("two.test", "b")},
    ]
    identities = [row for table, rows in client.inserted
                  if table == "post_urls" for row in rows]
    links = [row for table, rows in client.inserted
             if table == "post_links" for row in rows]
    assert len(identities) == 2
    assert len(links) == 1
    assert links[0]["target_url"] == "https://two.test/b"
    assert links[0]["observed_at"] == NOW
    assert len(str(links[0]["content_hash"])) == 64
    assert client.inserted[-1][0] == "link_index_versions"
    assert client.inserted[-1][1][0]["version"] == 1


def test_linked_article_does_not_claim_collection_time_as_publication(
    tmp_path: pathlib.Path,
) -> None:
    captured = dt.datetime(2026, 10, 17, 12, tzinfo=dt.UTC)
    article = PostRow(
        domain="publisher.test", post_id="article",
        url="https://publisher.test/article", author="Reporter", title="Title",
        html="<p>Body</p>", created_at=captured, updated_at=captured,
        source="linked_article",
    )
    assert post_payload(article, [], tmp_path)["published_at"] is None
    published = captured - dt.timedelta(days=1)
    assert post_payload(
        article._replace(created_at=published), [], tmp_path,
    )["published_at"] == published.isoformat()
    assert "published_at" not in post_payload(
        article._replace(source="rss"), [], tmp_path,
    )
