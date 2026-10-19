from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast

import pytest

from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.app import PostRow
from dank.web.reader_provenance import add_collection_info

T0 = dt.datetime(2026, 10, 17, 10, tzinfo=dt.UTC)
T1 = T0 + dt.timedelta(hours=1)
T2 = T0 + dt.timedelta(hours=2)
URL = "https://publisher.test/story"
REDDIT = "https://www.reddit.com/r/science/comments/123/story/"


class Client:
    def __init__(
        self, *, articles: list[dict[str, Any]] | None = None,
        aliases: list[dict[str, Any]] | None = None,
        captures: list[dict[str, Any]] | None = None,
        observations: list[dict[str, Any]] | None = None,
    ) -> None:
        self.results = {
            "provenance_urls": articles or [],
            "provenance_article_keys": aliases or [],
            "provenance_capture_keys": captures or [],
            "provenance_observation_urls": observations or [],
        }
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any],
    ) -> QueryResult:
        self.calls.append((query, params))
        return QueryResult(self.results[next(iter(params))])


def article(**kwargs: Any) -> dict[str, Any]:
    return {
        "lookup_url": URL, "domain": "publisher.test", "id": "article",
        "url": URL, "title": "Original", "source": "rss", "available": 1,
        "updated_at": T2,
        **kwargs,
    }


def capture(
    domain: str = "publisher.test", post_id: str = "article", **kwargs: Any,
) -> dict[str, Any]:
    return {
        "domain": domain, "post_id": post_id, "first_at": T1,
        "first_url": URL, "first_source": "rss", "first_origin": "",
        "successful_captures": 1, "collected_at": T1,
        "collected_url": URL, "collected_source": "rss",
        "collected_origin": "", **kwargs,
    }


def observation(**kwargs: Any) -> dict[str, Any]:
    return {
        "target_url": URL, "domain": "news.ycombinator.com", "post_id": "9",
        "url": "https://news.ycombinator.com/item?id=9",
        "source": "hacker_news", "first_observed_at": T0, **kwargs,
    }


async def info(client: Client, *, updated: dt.datetime = T2):
    payload: dict[str, Any] = {
        "domain": "reddit.com", "id": "123", "url": REDDIT,
        "source": "reddit", "reddit": {"external_url": URL},
    }
    row = PostRow(
        "reddit.com", "123", REDDIT, "writer", "Title", "Body",
        T0 - dt.timedelta(days=30), updated, "reddit",
    )
    await add_collection_info(cast(ClickHouseClient, client), [payload], [row])
    return payload.get("collection_info")


def own_capture(**kwargs: Any):
    return capture(
        "reddit.com", "123", first_at=T2, first_source="reddit",
        first_url=REDDIT, successful_captures=0, **kwargs,
    )


async def test_first_observation_and_actual_collection_route_are_distinct():
    client = Client(
        articles=[article()], captures=[own_capture(), capture()],
        observations=[observation(), observation(
            domain="reddit.com", post_id="123", source="reddit", url=REDDIT,
            first_observed_at=T2,
        )],
    )
    result = await info(client)
    assert result is not None
    assert result["first_seen_at"] == T0.isoformat()
    assert result["first_seen_from"][0]["label"] == "Hacker News"
    assert result["collected_via"]["label"] == "publisher.test"
    assert result["article_collected_at"] == T1.isoformat()
    assert result["post_first_seen_at"] == T2.isoformat()
    assert result["seen_before_post"] is True
    assert result["article_status"] == "collected"
    assert result["coverage"] == "saved_history"
    assert len(client.calls) == 4
    assert "AND kind='post'" in client.calls[-1][0]


async def test_reddit_origin_survives_reuse_and_preserves_observation_time():
    origin = json.dumps({
        "domain": "reddit.com", "post_id": "123", "source": "reddit",
        "url": REDDIT, "observed_at": T2.isoformat(), "community": "science",
    })
    fetched = T2 + dt.timedelta(seconds=10)
    client = Client(articles=[article(
        source="linked_article", updated_at=fetched,
    )], captures=[
        own_capture(), capture(
            first_at=fetched, first_source="linked_article",
            first_origin=origin,
            collected_at=fetched, collected_source="linked_article",
            collected_origin=origin,
        ),
    ])
    result = await info(client)
    assert result is not None
    assert result["first_seen_at"] == T2.isoformat()
    assert result["first_seen_from"][0]["label"] == "r/science"
    assert result["collected_via"]["source"] == "reddit"
    assert result["article_collected_at"] == fetched.isoformat()
    assert result["seen_before_post"] is False


async def test_redirect_aliases_share_earlier_observations():
    old_url = "https://old.test/story?edition=1"
    client = Client(
        articles=[article()], aliases=[{
            "domain": "publisher.test", "post_id": "article", "url": old_url,
        }], captures=[own_capture(), capture()],
        observations=[observation(target_url=old_url)],
    )
    result = await info(client)
    assert result is not None
    assert result["first_seen_at"] == T0.isoformat()
    assert client.calls[-1][1]["provenance_observation_urls"] == sorted([
        URL, old_url,
    ])


async def test_missing_article_keeps_observation_without_fake_capture():
    client = Client(captures=[own_capture()], observations=[observation(
        domain="reddit.com", post_id="123", url=REDDIT, source="reddit",
        first_observed_at=T2,
    )])
    result = await info(client)
    assert result is not None
    assert result["article"] is None
    assert result["article_status"] == "unavailable"
    assert result["article_collected_at"] is None
    assert result["collected_via"] is None
    assert result["first_seen_from"][0]["label"] == "r/science"
    assert result["seen_before_post"] is False


async def test_tied_first_sources_do_not_claim_a_single_winner():
    client = Client(captures=[own_capture()], observations=[
        observation(first_observed_at=T2),
        observation(domain="reddit.com", post_id="123", url=REDDIT,
                    source="reddit", first_observed_at=T2),
    ])
    result = await info(client)
    assert result is not None
    assert len(result["first_seen_from"]) == 2
    assert result["seen_before_post"] is False


@pytest.mark.parametrize("origin", ["", "not JSON", json.dumps({
    "domain": "reddit.com", "post_id": "123", "source": "reddit",
    "observed_at": (T2 + dt.timedelta(days=1)).isoformat(),
})])
async def test_legacy_or_invalid_route_is_unknown_not_inferred(origin: str):
    client = Client(articles=[article(source="linked_article")], captures=[
        own_capture(), capture(
            first_source="linked_article", first_origin=origin,
            collected_source="linked_article", collected_origin=origin,
        ),
    ])
    result = await info(client)
    assert result is not None
    assert result["collected_via"] is None
    assert result["first_seen_at"] == T1.isoformat()
    assert "unknown" in result["first_seen_from"][0]["label"]


async def test_retained_feed_text_without_page_capture_is_incomplete():
    client = Client(articles=[article()], captures=[
        own_capture(), capture(successful_captures=0),
    ])
    result = await info(client)
    assert result is not None
    assert result["article_status"] == "incomplete"
    assert result["article_collected_at"] is None


async def test_missing_post_history_does_not_guess_from_publication_date():
    client = Client(articles=[article()], captures=[capture()])
    result = await info(client)
    assert result is not None
    assert result["post_first_seen_at"] is None
    assert result["seen_before_post"] is None


async def test_newer_raw_post_does_not_rewrite_the_displayed_history():
    client = Client(articles=[article()], captures=[own_capture(), capture()])
    result = await info(client, updated=T1)
    assert result is not None
    assert result["post_first_seen_at"] is None
    assert result["seen_before_post"] is None


async def test_no_history_produces_no_invented_collection_info():
    assert await info(Client()) is None


async def test_unprocessed_success_cannot_upgrade_an_older_article_version():
    client = Client(articles=[article(updated_at=T1)], captures=[
        own_capture(), capture(first_at=T0, collected_at=T2),
    ])
    result = await info(client)
    assert result is not None
    assert result["article_status"] == "incomplete"
    assert result["article_collected_at"] is None
    assert result["collected_via"] is None
    query, params = client.calls[-2]
    assert "scraped_at <= parseDateTime64BestEffort" in query
    assert T1.isoformat() in params.values()


async def test_edited_post_compares_the_current_link_observation():
    own = own_capture()
    own["first_at"] = T0
    client = Client(articles=[article()], captures=[own, capture()],
                    observations=[observation(
                        domain="reddit.com", post_id="123", url=REDDIT,
                        source="reddit", first_observed_at=T2,
                    )])
    result = await info(client)
    assert result is not None
    assert result["post_first_seen_at"] == T0.isoformat()
    assert result["link_first_seen_at"] == T2.isoformat()
    assert result["first_seen_at"] == T1.isoformat()
    assert result["seen_before_post"] is True


async def test_independent_discovery_before_publication_qualifies_for_eye():
    client = Client(
        articles=[article()],
        captures=[own_capture(published_at=T1), capture()],
        observations=[observation(), observation(
            domain="reddit.com", post_id="123", source="reddit", url=REDDIT,
            first_observed_at=T2,
        )],
    )
    result = await info(client)
    assert result is not None
    assert result["post_published_at"] == T1.isoformat()
    assert result["discovered_before_publication"] is True
    assert result["earlier_discovered_at"] == T0.isoformat()
    assert result["earlier_discovered_from"][0]["label"] == "Hacker News"
    query = client.calls[-2][0]
    assert "argMaxIf(post_created_at,scraped_at" in query


async def test_collection_order_alone_does_not_qualify_for_eye():
    client = Client(
        articles=[article()],
        captures=[own_capture(published_at=T0), capture()],
        observations=[observation(first_observed_at=T1), observation(
            domain="reddit.com", post_id="123", source="reddit", url=REDDIT,
            first_observed_at=T2,
        )],
    )
    result = await info(client)
    assert result is not None
    assert result["seen_before_post"] is True
    assert result["discovered_before_publication"] is False
    assert result["earlier_discovered_at"] is None
    assert result["earlier_discovered_from"] == []


@pytest.mark.parametrize("published", [
    None, "invalid", T2 + dt.timedelta(days=1),
])
async def test_unknown_or_inconsistent_publication_never_uses_capture_time(
    published: object,
):
    client = Client(captures=[own_capture(published_at=published)],
                    observations=[observation()])
    result = await info(client)
    assert result is not None
    assert result["post_published_at"] is None
    assert result["discovered_before_publication"] is None
    assert result["earlier_discovered_from"] == []


async def test_current_post_and_its_article_capture_are_not_other_sources():
    origin = json.dumps({
        "domain": "reddit.com", "post_id": "123", "url": REDDIT,
        "source": "reddit", "observed_at": T0.isoformat(),
    })
    client = Client(articles=[article(source="linked_article")], captures=[
        own_capture(published_at=T1), capture(
            first_source="linked_article", first_origin=origin,
            collected_source="linked_article", collected_origin=origin,
        ),
    ], observations=[observation(
        domain="reddit.com", post_id="123", url=REDDIT, source="reddit",
        first_observed_at=T0,
    )])
    result = await info(client)
    assert result is not None
    assert result["discovered_before_publication"] is False


async def test_unattributed_legacy_capture_does_not_invent_an_earlier_source():
    client = Client(articles=[article(source="linked_article")], captures=[
        own_capture(published_at=T2), capture(
            first_source="linked_article", collected_source="linked_article",
        ),
    ])
    result = await info(client)
    assert result is not None
    assert result["discovered_before_publication"] is False


async def test_same_community_other_post_can_be_an_earlier_source():
    client = Client(captures=[own_capture(published_at=T1)],
                    observations=[observation(
                        domain="reddit.com", post_id="earlier",
                        source="reddit",
                        url="https://reddit.com/r/science/comments/earlier/",
                    )])
    result = await info(client)
    assert result is not None
    assert result["discovered_before_publication"] is True
    assert result["earlier_discovered_from"][0]["id"] == "earlier"


async def test_discovery_equal_to_publication_is_not_earlier():
    client = Client(captures=[own_capture(published_at=T0)],
                    observations=[observation()])
    result = await info(client)
    assert result is not None
    assert result["discovered_before_publication"] is False


@pytest.mark.parametrize("observations, expected", [
    ([], False), ([observation()], True),
])
async def test_media_needs_exact_independent_evidence(
    observations: list[dict[str, Any]], *, expected: bool,
):
    video_url = "https://v.redd.it/exactvideo"
    client = Client(captures=[own_capture(published_at=T1)],
                    observations=[{**row, "target_url": video_url}
                                  for row in observations])
    payload: dict[str, Any] = {
        "domain": "reddit.com", "id": "123", "source": "reddit",
        "url": REDDIT,
        "reddit": {"external_url": video_url},
    }
    row = PostRow("reddit.com", "123", REDDIT, "author", "Video", "",
                  T1, T2, "reddit")
    await add_collection_info(cast(ClickHouseClient, client), [payload], [row])
    result = payload["collection_info"]
    assert result["discovered_before_publication"] is expected


async def test_hn_compares_submission_time_not_front_page_arrival():
    payload: dict[str, Any] = {
        "domain": "news.ycombinator.com", "id": "123",
        "source": "hacker_news",
        "url": "https://news.ycombinator.com/item?id=123",
        "hacker_news": {"external_url": URL, "submitted_at": T0.isoformat()},
    }
    client = Client(captures=[capture(
        "news.ycombinator.com", "123", first_at=T2, published_at=T2,
    )], observations=[observation(first_observed_at=T1)])
    row = PostRow("news.ycombinator.com", "123", payload["url"], "author",
                  "Story", "", T2, T2, "hacker_news")
    await add_collection_info(cast(ClickHouseClient, client), [payload], [row])
    result = payload["collection_info"]
    assert result["post_published_at"] == T0.isoformat()
    assert result["discovered_before_publication"] is False
