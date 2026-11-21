from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import (
    BrowserScrapeSettings,
    ConfigError,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.runner import (
    _x_listing_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.x.payloads import account_timeline_post_ids
from dank.scrape.zendriver import NetworkResponse


def tweet(post_id: str, **extra: object) -> dict[str, Any]:
    return {
        "__typename": "Tweet", "rest_id": post_id,
        "core": {"user_results": {"result": {
            "core": {"screen_name": "writer"},
        }}},
        "legacy": {"full_text": f"Post {post_id}"},
        **extra,
    }


def entry(post: dict[str, Any]) -> dict[str, Any]:
    return {"entryId": f"tweet-{post['rest_id']}", "content": {
        "entryType": "TimelineTimelineItem", "itemContent": {
            "itemType": "TimelineTweet", "tweet_results": {"result": post},
        },
    }}


def page(*posts: dict[str, Any], pinned: str = "") -> NetworkResponse:
    instructions: list[dict[str, object]] = [{
        "type": "TimelineAddEntries", "entries": [entry(p) for p in posts],
    }]

    if pinned:
        instructions.insert(0, {
            "type": "TimelinePinEntry", "entry": entry(tweet(pinned)),
        })

    payload = {"data": {"user": {"result": {"timeline": {"timeline": {
        "instructions": instructions,
    }}}}}}

    return NetworkResponse(
        "https://x.com/i/api/graphql/id/UserOriginalsTimeline", 200,
        "application/json", json.dumps(payload), "fixture", "XHR",
    )


@pytest.fixture
def collect_x(monkeypatch: pytest.MonkeyPatch):
    scroll = AsyncMock()
    capture = MagicMock(start=AsyncMock(), stop=AsyncMock(), drain=AsyncMock())
    session = MagicMock(get_browser=AsyncMock(return_value=MagicMock(
        get=AsyncMock(return_value=MagicMock()),
    )))
    monkeypatch.setattr(
        "dank.scrape.x.NetworkCapture", MagicMock(return_value=capture),
    )
    monkeypatch.setattr("dank.scrape.x._scroll", scroll)
    monkeypatch.setattr("dank.scrape.x._navigate_to_account", AsyncMock())
    monkeypatch.setattr("dank.scrape.x._check_login_required", AsyncMock())
    monkeypatch.setattr("dank.scrape.x.wait_for_x_session", AsyncMock())

    return capture, scroll, session


async def test_x_overlap_saves_new_posts_before_stopping(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
) -> None:
    capture, scroll, session = collect_x
    quoted = {"result": tweet("1")}
    minimal = {"__typename": "Tweet", "rest_id": "2"}
    capture.drain.side_effect = [
        # Neither the repeated pin nor quoted and ID-only entries can stop.
        [page(tweet("10", quoted_status_result=quoted), tweet("3"),
              minimal, pinned="3")],
        # Evidence continues past a known prefix to catch a later new post.
        [page(tweet("8"), tweet("7"), tweet("9"))],
        [page(tweet("6"), tweet("6"))],
        [page(tweet("5"))],
    ]
    client = MagicMock(fetch_json=AsyncMock(return_value=SimpleNamespace(rows=[
        {"handle": "writer", "post_id": str(post_id)}
        for post_id in range(1, 9)
    ])))
    started_at = dt.datetime(2026, 11, 21, tzinfo=dt.UTC)
    batches = [batch async for batch in _x_listing_batches(
        XSettings(homepage=False, known_overlap=2), (" @Writer ",),
        session, client, scrape_started_at=started_at,
    )]

    assert [post.post_id for batch in batches for post in batch.posts
            if not post.is_reference] == ["10", "9"]
    assert [post.post_id for batch in batches for post in batch.posts
            if post.is_reference] == ["2"]
    assert scroll.await_count == 3
    capture.stop.assert_awaited_once()
    sql, params = client.fetch_json.call_args.args
    assert "FROM posts" in sql and "FROM raw_posts" in sql
    assert "NOT is_reference" in sql
    assert "scraped_at < %(cutoff)s" in sql
    assert "updated_at < %(cutoff)s" in sql
    assert params["accounts"] == ["writer"]
    assert params["cutoff"] == started_at


async def test_all_known_timeline_completes_without_scrolling(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    capture, scroll, session = collect_x
    capture.drain.side_effect = [[page(tweet("12"), tweet("11"))]]
    client = MagicMock(fetch_json=AsyncMock(return_value=SimpleNamespace(rows=[
        {"handle": "writer", "post_id": post_id} for post_id in ("12", "11")
    ])))
    source = SourceConfig("x.com", ("writer",))
    stats = SourceMetrics(1, source)
    caplog.set_level("INFO", logger="dank.scrape.x")
    token = current_source.set(stats)

    try:
        batches = [batch async for batch in _x_listing_batches(
            XSettings(homepage=False, known_overlap=2), source.accounts,
            session, client,
        )]
    finally:
        current_source.reset(token)

    assert batches == []
    assert stats.counts["fetch_failures"] == 0
    assert "outcome=completed" in caplog.text
    assert "reason=known account overlap" in caplog.text
    scroll.assert_not_awaited()


def test_only_authored_timeline_entries_qualify_for_overlap() -> None:
    payload = json.loads(page(
        tweet("11", quoted_status_result={"result": tweet("10")}),
        tweet("9"), {"__typename": "Tweet", "rest_id": "8"}, pinned="9",
    ).body)
    user = payload["data"]["user"]["result"]
    user["timeline_v2"] = user.pop("timeline")
    pinned: set[str] = set()
    assert list(account_timeline_post_ids(
        payload, "writer", pinned=pinned,
    )) == ["11"]
    assert list(account_timeline_post_ids(
        json.loads(page(tweet("9"), tweet("7")).body),
        "writer", pinned=pinned,
    )) == ["7"]


async def test_x_overlap_zero_collects_without_reading_history(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
) -> None:
    capture, scroll, session = collect_x
    capture.drain.side_effect = [[page(tweet("11"))], [page(tweet("10"))], []]
    client = MagicMock(fetch_json=AsyncMock())
    settings = XSettings(
        scrape=BrowserScrapeSettings(max_scrolls=1), homepage=False,
        known_overlap=0,
    )
    batches = [batch async for batch in _x_listing_batches(
        settings, ("writer",), session, client,
    )]

    assert [post.post_id for batch in batches for post in batch.posts] == [
        "11", "10",
    ]
    client.fetch_json.assert_not_awaited()
    scroll.assert_awaited_once()


@pytest.mark.parametrize("value", ["-1", "true", "2.5"])
def test_x_overlap_validation(tmp_path: Path, value: str) -> None:
    config = tmp_path / "settings.toml"
    config.write_text(f"[x]\nknown_overlap = {value}\n")

    with pytest.raises(ConfigError, match=r"x\.known_overlap"):
        load_settings(config)
