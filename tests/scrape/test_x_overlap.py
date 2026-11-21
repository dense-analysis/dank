from __future__ import annotations

import asyncio
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
    SourceAdapter,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.scrape.account_schedule import AccountCheck
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.runner import (
    _AccountSchedule,  # pyright: ignore[reportPrivateUsage]
    _load_account_checks,  # pyright: ignore[reportPrivateUsage]
    _plan_account_checks,  # pyright: ignore[reportPrivateUsage]
    _x_listing_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import (
    TimelineCoverage,
    _scrape_account,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.x.payloads import account_timeline_post_ids
from dank.scrape.zendriver import NetworkResponse

START = dt.datetime(2026, 11, 21, tzinfo=dt.UTC)


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
    capture = MagicMock(
        start=AsyncMock(), stop=AsyncMock(), drain=AsyncMock(),
        has_pending=MagicMock(return_value=False), has_ready=False,
    )
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


@pytest.fixture
async def account_schedule(tmp_path: Path) -> _AccountSchedule:
    config = tmp_path / "settings.toml"
    config.write_text("")
    settings = load_settings(config)._replace(
        data_dir=tmp_path, sources=(SourceConfig("x.com", ("writer",)),),
    )
    plans = await _plan_account_checks(settings, MagicMock(), START)

    return plans[SourceAdapter.X]


async def test_x_overlap_saves_new_posts_before_stopping(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
    tmp_path: Path,
    account_schedule: _AccountSchedule,
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
    state = tmp_path / "x-account-checks.json"
    batches: list[ScrapeBatch] = []

    async for batch in _x_listing_batches(
        XSettings(homepage=False, known_overlap=2), (" @Writer ",),
        session, client, scrape_started_at=START,
        account_plan=account_schedule._replace(coverage={
            "writer": TimelineCoverage(tuple(map(str, range(4, 9)))),
        }),
    ):
        assert not state.exists()
        batches.append(batch)

    assert _load_account_checks(state) == {
        "writer": AccountCheck(START, START, timeline_ids=(
            "10", "8", "7", "9", "6", "5",
        )),
    }
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
    assert params["cutoff"] == START


async def test_all_known_timeline_completes_without_scrolling(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    account_schedule: _AccountSchedule,
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
    state = tmp_path / "x-account-checks.json"

    try:
        batches = [batch async for batch in _x_listing_batches(
            XSettings(homepage=False, known_overlap=2), source.accounts,
            session, client, scrape_started_at=START,
            account_plan=account_schedule._replace(coverage={
                "writer": TimelineCoverage(("12", "11")),
            }),
        )]
    finally:
        current_source.reset(token)

    assert batches == []
    assert stats.counts["fetch_failures"] == 0
    assert "outcome=completed" in caplog.text
    assert "reason=known account overlap" in caplog.text
    scroll.assert_not_awaited()
    assert _load_account_checks(state) == {
        "writer": AccountCheck(START, START, timeline_ids=("12", "11")),
    }


@pytest.mark.parametrize("rate_limited", [False, True])
async def test_partial_timeline_does_not_record_success(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock], tmp_path: Path,
    account_schedule: _AccountSchedule, *, rate_limited: bool,
) -> None:
    capture, scroll, session = collect_x
    initial = [page(tweet("12"))]

    if rate_limited:
        capture.drain.side_effect = [
            initial, [initial[0]._replace(status=429)], [],
        ]
    else:
        capture.drain.side_effect = [initial]
        scroll.side_effect = TimeoutError("fixture timeout")

    client = MagicMock(fetch_json=AsyncMock(
        return_value=SimpleNamespace(rows=[]),
    ))
    state = tmp_path / "x-account-checks.json"
    batches = [batch async for batch in _x_listing_batches(
        XSettings(scrape=BrowserScrapeSettings(max_scrolls=1), homepage=False),
        ("writer",), session, client, account_plan=account_schedule,
    )]

    posts = [post.post_id for batch in batches for post in batch.posts]
    assert posts == ["12"]
    assert not state.exists()


async def test_rejects_previous_accounts_page_and_drains_last_scroll(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
) -> None:
    capture, scroll, _ = collect_x
    initial_started = asyncio.Event()
    initial_ready = asyncio.Event()
    captured: list[str] = []
    other = page(tweet("99"))
    other = other._replace(body=other.body.replace('"writer"', '"previous"'))

    async def wait_for_first() -> list[NetworkResponse]:
        initial_started.set()
        await initial_ready.wait()

        return [page(tweet("12"))]

    replies: list[list[NetworkResponse] | None] = [
        [other], None, [], [page(tweet("11"))],
    ]
    drains = iter(replies)

    async def drain(**kwargs: object) -> list[NetworkResponse]:
        drained = next(drains)

        return await wait_for_first() if drained is None else drained

    async def settle(*args: object, **kwargs: object) -> bool:
        capture.has_pending.return_value = False
        capture.has_ready = True

        return True

    async def advance(*args: object) -> None:
        capture.has_pending.return_value = True

    async def collect() -> None:
        async for batch in _scrape_account(
            MagicMock(), "writer",
            XSettings(BrowserScrapeSettings(max_scrolls=1)),
        ):
            captured.extend(post.post_id for post in batch.posts)
            capture.has_ready = False

    capture.drain.side_effect = drain
    capture.wait_for_pending = AsyncMock(side_effect=settle)
    scroll.side_effect = advance

    async with asyncio.timeout(1), asyncio.TaskGroup() as group:
        group.create_task(collect())
        await initial_started.wait()
        scroll.assert_not_awaited()
        assert captured == []
        initial_ready.set()

    assert captured == ["12", "11"]
    scroll.assert_awaited_once()
    capture.wait_for_pending.assert_awaited_once()


async def test_overlap_ignores_posts_saved_by_an_incomplete_check(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
    account_schedule: _AccountSchedule, tmp_path: Path,
) -> None:
    capture, scroll, session = collect_x
    capture.drain.side_effect = [
        [page(tweet("14"), tweet("13"))],
        [page(tweet("12"), tweet("11"), tweet("10"))],
    ]
    client = MagicMock(fetch_json=AsyncMock(return_value=SimpleNamespace(
        rows=[{"handle": "writer", "post_id": post_id}
              for post_id in ("14", "13", "11", "10")],
    )))
    plan = account_schedule._replace(coverage={
        "writer": TimelineCoverage(("11", "10")),
    })
    batches = [batch async for batch in _x_listing_batches(
        XSettings(homepage=False, known_overlap=2), ("writer",),
        session, client, account_plan=plan,
    )]

    ids = [post.post_id for batch in batches for post in batch.posts]
    assert ids == ["12"]
    assert _load_account_checks(tmp_path / "x-account-checks.json")[
        "writer"
    ].timeline_ids == ("14", "13", "12", "11", "10")
    scroll.assert_awaited_once()


@pytest.mark.parametrize("has_cursor", [False, True])
async def test_bootstrap_window_requires_the_first_page(
    collect_x: tuple[MagicMock, AsyncMock, MagicMock],
    tmp_path: Path, account_schedule: _AccountSchedule, *, has_cursor: bool,
) -> None:
    capture, _, session = collect_x
    post = tweet("10", legacy={
        "full_text": "Older post",
        "created_at": "Thu Nov 19 12:00:00 +0000 2026",
    })
    received = page(post)

    if has_cursor:
        received = received._replace(
            url=received.url + '?variables={"cursor":"next"}',
        )

    capture.drain.return_value = [received]
    client = MagicMock(fetch_json=AsyncMock(
        return_value=SimpleNamespace(rows=[]),
    ))
    settings = XSettings(
        scrape=BrowserScrapeSettings(max_scrolls=0), homepage=False,
    )
    batches = [batch async for batch in _x_listing_batches(
        settings, ("writer",), session, client, account_plan=account_schedule,
    )]
    ids = [post.post_id for batch in batches for post in batch.posts]
    assert ids == ["10"]
    state = tmp_path / "x-account-checks.json"

    if has_cursor:
        assert not state.exists()
    else:
        assert _load_account_checks(state)["writer"].timeline_ids == ("10",)


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
