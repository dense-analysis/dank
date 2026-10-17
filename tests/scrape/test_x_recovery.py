import asyncio
import datetime as dt
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import SourceConfig, XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import scrape_x_accounts, scrape_x_posts
from dank.scrape.x.session import LoginRequiredError


def post(identifier: str) -> RawPost:
    return RawPost(
        "x.com",
        identifier,
        f"https://x.com/writer/status/{identifier}",
        None,
        dt.datetime.now(dt.UTC),
        "x",
        "",
        "{}",
    )


def batch(identifier: str) -> ScrapeBatch:
    return ScrapeBatch(
        [post(identifier)],
        [
            AssetDiscovery(
                "x",
                "x.com",
                identifier,
                f"https://media.test/{identifier}.jpg",
                "photo",
                None,
            ),
        ],
    )


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch):
    first, second = MagicMock(), MagicMock()
    browser = MagicMock(
        get=AsyncMock(side_effect=[first, second, second, second]),
    )
    session = MagicMock(
        get_browser=AsyncMock(return_value=browser), close=AsyncMock(),
    )
    monkeypatch.setattr("dank.scrape.x.wait_for_x_session", AsyncMock())
    return session


async def test_retry_keeps_saved_records_once_then_visits_next_account(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    attempts: list[str] = []

    async def collect(page: Any, account: str, settings: XSettings):
        attempts.append(account)
        if account == "one":
            yield batch("1")
            if len(attempts) == 1:
                raise TimeoutError()
            yield batch("2")
        else:
            yield batch("3")

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    stats = SourceMetrics(1, SourceConfig("x.com", ("one", "two")))
    token = current_source.set(stats)
    try:
        saved = [
            b
            async for b in scrape_x_accounts(
                XSettings(), stats.source.accounts, session,
            )
        ]
    finally:
        current_source.reset(token)

    assert [p.post_id for b in saved for p in b.posts] == ["1", "2", "3"]
    assert [a.post_id for b in saved for a in b.assets] == ["1", "2", "3"]
    assert attempts == ["one", "one", "two"]
    assert stats.counts["retries"] == 1
    assert stats.counts["fetch_failures"] == 0
    session.close.assert_awaited_once()


@pytest.mark.parametrize("partial", [False, True])
async def test_persistent_account_failure_still_reaches_healthy_account(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    *,
    partial: bool,
):
    caplog.set_level("INFO", logger="dank.scrape.x")
    visited: list[str] = []

    async def collect(page: Any, account: str, settings: XSettings):
        visited.append(account)
        if account == "bad":
            if partial:
                yield batch("1")
            raise TimeoutError()
        yield batch("2")

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    saved = [
        b
        async for b in scrape_x_accounts(XSettings(), ("bad", "good"), session)
    ]
    assert visited == ["bad", "bad", "good"]
    assert [p.post_id for b in saved for p in b.posts] == (
        ["1", "2"] if partial else ["2"]
    )
    assert "account=good outcome=completed" in caplog.text
    assert "account=bad outcome=completed" not in caplog.text
    assert session.close.await_count == 2


async def test_login_failure_stops_without_browser_or_sign_in_retries(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    visited: list[str] = []

    async def collect(page: Any, account: str, settings: XSettings):
        visited.append(account)
        if account == "locked":
            raise LoginRequiredError("sign-in required")
        yield batch("1")

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    saved = [
        b
        async for b in scrape_x_accounts(
            XSettings(), ("one", "locked", "later"), session,
        )
    ]
    assert visited == ["one", "locked"]
    assert len(saved) == 1
    session.close.assert_not_awaited()


async def test_cancellation_is_not_retried_or_swallowed(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    async def collect(*args: Any) -> AsyncIterator[ScrapeBatch]:
        yield batch("1")
        raise asyncio.CancelledError()

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    saved: list[RawPost] = []
    with pytest.raises(asyncio.CancelledError):
        async for b in scrape_x_accounts(XSettings(), ("one", "two"), session):
            saved.extend(b.posts)
    assert [p.post_id for p in saved] == ["1"]
    session.close.assert_not_awaited()


async def test_stalled_cleanup_is_bounded_and_does_not_reopen_owned_profile(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def collect(*args: Any) -> AsyncIterator[ScrapeBatch]:
        raise TimeoutError()
        yield batch("1")

    async def hang():
        await asyncio.Event().wait()

    session.close.side_effect = hang
    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    monkeypatch.setattr("dank.scrape.x.BROWSER_RECOVERY_TIMEOUT_SECONDS", 0.01)
    async with asyncio.timeout(1):
        saved = [
            b
            async for b in scrape_x_accounts(
                XSettings(), ("one", "two"), session,
            )
        ]
    assert saved == []
    assert session.get_browser.await_count == 1
    assert "Could not close the owned X browser safely" in caplog.text


async def test_discussion_timeout_is_saved_before_fresh_browser_for_next_post(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    pages: list[Any] = []

    async def capture(page: Any, seed: RawPost, settings: Any):
        pages.append(page)
        reason = "timeout" if seed.post_id == "1" else None
        return seed._replace(
            payload=json.dumps(
                {
                    "conversation_capture": {
                        "status": "failed" if reason else "captured",
                        "reason": reason,
                    },
                },
            ),
        )

    monkeypatch.setattr("dank.scrape.x.capture_conversation", capture)
    saved = [
        b
        async for b in scrape_x_posts(
            XSettings(), [post("1"), post("2")], session,
        )
    ]
    assert len(saved) == 2
    assert pages[0] is not pages[1]
    assert (
        json.loads(saved[0].posts[0].payload)["conversation_capture"]["reason"]
        == "timeout"
    )
    session.close.assert_awaited_once()
