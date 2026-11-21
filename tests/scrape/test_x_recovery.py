import asyncio
import datetime as dt
import json
import pathlib
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import (
    EmailSettings,
    Settings,
    SourceConfig,
    XOnePasswordSettings,
    XSettings,
)
from dank.model import AssetDiscovery, RawPost
from dank.scrape.__main__ import main as scrape_main
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import scrape_x_accounts, scrape_x_posts
from dank.scrape.x.onepassword import LoginCredentials
from dank.scrape.x.session import (
    AppVerificationRequiredError,
    LoginRequiredError,
    SignInPromptError,
)


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

    async def collect(page: Any, account: str, settings: XSettings, **kw: Any):
        attempts.append(account)
        if account == "one":
            yield batch("1")
            if len(attempts) == 1:
                raise ConnectionError()
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
@pytest.mark.parametrize("failure", [TimeoutError, ValueError])
async def test_persistent_account_failure_still_reaches_healthy_account(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    *,
    partial: bool,
    failure: type[Exception],
):
    caplog.set_level("INFO", logger="dank.scrape.x")
    visited: list[str] = []
    pages: list[Any] = []

    async def collect(page: Any, account: str, settings: XSettings, **kw: Any):
        visited.append(account)
        pages.append(page)
        if account == "bad":
            if partial:
                yield batch("1")
            raise failure("account failed")
        yield batch("2")

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    saved = [
        b
        async for b in scrape_x_accounts(XSettings(), ("bad", "good"), session)
    ]
    assert visited == ["bad", "good"]
    assert [p.post_id for b in saved for p in b.posts] == (
        ["1", "2"] if partial else ["2"]
    )
    assert "account=good outcome=completed" in caplog.text
    assert "account=bad outcome=completed" not in caplog.text
    session.close.assert_not_awaited()
    assert pages[0] is pages[1]
    assert session.get_browser.return_value.get.await_count == 1


async def test_login_failure_stops_without_browser_or_sign_in_retries(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    visited: list[str] = []

    async def collect(page: Any, account: str, settings: XSettings, **kw: Any):
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


async def test_discussion_logs_app_verification_instructions(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        "dank.scrape.x.wait_for_x_session",
        AsyncMock(side_effect=AppVerificationRequiredError()),
    )
    batches = [
        item async for item in scrape_x_posts(
            XSettings(), [post("1"), post("2")], session,
        )
    ]

    assert len(batches) == 1
    assert "Open the X app on your phone" in caplog.text


async def test_email_prompt_without_imap_logs_why_collection_stopped(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        "dank.scrape.x.wait_for_x_session",
        AsyncMock(side_effect=SignInPromptError(stage="email_code")),
    )

    batches = [
        item async for item in scrape_x_accounts(
            XSettings(), ("writer",), session,
        )
    ]

    assert batches == []
    assert "X requires email verification. IMAP credentials are missing" in (
        caplog.text
    )


@pytest.mark.parametrize("direct", [True, False])
@pytest.mark.parametrize("headless", [False, True])
async def test_login_retries_same_account_with_saved_batches(
    session: Any, monkeypatch: pytest.MonkeyPatch,
    *, direct: bool, headless: bool,
) -> None:
    visited: list[str] = []
    pages: list[object] = []
    credentials = LoginCredentials("fixture-user", "fixture-password")
    references = XOnePasswordSettings("op://test/X/user", "op://test/X/pass")
    load = AsyncMock(return_value=credentials)
    login = AsyncMock()
    session.headless = headless

    async def collect(page: Any, account: str, settings: Any, **kw: Any):
        visited.append(account)
        pages.append(page)
        yield batch("3" if account == "locked" else "1")

        if len(visited) == 1 or account == "locked":
            raise SignInPromptError()

        yield batch("2")

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    monkeypatch.setattr("dank.scrape.x.load_credentials", load)
    monkeypatch.setattr("dank.scrape.x.sign_in_automatically", login)
    settings = XSettings(
        onepassword=references, credentials=credentials if direct else None,
    )
    saved = [
        item async for item in scrape_x_accounts(
            settings, ("one", "locked", "later"), session,
        )
    ]

    assert [p.post_id for item in saved for p in item.posts] == ["1", "2", "3"]
    assert visited == ["one", "one", "locked"]
    if direct:
        load.assert_not_awaited()
    else:
        load.assert_awaited_once_with(references)

    login.assert_awaited_once_with(pages[0], credentials, email="", imap=None)
    assert all(page is pages[0] for page in pages)
    assert session.get_browser.return_value.get.await_count == 1
    session.close.assert_not_awaited()


async def test_cancellation_is_not_retried_or_swallowed(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    async def collect(*args: Any, **kw: Any) -> AsyncIterator[ScrapeBatch]:
        yield batch("1")
        raise asyncio.CancelledError()

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    saved: list[RawPost] = []
    with pytest.raises(asyncio.CancelledError):
        async for b in scrape_x_accounts(XSettings(), ("one", "two"), session):
            saved.extend(b.posts)
    assert [p.post_id for p in saved] == ["1"]
    session.close.assert_not_awaited()


def test_default_scrape_command_uses_configured_sign_in(
    session: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    page = MagicMock()
    session.get_browser.return_value.get = AsyncMock(return_value=page)
    (tmp_path / "config.toml").write_text(
        'sources=[{domain="x.com",accounts=["writer"]}]\n'
        '[x]\nusername="fixture-user"\npassword="fixture-password"\n'
        'email="reader@example.test"\n'
        '[email]\nhost="imap.test"\nusername="inbox"\n'
        'password="mail-secret"\n',
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["scrape"])
    monkeypatch.setattr("dank.scrape.runner.configure_logging", MagicMock())

    async def sources(settings: Settings) -> Any:
        return MagicMock(collectable=settings.sources)

    async def collect(*args: Any, **kwargs: Any) -> AsyncIterator[ScrapeBatch]:
        yield batch("1")

    async def run(settings: Settings, **options: Any) -> None:
        session.headless = options["headless"]
        assert session.headless is False
        saved = [item async for item in scrape_x_accounts(
            settings.x, settings.sources[0].accounts, session,
        )]
        assert [post.post_id for item in saved for post in item.posts] == ["1"]

    login = AsyncMock()
    monkeypatch.setattr("dank.scrape.runner.sources_in_use", sources)
    monkeypatch.setattr("dank.scrape.runner.run_scrape", run)
    monkeypatch.setattr("dank.scrape.x.sign_in_automatically", login)
    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    monkeypatch.setattr(
        "dank.scrape.x.wait_for_x_session",
        AsyncMock(side_effect=SignInPromptError()),
    )

    scrape_main()

    login.assert_awaited_once_with(
        page,
        LoginCredentials("fixture-user", "fixture-password"),
        email="reader@example.test",
        imap=EmailSettings("imap.test", "inbox", "mail-secret"),
    )


async def test_stalled_cleanup_is_bounded_and_does_not_reopen_owned_profile(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def collect(*args: Any, **kw: Any) -> AsyncIterator[ScrapeBatch]:
        raise ConnectionError()
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


async def test_discussion_timeout_keeps_browser_for_next_post(
    session: Any,
    monkeypatch: pytest.MonkeyPatch,
):
    pages: list[Any] = []

    async def capture(page: Any, seed: RawPost, settings: Any, **kw: Any):
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
    assert pages[0] is pages[1]
    assert (
        json.loads(saved[0].posts[0].payload)["conversation_capture"]["reason"]
        == "timeout"
    )
    session.close.assert_not_awaited()
