from __future__ import annotations

import asyncio
import pathlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from zendriver.core.connection import ProtocolException

from dank.config import load_settings
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.x import (
    login as login_module,
    scrape_x_accounts,
)
from dank.scrape.x.session import (
    LOGIN_LIMITED_MESSAGE,
    LOGIN_REQUIRED_MESSAGE,
    X_HOME_URL,
    LoginRequiredError,
    is_signed_in,
    wait_for_x_session,
)
from dank.scrape.zendriver import BrowserConfig


def _page(states: tuple[object, ...] = ()) -> MagicMock:
    page = MagicMock()
    page.evaluate = AsyncMock(side_effect=states)
    page.browser.stopped = False

    return page


async def test_sign_in_waits_through_challenge_and_navigation() -> None:
    page = _page((False, ProtocolException("context destroyed"), True))
    await wait_for_x_session(page, timeout_seconds=6)
    assert page.evaluate.await_count == 3


async def test_temporary_limit_stops_immediately_without_retrying() -> None:
    page = _page(("limited", True))

    with pytest.raises(LoginRequiredError, match="temporarily limited"):
        await wait_for_x_session(page, timeout_seconds=10)

    assert page.evaluate.await_count == 1
    page.get.assert_not_called()


@pytest.mark.parametrize("state", [False, None, "true", 1])
async def test_page_or_cookie_presence_is_not_sign_in(state: object) -> None:
    assert not await is_signed_in(_page((state,)))


async def test_closed_browser_does_not_report_saved_session() -> None:
    page = _page((False,))
    page.browser.stopped = True

    with pytest.raises(LoginRequiredError, match="Browser closed"):
        await wait_for_x_session(page, timeout_seconds=1)


async def test_timeout_also_bounds_a_stalled_browser_command() -> None:
    page = _page()

    async def stalled(*args: Any) -> None:
        await asyncio.Event().wait()

    page.evaluate.side_effect = stalled

    with pytest.raises(LoginRequiredError, match="make x-login"):
        await wait_for_x_session(page, timeout_seconds=0.01)


@pytest.mark.parametrize("outcome", [
    None, LoginRequiredError("expired"), asyncio.CancelledError(),
    LoginRequiredError(LOGIN_LIMITED_MESSAGE),
])
async def test_login_reports_success_only_after_flushing_shared_profile(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], outcome: BaseException | None,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('[storage]\ndata_dir="private-data"\n')
    settings = load_settings(path)
    browser = MagicMock()
    page = _page(([1280, 900],))
    page.get = AsyncMock(return_value=page)
    page.set_window_size = AsyncMock()
    browser.get = AsyncMock(return_value=page)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=context)
    context.get_browser = AsyncMock(return_value=browser)

    async def closed(*args: Any) -> None:
        assert "session saved" not in capsys.readouterr().out

    context.__aexit__ = AsyncMock(side_effect=closed)
    factory = MagicMock(return_value=context)
    monkeypatch.setattr(login_module, "BrowserSession", factory)
    wait = AsyncMock(side_effect=outcome)
    monkeypatch.setattr(login_module, "wait_for_x_session", wait)

    if outcome is not None:
        with pytest.raises(type(outcome)):
            await login_module.sign_in(settings, timeout_seconds=10)
    else:
        await login_module.sign_in(settings, timeout_seconds=10)

    assert ("session saved" in capsys.readouterr().out) == (outcome is None)
    config = factory.call_args.args[0]
    assert config == BrowserConfig.from_settings(settings)
    assert not config.headless and not config.keep_open
    assert config.profile_dir == pathlib.Path("private-data/browser-profile")
    browser.get.assert_awaited_once_with("about:blank")
    page.get.assert_awaited_once_with(X_HOME_URL)
    page.set_window_size.assert_awaited_once_with(width=1280, height=900)
    wait.assert_awaited_once_with(page, timeout_seconds=10)
    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("message", [
    LOGIN_REQUIRED_MESSAGE, LOGIN_LIMITED_MESSAGE,
])
async def test_expired_session_stops_before_accounts_and_reports_recovery(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture, message: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources=[{domain="x.com",accounts=["one","two"]}]\n',
    )
    settings = load_settings(path)
    browser = MagicMock()
    browser.get = AsyncMock(return_value=_page((False,)))
    session = MagicMock(get_browser=AsyncMock(return_value=browser))
    monkeypatch.setattr(
        "dank.scrape.x.wait_for_x_session",
        AsyncMock(side_effect=LoginRequiredError(message)),
    )
    stats = SourceMetrics(1, settings.sources[0])
    token = current_source.set(stats)

    try:
        batches = [batch async for batch in scrape_x_accounts(
            settings.x, settings.sources[0].accounts, session,
        )]
    finally:
        current_source.reset(token)

    assert batches == []
    browser.get.assert_awaited_once_with(X_HOME_URL)
    assert stats.counts["fetch_failures"] == 1
    assert stats.error_type == "LoginRequiredError"
    assert message in caplog.text


async def test_saved_session_collects_without_password_or_email(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('sources=[{domain="x.com",accounts=["one","two"]}]')
    settings = load_settings(path)
    page = _page((True,))
    browser = MagicMock(get=AsyncMock(return_value=page))
    session = MagicMock(get_browser=AsyncMock(return_value=browser))
    visited: list[str] = []

    async def collect(*args: Any):
        visited.append(args[1])

        for batch in ():
            yield batch

    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    assert [batch async for batch in scrape_x_accounts(
        settings.x, settings.sources[0].accounts, session,
    )] == []
    assert visited == ["one", "two"]
    assert not hasattr(settings.x, "password")


def test_legacy_credentials_are_not_loaded(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        '[x]\npassword="old-x-secret"\nusername="reader"\n'
        '[email]\nhost="imap.test"\nusername="reader"\n'
        'password="old-mail-secret"\n',
    )
    settings = load_settings(path)
    assert "old-x-secret" not in repr(settings)
    assert "old-mail-secret" not in repr(settings)


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf"])
def test_cli_rejects_invalid_wait_before_loading_configuration(
    monkeypatch: pytest.MonkeyPatch, timeout: str,
) -> None:
    monkeypatch.setattr("sys.argv", ["x-login", "--timeout", timeout])
    loader = MagicMock()
    monkeypatch.setattr(login_module, "load_settings", loader)

    with pytest.raises(SystemExit) as error:
        login_module.main()

    assert error.value.code == 2
    loader.assert_not_called()
