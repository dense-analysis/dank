from __future__ import annotations

import asyncio
import pathlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.scrape import zendriver as browser_module
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
    ChromiumConfig,
)


@pytest.fixture(autouse=True)
def browser_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unit tests launch a fake browser and need no installed Chromium.
    monkeypatch.setattr(
        browser_module, "_resolve_browser_executable",
        MagicMock(return_value="/fixture/chromium"),
    )


async def _session(
    monkeypatch: pytest.MonkeyPatch,
    browser: MagicMock,
    *,
    keep_open: bool = False,
) -> BrowserSession:
    monkeypatch.setattr(
        browser_module.zendriver, "start", AsyncMock(return_value=browser),
    )
    session = BrowserSession(BrowserConfig(keep_open=keep_open))
    await session.get_browser()

    return session


def _browser() -> MagicMock:
    browser = MagicMock()
    browser.stopped = False
    browser.wait = AsyncMock()
    browser.connection.closed = False
    browser.connection.send = AsyncMock()
    browser.connection.aclose = AsyncMock()
    browser.stop = AsyncMock()

    return browser


async def test_graceful_close_waits_for_process_before_stopping_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    order: list[str] = []

    def exited() -> None:
        order.append("process exited")
        browser.stopped = True

    async def send(command: Any) -> None:
        assert next(command) == {"method": "Browser.close"}
        order.append("close requested")
        asyncio.get_running_loop().call_soon(exited)

    async def stop() -> None:
        order.append("driver stopped")
        assert browser.stopped

    browser.connection.send.side_effect = send
    browser.stop.side_effect = stop
    monkeypatch.setattr(browser_module, "BROWSER_SHUTDOWN_POLL_SECONDS", 0.001)
    session = await _session(monkeypatch, browser)
    await session.close()

    assert order == ["close requested", "process exited", "driver stopped"]
    browser.connection.aclose.assert_awaited_once()
    browser.stop.assert_awaited_once()


@pytest.mark.parametrize("hang", ["command", "process"])
async def test_graceful_timeout_still_stops_owned_browser(
    monkeypatch: pytest.MonkeyPatch,
    hang: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    browser = _browser()
    caplog.set_level("DEBUG", logger="dank.scrape.zendriver")

    async def send(_command: Any) -> None:
        await asyncio.Event().wait()

    if hang == "command":
        browser.connection.send.side_effect = send

    monkeypatch.setattr(
        browser_module, "BROWSER_SHUTDOWN_TIMEOUT_SECONDS", 0.01,
    )
    session = await _session(monkeypatch, browser)

    async with asyncio.timeout(1):
        await session.close()

    browser.stop.assert_awaited_once()
    browser.connection.send.assert_awaited_once()
    assert "graceful shutdown timed out" in caplog.text


async def test_closed_connection_falls_back_to_driver_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    browser.connection.send.side_effect = ConnectionError("browser gone")
    session = await _session(monkeypatch, browser)
    await session.close()

    browser.stop.assert_awaited_once()


async def test_already_stopped_browser_only_releases_driver_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    browser.stopped = True
    session = await _session(monkeypatch, browser)
    await session.close()

    browser.connection.send.assert_not_awaited()
    browser.stop.assert_awaited_once()


async def test_close_cancellation_still_stops_owned_browser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    closing = asyncio.Event()

    async def send(_command: Any) -> None:
        closing.set()
        await asyncio.Event().wait()

    browser.connection.send.side_effect = send
    session = await _session(monkeypatch, browser)
    task = asyncio.create_task(session.close())
    await closing.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    browser.stop.assert_awaited_once()


async def test_unstarted_session_close_does_not_launch_browser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = AsyncMock()
    monkeypatch.setattr(browser_module.zendriver, "start", start)
    await BrowserSession(BrowserConfig()).close()

    start.assert_not_awaited()


async def test_failed_scrape_still_holds_debug_browser_before_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    browser.stopped = True
    session = await _session(monkeypatch, browser, keep_open=True)
    order: list[str] = []

    async def hold() -> None:
        order.append("held for inspection")

    async def stop() -> None:
        order.append("stopped")

    monkeypatch.setattr(session, "hold_open", hold)
    browser.stop.side_effect = stop
    error = RuntimeError("scrape failed")
    await session.__aexit__(RuntimeError, error, None)

    assert order == ["held for inspection", "stopped"]
    browser.connection.send.assert_not_awaited()


async def test_profile_lock_prevents_overlapping_login_and_scrape(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    browser.stopped = True
    start = AsyncMock(return_value=browser)
    monkeypatch.setattr(browser_module.zendriver, "start", start)
    config = BrowserConfig(profile_dir=tmp_path / "browser-profile")

    async with BrowserSession(config) as first:
        await first.get_browser()

        async with BrowserSession(config) as second:
            with pytest.raises(BrowserProfileBusyError, match="in use"):
                await second.get_browser()

        start.assert_awaited_once()

    async with BrowserSession(config) as third:
        await third.get_browser()

    assert start.await_count == 2
    assert (tmp_path / "browser-profile").stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("headless", [False, True])
async def test_browser_keeps_normal_features_and_persistent_transport(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
    *, headless: bool,
) -> None:
    browser = _browser()
    browser.stopped = True
    start = AsyncMock(return_value=browser)
    monkeypatch.setattr(browser_module.zendriver, "start", start)
    profile = tmp_path / "browser-profile"

    async with BrowserSession(BrowserConfig(
        profile_dir=profile, headless=headless,
    )) as session:
        await session.get_browser()

    config: ChromiumConfig = start.call_args.kwargs["config"]
    config.host = "127.0.0.1"
    config.port = 9222
    args = config()
    assert f"--user-data-dir={profile}" in args
    assert "--remote-debugging-host=127.0.0.1" in args
    assert "--remote-debugging-port=9222" in args
    assert ("--headless=new" in args) == headless
    assert config.disable_webrtc is False
    assert config.disable_webgl is False
    assert not config.user_agent and not config.lang
    assert not any(arg.startswith((
        "--disable-features", "--webrtc-ip-handling-policy",
        "--force-webrtc", "--remote-allow-origins",
    )) for arg in args)


async def test_failed_browser_start_releases_profile_lock(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser = _browser()
    browser.stopped = True
    start = AsyncMock(side_effect=[RuntimeError("startup"), browser])
    monkeypatch.setattr(browser_module.zendriver, "start", start)
    config = BrowserConfig(profile_dir=tmp_path / "browser-profile")

    with pytest.raises(RuntimeError, match="startup"):
        async with BrowserSession(config) as first:
            await first.get_browser()

    async with BrowserSession(config) as second:
        await second.get_browser()

    assert start.await_count == 2
