from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import load_settings
from dank.scrape import youtube_login
from dank.scrape.zendriver import BrowserConfig


async def test_sign_in_uses_youtube_profile_and_closes_after_confirmation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
    )
    page = SimpleNamespace(evaluate=AsyncMock(return_value=True))
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    factory = Mock(return_value=context)
    monkeypatch.setattr(youtube_login, "BrowserSession", factory)
    assert await youtube_login.sign_in(settings, timeout_seconds=1)
    config: BrowserConfig = factory.call_args.args[0]
    assert config.profile_dir == tmp_path / "youtube-browser-profile"
    assert config.headless is False
    context.__aexit__.assert_awaited_once()


async def test_session_check_does_not_report_signed_out_as_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")
    settings = settings._replace(youtube=settings.youtube._replace(
        profile_dir=tmp_path / "custom-session",
    ))
    page = SimpleNamespace(evaluate=AsyncMock(return_value=False))
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    factory = Mock(return_value=context)
    monkeypatch.setattr(youtube_login, "BrowserSession", factory)
    assert not await youtube_login.sign_in(
        settings, timeout_seconds=0.01, check_only=True,
    )
    config: BrowserConfig = factory.call_args.args[0]
    assert config.profile_dir == tmp_path / "custom-session"
    assert config.headless is True
    context.__aexit__.assert_awaited_once()


async def test_login_navigation_disconnect_can_recover() -> None:
    page = SimpleNamespace(evaluate=AsyncMock(
        side_effect=[ConnectionError(), True],
    ))
    assert not await youtube_login.signed_in(cast(Any, page))
    assert await youtube_login.signed_in(cast(Any, page))
