from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import YouTubeOnePasswordSettings, load_settings
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
    config: BrowserConfig = factory.call_args_list[0].args[0]
    assert config.profile_dir == tmp_path / "youtube-browser-profile"
    assert config.headless is False
    assert context.__aexit__.await_count == 2
    assert factory.call_args_list[1].args[0].headless is True


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


def automated_settings() -> Any:
    settings = load_settings("config.native.example.toml")

    return settings._replace(youtube=settings.youtube._replace(
        onepassword=YouTubeOnePasswordSettings(
            "personal", "op://Vault/Google/username",
            "op://Vault/Google/password", "op://Vault/Google/otp?attribute=otp",
        ),
    ))


@pytest.mark.parametrize("check_only", [False, True])
async def test_saved_session_never_opens_vault(
    monkeypatch: pytest.MonkeyPatch, *, check_only: bool,
) -> None:
    page = SimpleNamespace(evaluate=AsyncMock(return_value=True))
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    credentials = Mock()
    monkeypatch.setattr(youtube_login, "BrowserSession", Mock(
        return_value=context,
    ))
    monkeypatch.setattr(youtube_login, "GoogleCredentialProvider", credentials)
    assert await youtube_login.sign_in(
        automated_settings(), timeout_seconds=1, check_only=check_only,
    )
    credentials.assert_not_called()


async def test_automated_login_checks_saved_session_before_lazy_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = automated_settings()
    page = SimpleNamespace(evaluate=AsyncMock(side_effect=[
        "signed_out", True, True,
    ]))
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    factory = Mock(return_value=context)
    credentials = Mock()
    flow = AsyncMock()
    monkeypatch.setattr(youtube_login, "BrowserSession", factory)
    monkeypatch.setattr(youtube_login, "GoogleCredentialProvider", credentials)
    monkeypatch.setattr(youtube_login, "google_sign_in", flow)
    assert await youtube_login.sign_in(
        settings, timeout_seconds=1, credentials_stdio=True,
    )
    credentials.assert_called_once_with(
        settings.youtube.onepassword, stdio=True,
    )
    flow.assert_awaited_once()
    assert factory.call_count == 2
    assert all(call.args[0].headless for call in factory.call_args_list)


async def test_session_must_survive_reopening_before_reporting_saved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AsyncMock()
    browser = SimpleNamespace(get=AsyncMock())
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    monkeypatch.setattr(youtube_login, "BrowserSession", Mock(
        return_value=context,
    ))
    monkeypatch.setattr(youtube_login, "_wait_for_session", AsyncMock(
        side_effect=[True, False],
    ))
    assert not await youtube_login.sign_in(
        automated_settings(), timeout_seconds=1,
    )


async def test_manual_login_does_not_use_configured_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AsyncMock()
    browser = SimpleNamespace(get=AsyncMock())
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    credentials = Mock()
    factory = Mock(return_value=context)
    monkeypatch.setattr(youtube_login, "BrowserSession", factory)
    monkeypatch.setattr(youtube_login, "GoogleCredentialProvider", credentials)
    monkeypatch.setattr(youtube_login, "_wait_for_session", AsyncMock(
        side_effect=[False, True, True],
    ))
    assert await youtube_login.sign_in(
        automated_settings(), timeout_seconds=1, manual=True,
    )
    credentials.assert_not_called()
    assert not factory.call_args_list[0].args[0].headless
