from __future__ import annotations

import asyncio
import json
import pathlib
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import YouTubeOnePasswordSettings, load_settings
from dank.scrape import youtube_login
from dank.scrape.zendriver import BrowserConfig

AUTHENTICATED = json.dumps({"signed_in": True, "document": 1000})
SIGNED_OUT = json.dumps({"signed_in": False, "document": 1000})


@pytest.fixture(autouse=True)
def session_cookie_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        youtube_login, "save_youtube_session_from_page",
        AsyncMock(return_value=True),
    )


async def test_sign_in_uses_youtube_profile_and_closes_after_confirmation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
    )
    page = SimpleNamespace(evaluate=AsyncMock(return_value=AUTHENTICATED))
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
    page = SimpleNamespace(evaluate=AsyncMock(return_value=AUTHENTICATED))
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
        SIGNED_OUT, AUTHENTICATED, AUTHENTICATED,
    ]))
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=browser),
    )
    factory = Mock(return_value=context)
    credentials = Mock()
    flow = AsyncMock()
    finalized = AsyncMock(return_value=True)
    monkeypatch.setattr(youtube_login, "BrowserSession", factory)
    monkeypatch.setattr(youtube_login, "GoogleCredentialProvider", credentials)
    monkeypatch.setattr(youtube_login, "google_sign_in", flow)
    monkeypatch.setattr(youtube_login, "_finalize_session", finalized)
    assert await youtube_login.sign_in(
        settings, timeout_seconds=1, credentials_stdio=True,
    )
    credentials.assert_called_once_with(
        settings.youtube.onepassword, stdio=True,
    )
    flow.assert_awaited_once()
    assert youtube_login.save_youtube_session_from_page.await_count == 2  # type: ignore
    assert factory.call_args_list[0].args[0].session_file.name == (
        "youtube-session.json"
    )
    finalized.assert_awaited_once_with(page)
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
    monkeypatch.setattr(youtube_login.YouTubeSession, "check", AsyncMock(
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
    monkeypatch.setattr(youtube_login.YouTubeSession, "check", AsyncMock(
        side_effect=[False, True],
    ))
    monkeypatch.setattr(youtube_login, "_wait_for_session", AsyncMock(
        return_value=True,
    ))
    finalized = AsyncMock(return_value=True)
    monkeypatch.setattr(youtube_login, "_finalize_session", finalized)
    assert await youtube_login.sign_in(
        automated_settings(), timeout_seconds=1, manual=True,
    )
    credentials.assert_not_called()
    assert not factory.call_args_list[0].args[0].headless
    finalized.assert_awaited_once()


async def test_finalization_requires_a_new_complete_signed_in_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reloaded = False
    checks = [0, 0]

    async def evaluate(_script: str) -> float | None:
        index = int(reloaded)
        checks[index] += 1

        if not reloaded:
            return None if checks[index] == 2 else 1000.0

        if checks[index] == 1:
            return 1000.0  # The old document briefly remains after reload.

        if checks[index] == 2:
            raise ConnectionError

        return 2000.0

    async def reload(url: str) -> None:
        nonlocal reloaded
        assert url == "https://www.youtube.com/?hl=en-GB"
        reloaded = True

    page = SimpleNamespace(evaluate=evaluate, get=reload)
    monkeypatch.setattr(youtube_login, "SESSION_POLL_SECONDS", 0.001)
    monkeypatch.setattr(youtube_login, "SESSION_SETTLE_SECONDS", 0.003)
    monkeypatch.setattr(youtube_login, "SESSION_FINALIZE_SECONDS", 0.2)
    assert await youtube_login._finalize_session(  # pyright: ignore[reportPrivateUsage]
        cast(Any, page),
    )
    assert checks[0] >= 3
    assert checks[1] >= 3


@pytest.mark.parametrize("after_reload", [1000.0, None, True])
async def test_finalization_rejects_stale_or_signed_out_documents(
    monkeypatch: pytest.MonkeyPatch, *, after_reload: float | bool | None,
) -> None:
    reloaded = False

    async def evaluate(_script: str) -> float | bool | None:
        return after_reload if reloaded else 1000.0

    async def reload(url: str) -> None:
        nonlocal reloaded
        reloaded = True

    page = SimpleNamespace(
        evaluate=evaluate, get=AsyncMock(side_effect=reload),
    )
    monkeypatch.setattr(youtube_login, "SESSION_POLL_SECONDS", 0.001)
    monkeypatch.setattr(youtube_login, "SESSION_SETTLE_SECONDS", 0.002)
    monkeypatch.setattr(youtube_login, "SESSION_FINALIZE_SECONDS", 0.02)
    assert not await youtube_login._finalize_session(  # pyright: ignore[reportPrivateUsage]
        cast(Any, page),
    )
    page.get.assert_awaited_once_with("https://www.youtube.com/?hl=en-GB")


async def test_finalization_failure_does_not_restart_login_or_read_vault_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(evaluate=AsyncMock(return_value=SIGNED_OUT))
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
    monkeypatch.setattr(youtube_login, "_finalize_session", AsyncMock(
        return_value=False,
    ))
    with pytest.raises(
        youtube_login.GoogleLoginError, match="clean YouTube home",
    ):
        await youtube_login.sign_in(automated_settings(), timeout_seconds=1)

    youtube_login.save_youtube_session_from_page.assert_not_awaited()  # type: ignore
    assert factory.call_count == 1
    credentials.assert_called_once()
    flow.assert_awaited_once()


async def test_finalization_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(evaluate=AsyncMock(
        side_effect=asyncio.CancelledError,
    ), reload=AsyncMock())

    with pytest.raises(asyncio.CancelledError):
        await youtube_login._finalize_session(  # pyright: ignore[reportPrivateUsage]
            cast(Any, page),
        )

    page.reload.assert_not_awaited()


async def test_unresolved_native_state_never_opens_vault_or_rewrites_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AsyncMock()
    context.__aenter__.return_value = SimpleNamespace(
        get_browser=AsyncMock(return_value=SimpleNamespace(get=AsyncMock())),
    )
    credentials = Mock()
    monkeypatch.setattr(youtube_login, "BrowserSession", Mock(
        return_value=context,
    ))
    monkeypatch.setattr(youtube_login.YouTubeSession, "check", AsyncMock(
        return_value=None,
    ))
    monkeypatch.setattr(youtube_login, "GoogleCredentialProvider", credentials)
    assert not await youtube_login.sign_in(
        automated_settings(), timeout_seconds=1,
    )
    credentials.assert_not_called()
    youtube_login.save_youtube_session_from_page.assert_not_awaited()  # type: ignore
