from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank import onepassword_login
from dank.config import YouTubeOnePasswordSettings
from dank.scrape import youtube_google_login as login

REFERENCES = YouTubeOnePasswordSettings(
    "personal.1password.com", "op://Vault/Google/username",
    "op://Vault/Google/password", "op://Vault/Google/otp?attribute=otp",
)


def provider() -> Any:
    return SimpleNamespace(
        read=AsyncMock(return_value="credential"), close=Mock(),
    )


async def test_standard_stages_submit_once_and_fetch_otp_only_when_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(
        get=AsyncMock(), evaluate=AsyncMock(return_value=True),
    )
    credentials = provider()
    credentials.read.side_effect = ["test@example.test", "secret", "123456"]
    stages = [
        "username", "username", "username", "password", "password",
        "another_way", "authenticator", "otp", "otp", "signed_in",
    ]
    monkeypatch.setattr(login, "_stage", AsyncMock(side_effect=stages))
    monkeypatch.setattr(login, "LOGIN_POLL_SECONDS", 0)
    await login.google_sign_in(cast(Any, page), credentials, timeout_seconds=1)
    assert [call.args for call in credentials.read.await_args_list] == [
        ("username",), ("password",), ("otp",),
    ]
    assert page.evaluate.await_count == 5
    page.get.assert_awaited_once_with(login.YOUTUBE_SIGN_IN_URL)
    credentials.close.assert_called_once()


@pytest.mark.parametrize("stage", [
    "captcha", "rejected", "unknown", "waiting",
])
async def test_unrecognized_or_blocked_page_never_reads_credentials(
    monkeypatch: pytest.MonkeyPatch, stage: str,
) -> None:
    page = SimpleNamespace(get=AsyncMock(), evaluate=AsyncMock())
    credentials = provider()
    monkeypatch.setattr(login, "_stage", AsyncMock(return_value=stage))
    monkeypatch.setattr(login, "UNKNOWN_STAGE_SECONDS", 0)

    with pytest.raises(login.GoogleLoginError, match="manual"):
        await login.google_sign_in(
            cast(Any, page), credentials, timeout_seconds=1,
        )

    credentials.read.assert_not_awaited()
    page.evaluate.assert_not_awaited()
    credentials.close.assert_called_once()


async def test_navigation_before_vault_read_cannot_receive_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(get=AsyncMock(), evaluate=AsyncMock())
    credentials = provider()
    monkeypatch.setattr(login, "_stage", AsyncMock(side_effect=[
        "password", "waiting",
    ]))

    with pytest.raises(login.GoogleLoginError, match="form changed"):
        await login.google_sign_in(
            cast(Any, page), credentials, timeout_seconds=1,
        )

    credentials.read.assert_not_awaited()
    page.evaluate.assert_not_awaited()


async def test_form_guard_failure_stops_without_retrying_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(
        get=AsyncMock(), evaluate=AsyncMock(return_value=False),
    )
    credentials = provider()
    monkeypatch.setattr(login, "_stage", AsyncMock(return_value="password"))

    with pytest.raises(login.GoogleLoginError, match="form changed"):
        await login.google_sign_in(
            cast(Any, page), credentials, timeout_seconds=1,
        )

    credentials.read.assert_awaited_once_with("password")
    page.evaluate.assert_awaited_once()


async def test_browser_protocol_failure_cannot_print_the_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "secret-never-log-me"
    page = SimpleNamespace(get=AsyncMock(), evaluate=AsyncMock(
        side_effect=RuntimeError(secret),
    ))
    credentials = provider()
    credentials.read.return_value = secret
    monkeypatch.setattr(login, "_stage", AsyncMock(return_value="password"))

    with pytest.raises(login.GoogleLoginError) as failure:
        await login.google_sign_in(
            cast(Any, page), credentials, timeout_seconds=1,
        )

    assert secret not in str(failure.value)
    assert failure.value.__suppress_context__


async def test_native_reads_one_item_and_keeps_secrets_out_of_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps({"fields": [
        {"id": "username", "value": "person@example.test"},
        {"id": "password", "value": "private-password"},
        {"id": "otp", "value": "otpauth://totp/Test?secret="
         "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"},
    ]}).encode()
    process = SimpleNamespace(
        communicate=AsyncMock(return_value=(payload, b"")),
        returncode=0, kill=Mock(), wait=AsyncMock(),
    )
    create = AsyncMock(return_value=process)
    monkeypatch.setattr(login.shutil, "which", Mock(
        return_value="/usr/local/bin/op",
    ))
    monkeypatch.setattr(login.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(onepassword_login.time, "time", Mock(return_value=59))
    credentials = login.GoogleCredentialProvider(REFERENCES)
    assert await credentials.read("username") == "person@example.test"
    assert await credentials.read("password") == "private-password"
    assert await credentials.read("otp") == "287082"
    create.assert_awaited_once()
    assert create.call_args.args == (
        "/usr/local/bin/op", "item", "get", "--account", REFERENCES.account,
        "--vault", "Vault", "Google", "--format", "json",
    )
    assert "private-password" not in str(create.call_args)
    credentials.close()

    with pytest.raises(login.GoogleLoginError):
        await credentials.read("password")

    assert create.await_count == 1


async def test_native_timeout_kills_provider_process_without_secret_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = SimpleNamespace(
        communicate=AsyncMock(side_effect=TimeoutError("secret")),
        returncode=None, kill=Mock(), wait=AsyncMock(),
    )
    monkeypatch.setattr(login.shutil, "which", Mock(
        return_value="/usr/local/bin/op",
    ))
    monkeypatch.setattr(login.asyncio, "create_subprocess_exec", AsyncMock(
        return_value=process,
    ))

    with pytest.raises(login.GoogleLoginError, match="timed out") as failure:
        await login.GoogleCredentialProvider(REFERENCES).read("password")

    assert "secret" not in str(failure.value)
    process.kill.assert_called_once()
    process.wait.assert_awaited_once()


@pytest.mark.parametrize("field,value", [
    ("username", "person@example.test"), ("password", "password $ ` quotes\""),
    ("otp", "123456"),
])
async def test_stdio_protocol_requests_one_field_and_never_echoes_value(
    capsys: pytest.CaptureFixture[str], field: str, value: str,
) -> None:
    credentials = login.GoogleCredentialProvider(REFERENCES, stdio=True)
    stream = asyncio.StreamReader()
    stream.feed_data((json.dumps({"value": value}) + "\n").encode())
    stream.feed_eof()
    credentials._reader = stream  # pyright: ignore[reportPrivateUsage]
    assert await credentials.read(field) == value
    assert capsys.readouterr().out == "DANK_YOUTUBE_CREDENTIAL:" + field + "\n"


@pytest.mark.parametrize("response", [
    b'{"error": true}\n', b'not json\n', b'{"value": 42}\n', b'[]\n',
    b'{"value": "otpauth://do-not-submit-a-seed"}\n',
    b'{"value": "123456\\nsecret"}\n',
])
async def test_stdio_rejects_errors_invalid_fields_and_totp_seeds(
    response: bytes,
) -> None:
    credentials = login.GoogleCredentialProvider(REFERENCES, stdio=True)
    stream = asyncio.StreamReader()
    stream.feed_data(response)
    stream.feed_eof()
    credentials._reader = stream  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(login.GoogleLoginError):
        await credentials.read("otp")


async def test_missing_otp_does_not_request_or_read_a_seed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    credentials = login.GoogleCredentialProvider(
        REFERENCES._replace(otp=""), stdio=True,
    )

    with pytest.raises(login.GoogleLoginError, match=r"configure|Configure"):
        await credentials.read("otp")

    assert capsys.readouterr().out == ""


async def test_cancellation_propagates_and_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(get=AsyncMock(side_effect=asyncio.CancelledError()))
    credentials = provider()

    with pytest.raises(asyncio.CancelledError):
        await login.google_sign_in(
            cast(Any, page), credentials, timeout_seconds=1,
        )

    credentials.close.assert_called_once()
