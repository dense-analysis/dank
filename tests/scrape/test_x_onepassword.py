from __future__ import annotations

import asyncio
import io
import pathlib
from unittest.mock import AsyncMock, MagicMock

import pytest
from zendriver.core.connection import ProtocolException

from dank.config import ConfigError, XOnePasswordSettings, load_settings
from dank.scrape.x import onepassword
from dank.scrape.x.onepassword import (
    LoginCredentials,
    OnePasswordError,
    fill_field,
    load_credentials,
    read_credentials,
)
from dank.scrape.x.session import LoginRequiredError, wait_for_x_session

REFERENCES = XOnePasswordSettings(
    "op://Private/X/username", "op://Private/X/password",
)


def test_config_accepts_only_optional_references(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("")
    assert load_settings(path).x.onepassword is None
    path.write_text(
        '[x.onepassword]\nusername="op://Private/X/username"\n'
        'password="op://Private/X/password"\n',
    )
    assert load_settings(path).x.onepassword == REFERENCES


@pytest.mark.parametrize("value", [
    '"plaintext-secret"', '"op://missing"', '"op://vault//password"',
    '"op://vault/item/field\\nextra"', '1', 'false', '[]',
])
def test_plaintext_and_malformed_references_are_rejected_without_echo(
    tmp_path: pathlib.Path, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        '[x.onepassword]\nusername="op://Private/X/username"\n'
        f'password={value}',
    )

    with pytest.raises(ConfigError) as error:
        load_settings(path)

    assert "must be an op:// secret reference" in str(error.value)
    assert "plaintext-secret" not in str(error.value)


async def test_unconfigured_or_missing_cli_keeps_manual_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execute = AsyncMock()
    monkeypatch.setattr(onepassword.asyncio, "create_subprocess_exec", execute)
    monkeypatch.setattr(
        onepassword.shutil, "which", MagicMock(return_value=None),
    )
    assert await load_credentials(None) is None
    assert await load_credentials(REFERENCES) is None
    execute.assert_not_called()


async def test_reads_only_configured_references_without_logging_secrets(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "  p'ass $`word\\with spaces  "
    processes = [MagicMock(), MagicMock()]

    for process, value in zip(processes, ("user", secret), strict=True):
        process.returncode = 0
        process.communicate = AsyncMock(return_value=(value.encode(), b""))

    execute = AsyncMock(side_effect=processes)
    monkeypatch.setattr(onepassword.asyncio, "create_subprocess_exec", execute)
    monkeypatch.setattr(
        onepassword.shutil, "which", MagicMock(return_value="/fixture/op"),
    )
    credentials = await load_credentials(REFERENCES)
    assert credentials == LoginCredentials("user", secret)
    assert secret not in repr(credentials)
    assert secret not in caplog.text
    assert [call.args for call in execute.call_args_list] == [
        ("/fixture/op", "read", "--no-newline", reference)
        for reference in REFERENCES
    ]


async def test_denied_vault_access_discards_sensitive_cli_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    process = MagicMock(returncode=1)
    process.communicate = AsyncMock(
        return_value=(b"partial-secret", b"secret"),
    )
    monkeypatch.setattr(
        onepassword.asyncio, "create_subprocess_exec",
        AsyncMock(return_value=process),
    )
    monkeypatch.setattr(
        onepassword.shutil, "which", MagicMock(return_value="op"),
    )
    assert await load_credentials(REFERENCES) is None
    assert "continue manually" in caplog.text
    assert "secret" not in caplog.text


async def test_cli_timeout_stops_the_process_and_keeps_manual_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = MagicMock(returncode=None)

    async def wait_forever():
        await asyncio.Event().wait()

    process.communicate = wait_forever
    process.wait = AsyncMock()
    monkeypatch.setattr(
        onepassword.asyncio, "create_subprocess_exec",
        AsyncMock(return_value=process),
    )
    monkeypatch.setattr(
        onepassword.shutil, "which", MagicMock(return_value="op"),
    )
    monkeypatch.setattr(onepassword, "OP_TIMEOUT_SECONDS", 0.01)
    assert await load_credentials(REFERENCES) is None
    process.kill.assert_called_once()
    process.wait.assert_awaited_once()


def test_pipe_preserves_password_and_redacts_repr() -> None:
    secret = b"  p'ass $`word\\with spaces  "
    credentials = read_credentials(io.BytesIO(b"user\0" + secret + b"\0"))
    assert credentials.password.encode() == secret
    assert secret.decode() not in repr(credentials)


@pytest.mark.parametrize("payload", [
    b"user\0partial", b"user\0password\0extra", b"user\0\0",
    b"user\0pass\nword\0", b"user\0pass\tword\0", b"user\0\xff\0",
    b"user\0" + b"p" * 8193 + b"\0",
])
def test_pipe_rejects_partial_or_unsafe_values(payload: bytes) -> None:
    with pytest.raises(OnePasswordError):
        read_credentials(io.BytesIO(payload))


async def test_hidden_fields_are_skipped_and_user_input_is_preserved() -> None:
    hidden = MagicMock(apply=AsyncMock(return_value=None))
    occupied = MagicMock(apply=AsyncMock(return_value="occupied"))
    page = MagicMock(query_selector_all=AsyncMock(
        return_value=[hidden, occupied],
    ))
    assert await fill_field(page, "input", "secret")
    hidden.send_keys.assert_not_called()
    occupied.send_keys.assert_not_called()


async def test_fill_error_cannot_expose_driver_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    element = MagicMock(
        apply=AsyncMock(return_value="empty"),
        send_keys=AsyncMock(side_effect=RuntimeError("private-password")),
    )
    page = MagicMock(query_selector_all=AsyncMock(return_value=[element]))
    assert await fill_field(page, "input", "private-password")
    assert "private-password" not in caplog.text


async def test_navigation_does_not_consume_an_autofill_attempt() -> None:
    page = MagicMock(query_selector_all=AsyncMock(
        side_effect=ProtocolException("context replaced"),
    ))
    assert not await fill_field(page, "input", "secret")


async def test_login_fills_each_field_once_without_submitting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = MagicMock(evaluate=AsyncMock(side_effect=[False, False, True]))
    page.browser.stopped = False
    fill = AsyncMock(return_value=True)
    monkeypatch.setattr("dank.scrape.x.session.fill_field", fill)
    monkeypatch.setattr("dank.scrape.x.session.SESSION_POLL_SECONDS", 0.001)
    await wait_for_x_session(
        page, timeout_seconds=1,
        credentials=LoginCredentials("user", "secret"),
    )
    assert fill.await_count == 2
    page.get.assert_not_called()
    page.send.assert_not_called()


async def test_limit_prevents_any_autofill(monkeypatch: pytest.MonkeyPatch):
    page = MagicMock(evaluate=AsyncMock(return_value="limited"))
    fill = AsyncMock()
    monkeypatch.setattr("dank.scrape.x.session.fill_field", fill)

    with pytest.raises(LoginRequiredError, match="temporarily limited"):
        await wait_for_x_session(
            page, timeout_seconds=1,
            credentials=LoginCredentials("user", "secret"),
        )

    fill.assert_not_called()
