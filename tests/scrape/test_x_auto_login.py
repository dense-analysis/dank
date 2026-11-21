import datetime
import pathlib
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from dank.config import ConfigError, EmailSettings, load_settings
from dank.scrape.x import auto_login
from dank.scrape.x.onepassword import (
    PASSWORD_SELECTOR,
    USERNAME_SELECTOR,
    LoginCredentials,
)
from dank.scrape.x.session import (
    EMAIL_ADDRESS_FIELDS,
    EMAIL_CODE_FIELDS,
    X_LOGIN_URL,
    AppVerificationRequiredError,
    LoginRequiredError,
)

CREDENTIALS = LoginCredentials("fixture-user", "fixture-password")


def test_private_config_resolves_and_redacts_direct_credentials(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "fixture.toml"
    path.write_text(
        '[x]\nusername="fixture-user"\npassword="fixture-password"',
    )
    settings = load_settings(path)

    assert settings.x.credentials == CREDENTIALS
    assert "fixture-password" not in repr(settings)


def test_incomplete_config_credentials_report_only_key_names(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "fixture.toml"
    path.write_text('[x]\npassword="fixture-password"')

    with pytest.raises(ConfigError, match=r"x.username and x.password") as err:
        load_settings(path)

    assert "fixture-password" not in str(err.value)


async def test_submits_visible_forms_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(auto_login, "LOGIN_KEY_PAUSE_SECONDS", 0)
    username = MagicMock(
        apply=AsyncMock(side_effect=["empty", False, "occupied", True]),
        send_keys=AsyncMock(), scroll_into_view=AsyncMock(),
        mouse_click=AsyncMock(),
    )
    password = MagicMock(
        apply=AsyncMock(side_effect=["empty", True]), send_keys=AsyncMock(),
    )
    hidden = MagicMock(apply=AsyncMock(return_value=None))
    page = MagicMock(
        get=AsyncMock(), evaluate=AsyncMock(side_effect=[
            "login", "login", "login", "login", True,
        ]), query_selector_all=AsyncMock(side_effect=[
            [username], [username], [hidden], [hidden, password],
        ]),
    )

    await auto_login.sign_in_automatically(page, CREDENTIALS)

    page.get.assert_awaited_once_with(X_LOGIN_URL)
    assert page.query_selector_all.await_args_list == [
        call(USERNAME_SELECTOR), call(USERNAME_SELECTOR),
        call(PASSWORD_SELECTOR), call(PASSWORD_SELECTOR),
    ]
    username.mouse_click.assert_awaited_once()
    assert username.send_keys.await_args_list == [
        call(char) for char in CREDENTIALS.username
    ]
    assert password.send_keys.await_args_list == [
        call(char) for char in CREDENTIALS.password
    ]
    hidden.send_keys.assert_not_called()


@pytest.mark.parametrize(("states", "message"), [
    (["verify"], "requires verification beyond an emailed code"),
    (["limited"], "temporarily limited"),
    (["app_verification"], "Open the X app on your phone"),
    (["email_address"], "asks for the account email address"),
    (["email_code"], "IMAP credentials are missing"),
])
async def test_verification_and_limits_stop_before_entering_credentials(
    states: list[object], message: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = MagicMock(get=AsyncMock(), evaluate=AsyncMock(side_effect=states))
    mailbox = EmailSettings("imap.test", "login-inbox", "fixture-secret")
    wait = AsyncMock()
    monkeypatch.setattr(auto_login, "wait_for_code", wait)
    manual = states[0] in ("verify", "app_verification")

    with pytest.raises(LoginRequiredError, match=message):
        await auto_login.sign_in_automatically(
            page, CREDENTIALS, imap=mailbox if manual else None,
        )

    page.query_selector_all.assert_not_called()
    wait.assert_not_awaited()


async def test_never_resubmits_after_x_fails_to_confirm_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(auto_login, "SIGN_IN_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(auto_login, "LOGIN_KEY_PAUSE_SECONDS", 0)
    field = MagicMock(
        apply=AsyncMock(side_effect=["empty", True, "empty", True]),
        send_keys=AsyncMock(), scroll_into_view=AsyncMock(),
        mouse_click=AsyncMock(),
    )
    page = MagicMock(
        get=AsyncMock(),
        evaluate=AsyncMock(side_effect=["login", "login", None]),
        query_selector_all=AsyncMock(return_value=[field]),
    )

    with pytest.raises(LoginRequiredError, match="did not confirm"):
        await auto_login.sign_in_automatically(page, CREDENTIALS)

    assert field.send_keys.await_args_list == [
        call(char) for char in CREDENTIALS.username + CREDENTIALS.password
    ]


async def test_email_verification_uses_one_code_from_this_attempt(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(auto_login, "LOGIN_KEY_PAUSE_SECONDS", 0)
    mailbox = EmailSettings("imap.test", "login-inbox", "fixture-secret")
    address = "x-account@example.test"
    wait = AsyncMock(return_value="AB1234")
    monkeypatch.setattr(auto_login, "wait_for_code", wait)
    caplog.set_level("INFO", logger=auto_login.__name__)
    fields = [MagicMock(
        apply=AsyncMock(side_effect=["empty", True]), send_keys=AsyncMock(),
        scroll_into_view=AsyncMock(), mouse_click=AsyncMock(),
    ) for _ in range(4)]
    page = MagicMock(
        get=AsyncMock(),
        evaluate=AsyncMock(side_effect=[
            "login", "email_address", "login", "email_code", True,
        ]),
        query_selector_all=AsyncMock(
            side_effect=[[field] for field in fields],
        ),
    )
    start = datetime.datetime.now(datetime.UTC).replace(microsecond=0)

    await auto_login.sign_in_automatically(
        page, CREDENTIALS, email=address, imap=mailbox,
    )

    assert page.query_selector_all.await_args_list == [
        call(steps) for steps in (
            USERNAME_SELECTOR, EMAIL_ADDRESS_FIELDS, PASSWORD_SELECTOR,
            EMAIL_CODE_FIELDS,
        )
    ]
    wait.assert_awaited_once()
    assert "X requires email verification. Checking the IMAP inbox." in (
        caplog.text
    )
    args = wait.await_args
    assert args is not None and args.args[0] == mailbox
    assert start <= args.args[1] <= datetime.datetime.now(datetime.UTC)

    values = (CREDENTIALS.username, address, CREDENTIALS.password, "AB1234")

    for field, value in zip(fields, values, strict=True):
        expected = [call(char) for char in value]
        assert field.send_keys.await_args_list == expected


async def test_missing_email_code_stops_without_a_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mailbox = EmailSettings("imap.test", "login-inbox", "fixture-secret")
    wait = AsyncMock(return_value=None)
    monkeypatch.setattr(auto_login, "wait_for_code", wait)
    page = MagicMock(
        get=AsyncMock(), evaluate=AsyncMock(return_value="email_code"),
    )

    with pytest.raises(
        LoginRequiredError, match="No recent X verification email",
    ):
        await auto_login.sign_in_automatically(page, CREDENTIALS, imap=mailbox)

    wait.assert_awaited_once()
    page.query_selector_all.assert_not_called()


async def test_app_verification_stops_sign_in_after_password_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mailbox = EmailSettings("imap.test", "login-inbox", "fixture-secret")
    wait = AsyncMock()
    monkeypatch.setattr(auto_login, "wait_for_code", wait)
    field = MagicMock(apply=AsyncMock(side_effect=[
        "occupied", True, "occupied", True,
    ]))
    page = MagicMock(
        get=AsyncMock(), evaluate=AsyncMock(side_effect=[
            "login", "login", "app_verification",
        ]),
        query_selector_all=AsyncMock(return_value=[field]),
    )

    with pytest.raises(
        AppVerificationRequiredError, match="Open the X app on your phone",
    ):
        await auto_login.sign_in_automatically(page, CREDENTIALS, imap=mailbox)

    page.get.assert_awaited_once_with(X_LOGIN_URL)
    assert page.evaluate.await_count == 3
    assert page.query_selector_all.await_args_list == [
        call(USERNAME_SELECTOR), call(PASSWORD_SELECTOR),
    ]
    wait.assert_not_awaited()
