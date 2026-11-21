from __future__ import annotations

import io
import json
import signal
import subprocess
import sys
from unittest.mock import MagicMock, Mock

import pytest

from dank import run_youtube_login as launcher

REFERENCES = {
    "account": "account-id",
    "username": "op://vault/google/username",
    "password": "op://vault/google/password",
    "otp": "op://vault/google/totp?attribute=otp",
}


@pytest.fixture
def spawn(monkeypatch: pytest.MonkeyPatch) -> Mock:
    spawn = Mock()
    monkeypatch.setattr(launcher.subprocess, "Popen", spawn)

    return spawn


def child(*lines: str, running: bool = False) -> MagicMock:
    process = MagicMock()
    process.stdout = io.StringIO("".join(line + "\n" for line in lines))
    process.wait.return_value = 0
    process.poll.return_value = None if running else 0

    return process


def test_credentials_are_requested_lazily_and_only_passed_over_stdin(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    spawn: Mock,
) -> None:
    secret = "a private 'password' with $shell characters"
    values = {"username": "private-user", "password": secret, "otp": "123456"}
    read = Mock(side_effect=values.__getitem__)
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    process = spawn.return_value = child(
        *(launcher.REQUEST_PREFIX + field for field in values),
        "Session saved",
    )
    command = ["compose", "login-fixture"]

    assert launcher.broker(command, REFERENCES) == 0
    assert [call.args[0] for call in read.call_args_list] == list(values)
    assert [
        json.loads(call.args[0]) for call in process.stdin.write.call_args_list
    ] == [{"value": value} for value in values.values()]
    output = capsys.readouterr()
    assert output.out == "Session saved\n"
    assert not output.err
    assert all(value not in repr(spawn.call_args) for value in values.values())
    assert all(value not in output.out for value in values.values())


def test_unknown_request_never_reads_the_vault(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    spawn: Mock,
) -> None:
    read = Mock()
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    process = spawn.return_value = child(
        launcher.REQUEST_PREFIX + "op://other/vault/secret",
    )

    assert launcher.broker(["login-fixture"], REFERENCES) == 0
    read.assert_not_called()
    assert json.loads(process.stdin.write.call_args.args[0]) == {"error": True}
    assert "not configured" in capsys.readouterr().err


def test_otp_is_not_requested_when_google_does_not_need_it(
    monkeypatch: pytest.MonkeyPatch, spawn: Mock,
) -> None:
    read = Mock(return_value="private-user")
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    process = spawn.return_value = child(launcher.REQUEST_PREFIX + "username")

    assert launcher.broker(["login-fixture"], REFERENCES) == 0
    read.assert_called_once_with("username")
    assert json.loads(process.stdin.write.call_args.args[0]) == {
        "value": "private-user",
    }


def test_reused_session_never_asks_for_references_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
):
    call = Mock(side_effect=[0, 0])
    run = Mock()
    monkeypatch.setattr(launcher.subprocess, "call", call)
    monkeypatch.setattr(launcher.subprocess, "run", run)
    assert launcher.run(["docker", "compose"], []) == 0
    run.assert_not_called()
    assert "--check-session" in call.call_args_list[1].args[0]


def test_default_without_references_does_not_open_vnc(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    call = Mock(side_effect=[0, 3])
    monkeypatch.setattr(launcher.subprocess, "call", call)
    monkeypatch.setattr(launcher.subprocess, "run", Mock(
        return_value=subprocess.CompletedProcess([], 0, "{}", ""),
    ))
    assert launcher.run(["docker", "compose"], []) == 3
    assert call.call_count == 2
    assert "--manual" in capsys.readouterr().err


def test_explicit_manual_mode_keeps_browser_window(
    monkeypatch: pytest.MonkeyPatch,
):
    call = Mock(side_effect=[0, 3, 0])
    monkeypatch.setattr(launcher.subprocess, "call", call)
    assert launcher.run(["docker", "compose"], ["--manual"]) == 0
    assert "127.0.0.1:6081:6080" in call.call_args.args[0]


@pytest.mark.parametrize("payload", [
    "null", "[]", '{"username":"plaintext"}',
    json.dumps({**REFERENCES, "password": "plaintext-secret"}),
])
def test_invalid_references_do_not_echo_payload(payload: str):
    with pytest.raises(launcher.CredentialAccessError) as error:
        launcher.parse_references(payload)

    assert "plaintext-secret" not in str(error.value)


def test_reference_contract():
    assert launcher.parse_references(json.dumps(REFERENCES)) == REFERENCES
    assert launcher.parse_references("{}") == {}


def test_closed_credential_pipe_still_terminates_child(
    monkeypatch: pytest.MonkeyPatch, spawn: Mock,
) -> None:
    process = spawn.return_value = child(
        launcher.REQUEST_PREFIX + "password", running=True,
    )
    process.stdin.write.side_effect = BrokenPipeError
    credentials = Mock(read=Mock(return_value="secret"))
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=credentials),
    )

    with pytest.raises(BrokenPipeError):
        launcher.broker(["login-fixture"], REFERENCES)

    credentials.close.assert_called_once_with()
    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=30)


def test_termination_closes_login_child_without_vault_access(
    monkeypatch: pytest.MonkeyPatch, spawn: Mock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    process = spawn.return_value = child(running=True)
    process.stdout = MagicMock()
    credentials = Mock()
    handler = Mock()

    def dispatch() -> None:
        handler.call_args.args[1](signal.SIGTERM, None)

    def run(compose: list[str], arguments: list[str]) -> int:
        return launcher.broker([*compose, *arguments], REFERENCES)

    process.stdout.__iter__.side_effect = dispatch
    monkeypatch.setattr(signal, "signal", handler)
    monkeypatch.setattr(launcher, "run", run)
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=credentials),
    )
    monkeypatch.setattr(
        sys, "argv", ["youtube-login", "fixture-compose", "--"],
    )

    with pytest.raises(SystemExit) as exited:
        launcher.main()

    assert exited.value.code == 130
    assert "cancelled" in capsys.readouterr().err
    credentials.read.assert_not_called()
    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=30)
