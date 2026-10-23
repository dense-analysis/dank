from __future__ import annotations

import json
import subprocess
import sys
from subprocess import Popen
from typing import Any
from unittest.mock import Mock

import pytest

from dank import run_youtube_login as launcher

REFERENCES = {
    "account": "account-id",
    "username": "op://vault/google/username",
    "password": "op://vault/google/password",
    "otp": "op://vault/google/totp?attribute=otp",
}


def test_credentials_are_requested_lazily_and_only_passed_over_stdin(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(launcher.subprocess, "Popen", Popen)
    secret = "a private 'password' with $shell characters"
    values = {"username": "private-user", "password": secret, "otp": "123456"}
    requested: list[str] = []

    def read(field: str) -> str:
        requested.append(field)
        return values[field]

    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    code = '''
import json, sys
for field in ('username', 'password', 'otp'):
    print('DANK_YOUTUBE_CREDENTIAL:' + field, flush=True)
    value = json.loads(sys.stdin.readline())['value']
    assert value and value not in str(sys.argv)
print('Session saved', flush=True)
'''
    assert launcher.broker([sys.executable, "-c", code], REFERENCES) == 0
    assert requested == ["username", "password", "otp"]
    output = capsys.readouterr()
    assert output.out == "Session saved\n"
    assert not output.err
    assert all(value not in output.out for value in values.values())


def test_unknown_request_never_reads_the_vault(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(launcher.subprocess, "Popen", Popen)
    read = Mock()
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    code = '''
import json, sys
print('DANK_YOUTUBE_CREDENTIAL:op://other/vault/secret', flush=True)
assert json.loads(sys.stdin.readline()) == {'error': True}
'''
    assert launcher.broker([sys.executable, "-c", code], REFERENCES) == 0
    read.assert_not_called()
    assert "not configured" in capsys.readouterr().err


def test_otp_is_not_requested_when_google_does_not_need_it(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(launcher.subprocess, "Popen", Popen)
    read = Mock(return_value="private-user")
    monkeypatch.setattr(
        launcher, "OnePasswordLogin", Mock(return_value=Mock(read=read)),
    )
    code = '''
import json, sys
print('DANK_YOUTUBE_CREDENTIAL:username', flush=True)
assert json.loads(sys.stdin.readline())['value']
'''
    assert launcher.broker([sys.executable, "-c", code], REFERENCES) == 0
    read.assert_called_once_with("username")


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
    monkeypatch: pytest.MonkeyPatch,
):
    children: list[subprocess.Popen[str]] = []

    def start(*args: Any, **kwargs: Any) -> Popen[str]:
        process = Popen(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(launcher.subprocess, "Popen", start)
    monkeypatch.setattr(
        launcher, "OnePasswordLogin",
        Mock(return_value=Mock(read=Mock(return_value="secret"))),
    )
    code = """
import os, time
os.close(0)
print('DANK_YOUTUBE_CREDENTIAL:password', flush=True)
time.sleep(30)
"""

    with pytest.raises(BrokenPipeError):
        launcher.broker([sys.executable, "-c", code], REFERENCES)

    assert len(children) == 1
    assert children[0].poll() is not None


def test_termination_closes_login_child_without_vault_access():
    import os

    code = """
import sys
from dank import run_youtube_login as launcher
references = {
 'account':'fixture','username':'op://vault/item/username',
 'password':'op://vault/item/password','otp':''
}
child = "import os,time; print(os.getpid(),flush=True); time.sleep(30)"
launcher.run = lambda *_: launcher.broker(
 [sys.executable, '-c', child], references
)
sys.argv = ['youtube-login', 'fixture-compose', '--']
launcher.main()
"""
    process = Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert process.stdout is not None

    try:
        child_pid = int(process.stdout.readline())
        process.terminate()
        _stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 130
        assert "cancelled" in stderr

        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
