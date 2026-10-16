from __future__ import annotations

import json
import os
import pathlib
import shlex
import sys
from subprocess import PIPE, Popen

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
USERNAME = "fixture-user"
PASSWORD = "  p'ass $`word\\ $(touch should-not-exist)  "


@pytest.mark.parametrize("scenario", [
    "configured", "missing", "denied", "manual", "unconfigured",
    "saved", "check-error",
])
def test_optional_host_cli_passes_secrets_only_through_stdin(
    tmp_path: pathlib.Path, scenario: str,
) -> None:
    workspace = tmp_path / "checkout with spaces"
    binary_dir = workspace / "bin"
    binary_dir.mkdir(parents=True)
    compose = workspace / "compose.py"
    compose.write_text(
        "import json, os, pathlib, sys\n"
        "args=sys.argv[1:]\n"
        "with pathlib.Path('calls').open('a') as f:\n"
        "    f.write(json.dumps(args)+'\\n')\n"
        "if '--check-session' in args:\n"
        f"    if {scenario!r} == 'saved':\n"
        "        print('Already signed in to X. Reusing the saved session.')\n"
        "        sys.exit(0)\n"
        f"    sys.exit(1 if {scenario!r} == 'check-error' else 3)\n"
        "elif '--onepassword-references' in args:\n"
        "    if '--build' in args: print('Docker build progress')\n"
        f"    if {scenario!r} != 'unconfigured':\n"
        "        print('op://Personal Vault/X/username')\n"
        "        print('op://Personal Vault/X/password')\n"
        "elif '--credentials-stdin' in args:\n"
        f"    expected={USERNAME.encode()!r}+b'\\0'\n"
        f"    expected+={PASSWORD.encode()!r}+b'\\0'\n"
        "    assert sys.stdin.buffer.read() == expected\n"
        "    assert 'login_username' not in os.environ\n"
        "    assert 'login_password' not in os.environ\n"
        "    print('Received expected credentials privately')\n"
        "else:\n"
        "    print('Manual sign-in')\n",
    )
    op_script = workspace / "op.py"
    op_script.write_text(
        "import pathlib, sys\n"
        "pathlib.Path('op-called').touch()\n"
        "assert sys.argv[1:3] == ['read','--no-newline']\n"
        f"if {scenario!r} == 'denied':\n"
        f"    print({PASSWORD!r},file=sys.stderr)\n"
        "    sys.exit(1)\n"
        f"sys.stdout.write({USERNAME!r} if sys.argv[-1].endswith('/username')"
        f" else {PASSWORD!r})\n",
    )

    if scenario != "missing":
        op = binary_dir / "op"
        op.write_text(
            "#!/bin/sh\nexec "
            + shlex.join([sys.executable, str(op_script)]) + ' "$@"\n',
        )
        op.chmod(0o755)

    command = [
        "/bin/bash", str(ROOT / "src/dank/run_x_login.sh"), sys.executable,
        str(compose), "--", "--timeout", "30",
    ]

    if scenario == "manual":
        command.append("--manual")

    process = Popen(
        command, cwd=workspace, stdout=PIPE, stderr=PIPE, text=True,
        env={**os.environ, "PATH": str(binary_dir),
             "login_username": "inherited", "login_password": "inherited"},
    )
    stdout, stderr = process.communicate(timeout=10)
    expected_status = 1 if scenario == "check-error" else 0
    assert process.returncode == expected_status, stderr
    assert PASSWORD not in stdout + stderr
    assert USERNAME not in stdout + stderr
    assert not (workspace / "should-not-exist").exists()
    calls = [
        json.loads(line)
        for line in (workspace / "calls").read_text().splitlines()
    ]
    assert PASSWORD not in repr(calls)

    if scenario in {"saved", "check-error"}:
        assert "--check-session" in calls[-1]
        assert len(calls) == 2
        assert not (workspace / "op-called").exists()
        assert "--credentials-stdin" not in stdout
        assert "6080" not in stdout
        return

    if scenario == "configured":
        assert "Received expected credentials privately" in stdout
        assert "--credentials-stdin" in calls[-1]
        assert "--build" not in calls[-1]
        assert calls[0] == ["build", "dank"]
    else:
        assert "Manual sign-in" in stdout
        assert "--credentials-stdin" not in calls[-1]

    assert "127.0.0.1:6080:6080" in calls[-1]
    assert not (workspace / "logs").exists()
