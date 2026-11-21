from __future__ import annotations

import json
import os
import pathlib
import shlex
import shutil
import sys
from subprocess import PIPE, Popen

import pytest

pytestmark = pytest.mark.shell_io


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


@pytest.mark.parametrize(
    "scenario", ["missing", "saved", "configured", "denied"],
)
def test_scrape_requests_host_credentials_only_when_needed(
    tmp_path: pathlib.Path, scenario: str,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    for name in ("mkfifo", "mktemp", "rm", "rmdir"):
        executable = shutil.which(name)
        assert executable is not None
        (bin_dir / name).symlink_to(executable)

    if scenario != "missing":
        op = bin_dir / "op"
        op_script = tmp_path / "op.py"
        op_script.write_text(
            "import sys\n"
            f"if {scenario!r} == 'denied': sys.exit(1)\n"
            f"sys.stdout.write({USERNAME!r} if sys.argv[-1].endswith('/user')"
            f" else {PASSWORD!r})\n",
        )
        op.write_text(
            "#!/bin/sh\nexec "
            + shlex.join([sys.executable, str(op_script)]) + ' "$@"\n',
        )
        op.chmod(0o700)

    compose = tmp_path / "compose.py"
    expected_credentials = (
        f"{USERNAME}\0{PASSWORD}\0".encode()
        if scenario == "configured" else b""
    )
    compose.write_text(
        "import pathlib, sys\n"
        "args = sys.argv[1:]\n"
        "if 'x-login' in args:\n"
        "    assert args[-2:] == ['--config', 'custom config.toml']\n"
        "    pathlib.Path('requested').touch()\n"
        "    print('op://Vault/X/user', 'op://Vault/X/password', sep='\\n')\n"
        "    sys.exit(0)\n"
        "assert args[:6] == ['run', '--rm', '-T', 'dank', 'scrape', "
        "'--headless']\n"
        "assert ('--x-credentials-stdio' in args) is "
        f"{scenario != 'missing'!r}\n"
        "print('collecting', flush=True)\n"
        f"if {scenario!r} in ('configured', 'denied'):\n"
        "    print('DANK_REQUEST_X_CREDENTIALS', flush=True)\n"
        f"    assert sys.stdin.buffer.read() == {expected_credentials!r}\n"
        "    print('resumed', flush=True)\n"
        f"sys.exit(7 if {scenario!r} == 'denied' else 0)\n",
    )
    command = [
        "/bin/bash", str(ROOT / "src/dank/run_x_login.sh"), "--scrape",
        sys.executable, str(compose), "--", "--config=custom config.toml",
    ]
    process = Popen(
        command, cwd=tmp_path, stdout=PIPE, stderr=PIPE, text=True,
        env={**os.environ, "PATH": str(bin_dir), "TMPDIR": str(tmp_path)},
    )
    stdout, stderr = process.communicate(timeout=10)
    requested = scenario in {"configured", "denied"}

    assert process.returncode == (7 if scenario == "denied" else 0), stderr
    assert stdout == ("collecting\nresumed\n" if requested else "collecting\n")
    assert (tmp_path / "requested").exists() == requested
    assert PASSWORD not in stderr and USERNAME not in stderr
    assert not (tmp_path / "should-not-exist").exists()
    assert not list(tmp_path.glob("dank-x-credentials.*"))
