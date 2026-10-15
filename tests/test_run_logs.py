from __future__ import annotations

import json
import os
import pathlib
import re
import shlex
import shutil
import signal
import sys
import time
from subprocess import PIPE, CompletedProcess, Popen, TimeoutExpired

import pytest

# Bind Popen before the session network guard; only local fixtures run here.
ROOT = pathlib.Path(__file__).resolve().parents[1]
WRAPPER = ROOT / "src/dank/run_logged.sh"


@pytest.fixture
def workspace(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "checkout with spaces"
    (path / "src/dank").mkdir(parents=True)
    shutil.copyfile(ROOT / "Makefile", path / "Makefile")
    shutil.copyfile(WRAPPER, path / "src/dank/run_logged.sh")
    (path / "config.toml").touch()
    (path / "launcher.py").write_text(
        'import json, sys\n'
        'print(json.dumps(sys.argv[1:]), flush=True)\n'
        'print("regular progress", flush=True)\n'
        'print("WARNING source unavailable", file=sys.stderr, flush=True)\n'
        'if "--burst" in sys.argv:\n'
        '    for i in range(2000):\n'
        '        print(f"stdout {i}")\n'
        '        print(f"stderr {i}", file=sys.stderr)\n'
        'if "--wait" in sys.argv:\n'
        '    print("ready", flush=True)\n'
        '    sys.stdin.readline()\n'
        'sys.exit(7 if "--fail" in sys.argv else 0)\n',
    )

    return path


def _capture(process: Popen[str]) -> CompletedProcess[str]:
    try:
        stdout, stderr = process.communicate(timeout=10)
    except TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise

    return CompletedProcess(process.args, process.returncode, stdout, stderr)


def _start(command: list[str], cwd: pathlib.Path) -> Popen[str]:
    return Popen(
        command, cwd=cwd, stdin=PIPE, stdout=PIPE, stderr=PIPE,
        text=True, start_new_session=True,
    )


def _command(workspace: pathlib.Path, *args: str) -> list[str]:
    return [
        "bash", str(WRAPPER), "scrape", "native", sys.executable,
        str(workspace / "launcher.py"), *args,
    ]


def _log(workspace: pathlib.Path, action: str = "scrape") -> pathlib.Path:
    paths = list((workspace / "logs" / action).glob("*.log"))
    assert len(paths) == 1

    return paths[0]


@pytest.mark.parametrize("action", ["scrape", "process"])
@pytest.mark.parametrize("mode", ["native", "docker"])
def test_make_archives_both_streams_and_preserves_quoted_arguments(
    workspace: pathlib.Path, action: str, mode: str,
) -> None:
    launcher = shlex.join([sys.executable, str(workspace / "launcher.py")])
    args = ["--domains", r"^example\.com$", "--label", "a b 'c'",
            "--literal", "$(touch should-not-exist)"]
    result = _capture(_start([
        "make", action, f"MODE={mode}", f"UV={launcher}",
        f"COMPOSE={launcher}", f"ARGS={shlex.join(args)}",
    ], workspace))

    assert result.returncode == 0, result.stderr
    saved = _log(workspace, action)
    text = saved.read_text()
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z-\d+\.log", saved.name,
    )
    prefix = ["run"]

    if mode == "docker":
        prefix += ["--rm", "-T", "dank"]

    prefix += [action]

    if mode == "docker" and action == "scrape":
        prefix += ["--headless"]

    assert json.dumps(prefix + args) in result.stdout
    assert json.dumps(prefix + args) in text
    assert "regular progress" in result.stdout
    assert "regular progress" not in result.stderr
    assert "WARNING source unavailable" in result.stderr
    assert "WARNING source unavailable" not in result.stdout
    assert "regular progress" in text
    assert "WARNING source unavailable" in text
    assert f"Mode: {mode}" in text
    assert "Checkout revision: unknown" in text
    assert f"Working directory: {workspace}" in text
    assert "Command:" in text
    assert "Finished (UTC):" in text
    assert "Duration (seconds):" in text
    assert text.endswith("Exit status: 0\nLog capture status: 0\n")
    assert saved.stat().st_mode & 0o777 == 0o600
    assert not (workspace / "should-not-exist").exists()


def test_nonzero_command_exit_is_recorded_and_still_fails_make(
    workspace: pathlib.Path,
) -> None:
    result = _capture(_start(_command(workspace, "--fail"), workspace))
    assert result.returncode == 7
    assert "Exit status: 7" in _log(workspace).read_text()
    launcher = shlex.join([sys.executable, str(workspace / "launcher.py")])
    result = _capture(_start([
        "make", "process", "MODE=native", f"UV={launcher}", "ARGS=--fail",
    ], workspace))
    assert result.returncode != 0
    assert "Exit status: 7" in _log(workspace, "process").read_text()


def test_capture_drains_both_streams_before_final_status(
    workspace: pathlib.Path,
) -> None:
    result = _capture(_start(_command(workspace, "--burst"), workspace))
    assert result.returncode == 0
    text = _log(workspace).read_text()

    for index in range(2000):
        assert f"stdout {index}\n" in text
        assert f"stderr {index}\n" in text

    assert text.index("stderr 1999") < text.index("Finished (UTC):")
    assert text.index("stdout 1999") < text.index("Finished (UTC):")
    assert text.endswith("Exit status: 0\nLog capture status: 0\n")


def test_concurrent_runs_keep_separate_logs(workspace: pathlib.Path) -> None:
    first = _start(_command(workspace, "first"), workspace)
    second = _start(_command(workspace, "second"), workspace)
    assert _capture(first).returncode == 0
    assert _capture(second).returncode == 0
    logs = [
        path.read_text() for path in (workspace / "logs/scrape").glob("*.log")
    ]
    assert len(logs) == 2
    assert sum('["first"]' in text for text in logs) == 1
    assert sum('["second"]' in text for text in logs) == 1
    assert all("Exit status: 0" in text for text in logs)


@pytest.mark.parametrize("interrupt", [signal.SIGINT, signal.SIGTERM])
def test_output_is_saved_while_running_and_interruptions_are_recorded(
    workspace: pathlib.Path, interrupt: signal.Signals,
) -> None:
    process = _start(_command(workspace, "--wait"), workspace)

    try:
        deadline = time.monotonic() + 5
        saved = None

        while time.monotonic() < deadline:
            paths = list((workspace / "logs/scrape").glob("*.log"))

            if paths and "\nready\n" in paths[0].read_text():
                saved = paths[0]
                break

            time.sleep(0.01)

        assert saved is not None, "Output was not archived during the run"
        assert process.poll() is None
        assert "Finished (UTC):" not in saved.read_text()
        os.killpg(process.pid, interrupt)
        result = _capture(process)
        assert result.returncode == 128 + interrupt
        text = saved.read_text()
        assert "regular progress" in text
        assert f"Exit status: {128 + interrupt}" in text
        assert text.count("Finished (UTC):") == 1
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()


def test_startup_failure_is_archived(workspace: pathlib.Path) -> None:
    result = _capture(_start([
        "bash", str(WRAPPER), "process", "native", "./missing-launcher",
    ], workspace))
    assert result.returncode != 0
    text = _log(workspace, "process").read_text()
    assert "No such file or directory" in text
    assert f"Exit status: {result.returncode}" in text


def test_log_creation_failure_does_not_start_the_command(
    workspace: pathlib.Path,
) -> None:
    (workspace / "logs").write_text("Not a directory")
    result = _capture(_start(_command(workspace), workspace))
    assert result.returncode != 0
    assert "regular progress" not in result.stdout


def test_make_dry_run_creates_no_logs(workspace: pathlib.Path) -> None:
    result = _capture(_start(["make", "-n", "scrape"], workspace))
    assert result.returncode == 0
    assert not (workspace / "logs").exists()
