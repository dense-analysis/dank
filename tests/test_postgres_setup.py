from __future__ import annotations

import json
import pathlib
import re
import shlex
import shutil
import stat
import sys
from subprocess import PIPE, CompletedProcess, Popen

import pytest

# Bind Popen before the session network guard; only local fixtures run here.
ROOT = pathlib.Path(__file__).resolve().parents[1]
FAKE_COMPOSE = """\
import json
import pathlib
import sys

here = pathlib.Path(sys.argv[0]).parent
(here / "compose-args.json").write_text(json.dumps(sys.argv[1:]))

if "pg_restore" in sys.argv:
    (here / "restored").write_bytes(sys.stdin.buffer.read())
else:
    sys.stdout.buffer.write(b"dump bytes")
    sys.stdout.flush()

sys.exit(1 if (here / "fail").exists() else 0)
"""


@pytest.fixture
def checkout(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "checkout with spaces"
    path.mkdir()

    for name in ("Makefile", "config.example.toml",
                 "config.native.example.toml"):
        shutil.copyfile(ROOT / name, path / name)

    (path / "fake_compose.py").write_text(FAKE_COMPOSE)

    return path


def _make(checkout: pathlib.Path, *args: str) -> CompletedProcess[str]:
    process = Popen(
        ["make", *args], cwd=checkout, stdout=PIPE, stderr=PIPE, text=True,
    )
    stdout, stderr = process.communicate(timeout=30)

    return CompletedProcess(process.args, process.returncode, stdout, stderr)


def _compose(checkout: pathlib.Path) -> str:
    launcher = shlex.join([sys.executable, str(checkout / "fake_compose.py")])

    return f"COMPOSE={launcher}"


def _compose_args(checkout: pathlib.Path) -> list[str]:
    return json.loads((checkout / "compose-args.json").read_text())


@pytest.mark.parametrize("mode", ["docker", "native"])
def test_config_creates_a_private_password_without_printing_it(
    checkout: pathlib.Path, mode: str,
) -> None:
    result = _make(checkout, "config", f"MODE={mode}")
    password_file = checkout / "postgres-password"
    password = password_file.read_text()

    assert result.returncode == 0
    assert re.fullmatch("[0-9a-f]{64}", password)
    assert stat.S_IMODE(password_file.stat().st_mode) == 0o600
    assert "postgres-password" in result.stdout
    assert password not in result.stdout + result.stderr


def test_config_adds_a_password_beside_existing_configuration(
    checkout: pathlib.Path,
) -> None:
    (checkout / "config.toml").write_text("sources = []\n")

    result = _make(checkout, "config")

    assert result.returncode == 0
    assert (checkout / "config.toml").read_text() == "sources = []\n"
    assert (checkout / "postgres-password").exists()


def test_config_keeps_an_existing_password(checkout: pathlib.Path) -> None:
    (checkout / "postgres-password").write_text("chosen by you\n")

    result = _make(checkout, "config")

    assert result.returncode == 0
    assert (checkout / "postgres-password").read_text() == "chosen by you\n"
    assert "postgres-password" not in result.stdout


def test_each_checkout_gets_its_own_password(checkout: pathlib.Path) -> None:
    password_file = checkout / "postgres-password"
    _make(checkout, "config")
    first = password_file.read_text()
    password_file.unlink()
    _make(checkout, "config")

    assert password_file.read_text() != first


def test_password_and_backups_are_ignored_by_git() -> None:
    ignored = (ROOT / ".gitignore").read_text().splitlines()

    assert "/postgres-password" in ignored
    assert "/backups/" in ignored


def test_dump_saves_a_private_custom_format_file(
    checkout: pathlib.Path,
) -> None:
    result = _make(checkout, "postgres-dump", _compose(checkout))
    dumps = list((checkout / "backups").iterdir())

    assert result.returncode == 0
    assert len(dumps) == 1
    assert re.fullmatch(r"postgres-\d{8}T\d{6}Z\.dump", dumps[0].name)
    assert dumps[0].read_bytes() == b"dump bytes"
    assert stat.S_IMODE(dumps[0].stat().st_mode) == 0o600
    assert dumps[0].name in result.stdout

    args = _compose_args(checkout)

    assert args[:4] == ["exec", "-T", "postgres", "pg_dump"]
    assert "--format=custom" in args


def test_failed_dump_cannot_pass_for_a_backup(checkout: pathlib.Path) -> None:
    (checkout / "fail").touch()

    result = _make(checkout, "postgres-dump", _compose(checkout))

    assert result.returncode != 0
    assert list((checkout / "backups").glob("*.dump")) == []


def test_restore_requires_a_file(checkout: pathlib.Path) -> None:
    result = _make(checkout, "postgres-restore", _compose(checkout))

    assert result.returncode != 0
    assert "FILE=" in result.stderr
    assert not (checkout / "compose-args.json").exists()


def test_restore_streams_the_file_in_one_transaction(
    checkout: pathlib.Path,
) -> None:
    (checkout / "my backup.dump").write_bytes(b"saved bytes")

    result = _make(
        checkout, "postgres-restore", "FILE=my backup.dump",
        _compose(checkout),
    )

    assert result.returncode == 0
    assert (checkout / "restored").read_bytes() == b"saved bytes"

    args = _compose_args(checkout)

    assert args[:4] == ["exec", "-T", "postgres", "pg_restore"]
    assert "--single-transaction" in args


def test_failed_restore_reports_failure(checkout: pathlib.Path) -> None:
    (checkout / "backup.dump").write_bytes(b"saved bytes")
    (checkout / "fail").touch()

    result = _make(
        checkout, "postgres-restore", "FILE=backup.dump", _compose(checkout),
    )

    assert result.returncode != 0


@pytest.mark.parametrize("target", ["postgres-dump", "postgres-restore"])
def test_native_mode_points_to_your_own_server_tools(
    checkout: pathlib.Path, target: str,
) -> None:
    result = _make(checkout, target, "MODE=native", _compose(checkout))

    assert result.returncode == 2
    assert "docs/setup.md" in result.stderr
    assert not (checkout / "compose-args.json").exists()
