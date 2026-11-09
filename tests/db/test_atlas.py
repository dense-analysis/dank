import asyncio
import json
import pathlib
import sys

import pytest

from dank.config import PostgresSettings
from dank.db import atlas, install_atlas
from dank.db.atlas import SchemaState, SchemaStatus, parse_status
from dank.web.startup import prepare_postgres

ROOT = pathlib.Path(__file__).resolve().parents[2]
SECRET = "p@ss/w:rd %?#&"


def _status_output(applied: list[str], available: list[str]) -> str:
    # The shape Atlas v1.3.0 prints, including null for an empty list.
    return json.dumps({
        "Available": [
            {"Name": f"{version}_x.sql", "Version": version}
            for version in available
        ] or None,
        "Applied": [
            {"Version": version, "Description": "x", "Type": "applied"}
            for version in applied
        ] or None,
        "Current": applied[-1] if applied else "No migration applied yet",
        "Next": "Already at latest version",
        "Status": "OK",
    })


def test_status_reads_applied_and_available_versions() -> None:
    assert parse_status(_status_output(["1"], ["1", "2"])) == SchemaStatus(
        frozenset({"1"}),
        frozenset({"1", "2"}),
    )


def test_status_handles_nothing_applied_or_available() -> None:
    assert parse_status(_status_output([], [])) == SchemaStatus(
        frozenset(),
        frozenset(),
    )


@pytest.mark.parametrize(
    ("applied", "available", "state"),
    [
        (["1", "2"], ["1", "2"], SchemaState.AT_HEAD),
        ([], [], SchemaState.AT_HEAD),
        (["1"], ["1", "2"], SchemaState.BEHIND),
        ([], ["1"], SchemaState.BEHIND),
        (["1", "2"], ["1"], SchemaState.AHEAD),
        # A branch with its own migration is still ahead of this code.
        (["1", "3"], ["1", "2"], SchemaState.AHEAD),
    ],
)
def test_state_compares_versions_rather_than_atlas_status(
    applied: list[str],
    available: list[str],
    state: SchemaState,
) -> None:
    # Atlas calls all of these "OK" or "PENDING"; "OK" covers being ahead.
    assert parse_status(_status_output(applied, available)).state is state


def test_url_encodes_credentials_and_negotiates_tls() -> None:
    settings = PostgresSettings(
        host="db.example", port=5433, database="d b", user="u/1",
    )

    assert atlas.atlas_url(settings, SECRET) == (
        "postgres://u%2F1:p%40ss%2Fw%3Ard%20%25%3F%23%26@db.example:5433/d%20b"
        "?sslmode=prefer&search_path=public"
    )


def test_url_brackets_an_ipv6_host() -> None:
    assert "@[::1]:5432/" in atlas.atlas_url(
        PostgresSettings(host="::1"),
        "x",
    )


def test_child_environment_turns_off_telemetry_and_update_checks() -> None:
    environment = atlas.child_environment("the-url", "the-dev-url")

    assert environment["ATLAS_NO_ANON_TELEMETRY"] == "true"
    assert environment["ATLAS_NO_UPDATE_NOTIFIER"] == "true"
    assert environment["DANK_ATLAS_URL"] == "the-url"
    assert environment["DANK_ATLAS_DEV_URL"] == "the-dev-url"


def test_scrub_removes_the_password_in_both_forms() -> None:
    encoded = atlas.quote(SECRET, safe="")
    text = f"failed for {SECRET} at postgres://u:{encoded}@h"

    assert atlas.scrub(text, SECRET) == "failed for *** at postgres://u:***@h"


def test_scrub_leaves_text_alone_without_a_password() -> None:
    assert atlas.scrub("text", "") == "text"


def test_failure_keeps_the_error_and_drops_suggestions() -> None:
    output = "Error: it broke\n\nNeed help with this error?\n  Install things"

    assert atlas._failure(output, "") == (  # pyright: ignore[reportPrivateUsage]
        "Error: it broke"
    )


async def test_missing_binary_explains_the_way_out(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    password_file = tmp_path / "password"
    password_file.write_text("x")
    monkeypatch.setattr(atlas, "find_atlas", lambda: None)

    with pytest.raises(atlas.SchemaError) as caught:
        await atlas.upgrade(PostgresSettings(password_file=password_file))

    assert "make atlas" in str(caught.value)


SHARED = "/home/someone/.local/bin/atlas"


class FakeProcess:
    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout.encode()
        self.returncode = returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        return self.stdout, b""

    async def wait(self) -> int:
        return self.returncode


def fake_atlas(
    monkeypatch: pytest.MonkeyPatch, answer: FakeProcess,
) -> list[tuple[str, ...]]:
    """Answer `atlas version` with `answer`, and record any other command.

    The test suite refuses real subprocesses outside the Postgres lane.
    """
    ran: list[tuple[str, ...]] = []

    async def start(program: str, *args: str, **_: object) -> FakeProcess:
        assert program == SHARED

        if args == ("version",):
            return answer

        ran.append(args)

        return FakeProcess()

    monkeypatch.setattr(atlas, "find_atlas", lambda: SHARED)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", start)

    return ran


def reports(version: str) -> FakeProcess:
    # What the pinned download prints.
    return FakeProcess(
        f"atlas version {version}\n"
        f"https://github.com/ariga/atlas/releases/tag/{version}\n",
    )


async def test_the_pinned_version_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    ran = fake_atlas(monkeypatch, reports(install_atlas.VERSION))

    await atlas.validate_migrations(root=tmp_path)

    assert ran == [("migrate", "validate", "--env", "dank")]


async def test_another_version_is_refused_before_it_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    ran = fake_atlas(monkeypatch, reports("v1.2.0"))

    with pytest.raises(atlas.SchemaError) as caught:
        await atlas.validate_migrations(root=tmp_path)

    assert str(caught.value) == (
        f"{SHARED} is Atlas v1.2.0, but DANK needs {install_atlas.VERSION}. "
        "Run `make atlas`; see docs/setup.md#atlas."
    )
    assert ran == []


@pytest.mark.parametrize("answer", [
    FakeProcess("something else entirely\n"),
    FakeProcess(returncode=1),
])
async def test_an_atlas_that_won_t_say_its_version_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    answer: FakeProcess,
) -> None:
    ran = fake_atlas(monkeypatch, answer)

    with pytest.raises(atlas.SchemaError) as caught:
        await atlas.validate_migrations(root=tmp_path)

    assert str(caught.value) == (
        f"{SHARED} didn't say which Atlas version it is, and DANK needs "
        f"{install_atlas.VERSION}. Run `make atlas`; see docs/setup.md#atlas."
    )
    assert ran == []


async def test_an_interactive_command_checks_the_version_too(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    ran = fake_atlas(monkeypatch, reports("v1.2.0"))

    with pytest.raises(atlas.SchemaError, match=r"v1\.2\.0"):
        await atlas.run_interactive("migrate", "hash", root=tmp_path)

    assert ran == []


def executable(path: pathlib.Path) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n")
    path.chmod(0o755)

    return path


def test_a_shared_install_on_path_serves_a_checkout_without_its_own(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    shared = executable(tmp_path / "shared" / "atlas")
    monkeypatch.setattr(sys, "prefix", str(tmp_path / "venv"))
    monkeypatch.setenv("PATH", str(shared.parent))

    assert atlas.find_atlas() == str(shared)


def test_a_checkout_s_own_copy_comes_before_the_shared_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    own = executable(tmp_path / "venv" / "bin" / "atlas")
    shared = executable(tmp_path / "shared" / "atlas")
    monkeypatch.setattr(sys, "prefix", str(tmp_path / "venv"))
    monkeypatch.setenv("PATH", str(shared.parent))

    assert atlas.find_atlas() == str(own)


async def test_startup_failure_explains_the_ways_out(
    tmp_path: pathlib.Path,
) -> None:
    missing = tmp_path / "password"

    with pytest.raises(atlas.SchemaError) as caught:
        await prepare_postgres(PostgresSettings(password_file=missing))

    assert "postgres.password_file" in str(caught.value)
    assert "docs/setup.md#postgres-on-your-own-server" in str(caught.value)
    assert "enabled = false" in str(caught.value)


async def test_startup_skips_a_disabled_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Neither Atlas nor a password file exists, so running would raise.
    monkeypatch.setattr(atlas, "find_atlas", lambda: None)

    await prepare_postgres(PostgresSettings(
        enabled=False,
        password_file=pathlib.Path("/nonexistent/password"),
    ))


def test_dev_database_matches_the_compose_postgres() -> None:
    compose = (ROOT / "compose.yaml").read_text()
    makefile = (ROOT / "Makefile").read_text()
    major = atlas.DOCKER_DEV_URL.split("/")[3]

    assert f"image: postgres:{major}\n" in compose
    assert f"POSTGRES_IMAGE := postgres:{major}\n" in makefile


def test_every_migration_is_in_the_checksum_file() -> None:
    migrations = atlas.DB_DIR / "migrations"
    checksums = (migrations / "atlas.sum").read_text()
    files = sorted(path.name for path in migrations.glob("*.sql"))

    assert files
    assert all(name in checksums for name in files)
