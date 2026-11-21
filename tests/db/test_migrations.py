"""Migration decisions and Atlas command handling with an in-memory runner."""

from __future__ import annotations

import asyncio
import json
import pathlib
from collections import deque
from typing import NamedTuple, cast
from unittest.mock import MagicMock

import pytest

from dank.api import deps
from dank.api.lifespan import Resources
from dank.api.refresh.manager import RefreshManager
from dank.api.youtube.collector import CollectionGate
from dank.config import PostgresSettings, Settings
from dank.db import atlas, install_atlas
from dank.db.atlas import SchemaAheadError, SchemaError, SchemaState
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_source_icons import SourceIconCache
from dank.web.startup import prepare_postgres

ATLAS = "/fixture/atlas"
DEV_URL = "postgres://database.test/dev"
SECRET = "fixture/pass%word"
BASELINE = "20261104001556"
HEAD = "20261109193052"
AHEAD = "20991231000001"


class Reply(NamedTuple):
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0

    async def communicate(self) -> tuple[bytes, bytes]:
        return self.stdout.encode(), self.stderr.encode()


class Command(NamedTuple):
    args: tuple[str, ...]
    cwd: pathlib.Path
    env: dict[str, str]


class AtlasFixture(NamedTuple):
    replies: deque[Reply]
    commands: list[Command]


def status(*applied: str) -> Reply:
    return Reply(json.dumps({
        "Applied": [{"Version": version} for version in applied],
        "Available": [{"Version": BASELINE}, {"Version": HEAD}],
    }))


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> AtlasFixture:
    fixture = AtlasFixture(deque(), [])

    async def start(
        program: str, *args: str, cwd: pathlib.Path = atlas.DB_DIR,
        env: dict[str, str], **_: object,
    ) -> Reply:
        assert program == ATLAS

        if args == ("version",):
            return Reply(f"atlas version {install_atlas.VERSION}\n")

        fixture.commands.append(Command(args, cwd, env))

        return fixture.replies.popleft()

    monkeypatch.setattr(atlas, "find_atlas", lambda: ATLAS)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", start)

    return fixture


@pytest.fixture
def postgres(memory_path: pathlib.Path) -> PostgresSettings:
    password = memory_path / "password"
    password.write_text(SECRET)

    return PostgresSettings(password_file=password)


@pytest.mark.parametrize("applied", [(), (BASELINE,), (BASELINE, HEAD)])
async def test_upgrade_applies_with_the_configured_lock_timeout(
    runner: AtlasFixture, postgres: PostgresSettings, applied: tuple[str, ...],
) -> None:
    runner.replies.extend([status(*applied), Reply(), status(BASELINE, HEAD)])
    await atlas.upgrade(postgres)

    [read, apply] = runner.commands
    assert read.args[:2] == ("migrate", "status")
    assert apply.args == (
        "migrate", "apply", "--env", "dank", "--lock-timeout", "5m",
    )
    assert SECRET not in repr(apply.args)
    assert "postgres://" not in repr(apply.args)
    assert apply.env["DANK_ATLAS_URL"] == atlas.atlas_url(postgres, SECRET)
    assert (await atlas.schema_status(postgres)).state is SchemaState.AT_HEAD


@pytest.mark.parametrize("startup", [False, True])
async def test_code_refuses_an_ahead_database(
    runner: AtlasFixture, postgres: PostgresSettings, *, startup: bool,
) -> None:
    runner.replies.append(status(BASELINE, HEAD, AHEAD))
    operation = prepare_postgres if startup else atlas.upgrade

    with pytest.raises(SchemaAheadError, match=f"doesn't know about.*{AHEAD}"):
        await operation(postgres)

    [command] = runner.commands
    assert command.args[:2] == ("migrate", "status")


@pytest.mark.parametrize("applied,error", [
    ((), "behind this code"),
    ((BASELINE, HEAD), ""),
    ((BASELINE, HEAD, AHEAD), "doesn't know about"),
])
async def test_cli_check_never_attempts_to_migrate(
    runner: AtlasFixture, postgres: PostgresSettings,
    applied: tuple[str, ...], error: str,
) -> None:
    runner.replies.append(status(*applied))

    if error:
        with pytest.raises(SchemaError, match=error):
            await atlas.require_head(postgres)
    else:
        await atlas.require_head(postgres)

    [command] = runner.commands
    assert command.args[:2] == ("migrate", "status")


async def test_failed_upgrade_reports_error_and_can_be_retried(
    runner: AtlasFixture, postgres: PostgresSettings,
) -> None:
    runner.replies.extend([
        status(BASELINE),
        Reply(stderr="Error: division by zero", returncode=1),
        status(BASELINE), Reply(),
    ])

    with pytest.raises(SchemaError, match="division by zero"):
        await atlas.upgrade(postgres)

    await atlas.upgrade(postgres)

    assert [item.args[1] for item in runner.commands] == [
        "status", "apply", "status", "apply",
    ]


async def test_authentication_error_removes_the_password_from_output(
    runner: AtlasFixture, postgres: PostgresSettings,
) -> None:
    runner.replies.append(Reply(
        stderr=f"Error: password authentication failed for {SECRET}",
        returncode=1,
    ))

    with pytest.raises(
        SchemaError, match="password authentication failed",
    ) as raised:
        await atlas.upgrade(postgres)

    assert SECRET not in str(raised.value)


async def test_startup_upgrades_an_enabled_postgres(
    runner: AtlasFixture, postgres: PostgresSettings,
) -> None:
    runner.replies.extend([status(), Reply()])
    await prepare_postgres(postgres)

    assert [item.args[1] for item in runner.commands] == ["status", "apply"]


@pytest.mark.parametrize("valid", [False, True])
async def test_validation_passes_the_development_url_and_reports_errors(
    runner: AtlasFixture, *, valid: bool,
) -> None:
    error = Reply(stderr="Error: checksum mismatch", returncode=1)
    runner.replies.append(Reply() if valid else error)

    if valid:
        await atlas.validate_migrations(DEV_URL)
    else:
        with pytest.raises(SchemaError, match="checksum mismatch"):
            await atlas.validate_migrations(DEV_URL)

    [command] = runner.commands
    assert command.args[:2] == ("migrate", "validate")
    assert command.env["DANK_ATLAS_DEV_URL"] == DEV_URL


@pytest.mark.parametrize("changed", [False, True])
async def test_drift_uses_a_disposable_copy(
    monkeypatch: pytest.MonkeyPatch, memory_path: pathlib.Path,
    *, changed: bool,
) -> None:
    root = memory_path / "db"
    migrations = root / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "baseline.sql").write_text("CREATE TABLE a (id int);")
    sql = "CREATE TABLE drifted (id int);"

    async def atlas_diff(*args: str, **kwargs: object) -> str:
        assert args[:3] == ("migrate", "diff", "drift")
        assert kwargs["root"] == root
        assert kwargs["dev_url"] == DEV_URL
        copy = pathlib.Path(args[-1].removeprefix("file://"))
        assert copy != migrations
        assert (copy / "baseline.sql").exists()

        if changed:
            (copy / "drift.sql").write_text(sql)

        return ""

    monkeypatch.setattr(atlas, "_run", atlas_diff)

    drift = await atlas.find_drift(DEV_URL, root=root)
    assert drift == (sql if changed else "")
    assert [path.name for path in migrations.iterdir()] == ["baseline.sql"]


async def test_fastapi_dependency_opens_and_closes_a_session() -> None:
    sessions = MagicMock()
    resources = Resources(
        settings=cast("Settings", None),
        clickhouse=cast("ClickHouseClient", None),
        postgres=sessions,
        refresh=cast("RefreshManager", None),
        source_icons=cast("SourceIconCache", None),
        assets_dir=pathlib.Path(),
        metadata_gate=cast("CollectionGate", None),
        transcript_gate=cast("CollectionGate", None),
    )

    assert [session async for session in deps.postgres_session(resources)] == [
        sessions.return_value.__aenter__.return_value,
    ]
    sessions.return_value.__aexit__.assert_awaited_once()
