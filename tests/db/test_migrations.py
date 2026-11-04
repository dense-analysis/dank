"""Migrations against a real Postgres, with the real Atlas binary."""

from __future__ import annotations

import asyncio
import pathlib
import shutil
import stat
from typing import cast

import pytest
from sqlalchemy import text

from dank.api import deps
from dank.api.lifespan import Resources
from dank.config import PostgresSettings, Settings
from dank.db import atlas
from dank.db.atlas import (
    SchemaAheadError,
    SchemaError,
    SchemaState,
    find_drift,
    require_head,
    schema_status,
    upgrade,
    validate_migrations,
)
from dank.db.engine import async_engine, async_sessions, sync_engine
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_refresh import RefreshManager
from dank.web.reader_source_icons import SourceIconCache
from dank.web.startup import prepare_postgres

pytestmark = pytest.mark.postgres

ENV = ("--env", atlas.ENV)
# Longer than the 10 s lock timeout Atlas uses by default.
SLOW_MIGRATION_SECONDS = 11


@pytest.fixture
def root(tmp_path: pathlib.Path) -> pathlib.Path:
    """A copy of the schema directory that a test can add migrations to."""
    copy = tmp_path / "db with spaces"
    copy.mkdir()
    shutil.copy(atlas.DB_DIR / "atlas.hcl", copy)
    shutil.copytree(atlas.DB_DIR / "migrations", copy / "migrations")
    shutil.copytree(
        atlas.DB_DIR / "models",
        copy / "models",
        ignore=shutil.ignore_patterns("__pycache__"),
    )

    return copy


async def _add_migration(
    root: pathlib.Path,
    version: str,
    sql: str,
) -> None:
    (root / "migrations" / f"{version}_test.sql").write_text(sql)
    await atlas.run_interactive("migrate", "hash", *ENV, root=root)


def _table_exists(settings: PostgresSettings, table: str) -> bool:
    engine = sync_engine(settings)

    with engine.connect() as connection:
        exists = connection.scalar(
            text("SELECT to_regclass(:table) IS NOT NULL"),
            {"table": table},
        )

    engine.dispose()

    return bool(exists)


async def test_upgrade_from_empty_reaches_head(
    fresh_postgres: PostgresSettings,
) -> None:
    before = await schema_status(fresh_postgres)
    assert before.state is SchemaState.BEHIND
    assert not before.applied

    await upgrade(fresh_postgres)

    after = await schema_status(fresh_postgres)
    assert after.state is SchemaState.AT_HEAD
    assert after.applied == after.available


async def test_upgrade_at_head_changes_nothing(
    fresh_postgres: PostgresSettings,
) -> None:
    await upgrade(fresh_postgres)
    status = await schema_status(fresh_postgres)

    await upgrade(fresh_postgres)

    assert await schema_status(fresh_postgres) == status


async def test_each_revision_applies_on_top_of_the_previous_head(
    fresh_postgres: PostgresSettings,
    root: pathlib.Path,
) -> None:
    await _add_migration(root, "20991231000001", "CREATE TABLE a (id int);")
    await _add_migration(
        root, "20991231000002", "ALTER TABLE a ADD COLUMN b int;",
    )
    versions = sorted(
        (await schema_status(fresh_postgres, root=root)).available,
    )

    for applied, version in enumerate(versions, start=1):
        await atlas._run_on_database(  # pyright: ignore[reportPrivateUsage]
            fresh_postgres, "migrate", "apply", "1", *ENV, root=root,
        )
        status = await schema_status(fresh_postgres, root=root)
        assert status.applied == frozenset(versions[:applied])
        assert version in status.applied

    final = await schema_status(fresh_postgres, root=root)
    assert final.state is SchemaState.AT_HEAD


async def test_real_migrations_are_valid(postgres_dev_url: str) -> None:
    await validate_migrations(postgres_dev_url)


async def test_edited_migration_fails_validation(
    postgres_dev_url: str,
    root: pathlib.Path,
) -> None:
    migration = next((root / "migrations").glob("*.sql"))
    migration.write_text("CREATE TABLE edited_later (id int);\n")

    with pytest.raises(SchemaError):
        await validate_migrations(postgres_dev_url, root=root)


async def test_real_models_match_the_migrations(postgres_dev_url: str) -> None:
    assert await find_drift(postgres_dev_url) == ""


async def test_a_model_without_a_migration_is_drift(
    postgres_dev_url: str,
    root: pathlib.Path,
) -> None:
    (root / "models" / "drifted.py").write_text(
        "from sqlalchemy.orm import Mapped, mapped_column\n"
        "from dank.db.models import Base\n"
        "class Drifted(Base):\n"
        "    __tablename__ = 'drifted'\n"
        "    id: Mapped[int] = mapped_column(primary_key=True)\n",
    )
    before = sorted(path.name for path in (root / "migrations").iterdir())

    drift = await find_drift(postgres_dev_url, root=root)

    assert '"drifted"' in drift
    # The check must not leave a migration behind.
    assert sorted(path.name for path in (root / "migrations").iterdir()) == (
        before
    )


async def test_two_processes_migrate_once_during_a_slow_migration(
    fresh_postgres: PostgresSettings,
    root: pathlib.Path,
) -> None:
    await _add_migration(
        root,
        "20991231000001",
        "CREATE TABLE ran (at timestamptz DEFAULT now());\n"
        "INSERT INTO ran DEFAULT VALUES;\n"
        f"SELECT pg_sleep({SLOW_MIGRATION_SECONDS});\n",
    )

    await asyncio.gather(
        upgrade(fresh_postgres, root=root),
        upgrade(fresh_postgres, root=root),
    )

    engine = sync_engine(fresh_postgres)

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM ran")) == 1

    engine.dispose()
    status = await schema_status(fresh_postgres, root=root)
    assert status.state is SchemaState.AT_HEAD


async def test_database_ahead_of_the_code_is_refused(
    fresh_postgres: PostgresSettings,
    root: pathlib.Path,
) -> None:
    version = "20991231000001"
    await _add_migration(root, version, "CREATE TABLE newer (id int);")
    await upgrade(fresh_postgres, root=root)

    with pytest.raises(SchemaError, match=f"doesn't know about.*{version}"):
        await upgrade(fresh_postgres)

    with pytest.raises(SchemaError, match=f"doesn't know about.*{version}"):
        await require_head(fresh_postgres)

    # Refusing changed nothing.
    assert _table_exists(fresh_postgres, "newer")
    status = await schema_status(fresh_postgres)
    assert status.state is SchemaState.AHEAD


async def test_cli_check_passes_only_at_head_and_never_migrates(
    fresh_postgres: PostgresSettings,
) -> None:
    with pytest.raises(SchemaError, match="behind this code"):
        await require_head(fresh_postgres)

    status = await schema_status(fresh_postgres)
    assert not status.applied

    await upgrade(fresh_postgres)

    await require_head(fresh_postgres)


async def test_failed_migration_rolls_back_and_can_be_retried(
    fresh_postgres: PostgresSettings,
    root: pathlib.Path,
) -> None:
    await upgrade(fresh_postgres, root=root)
    await _add_migration(
        root,
        "20991231000001",
        "CREATE TABLE half (id int);\nSELECT 1/0;\n",
    )

    with pytest.raises(SchemaError, match="division by zero"):
        await upgrade(fresh_postgres, root=root)

    assert not _table_exists(fresh_postgres, "half")

    await _add_migration(
        root,
        "20991231000001",
        "CREATE TABLE half (id int);\nSELECT 1;\n",
    )
    await upgrade(fresh_postgres, root=root)

    assert _table_exists(fresh_postgres, "half")


async def test_wrong_password_never_appears_in_the_error(
    fresh_postgres: PostgresSettings,
    tmp_path: pathlib.Path,
) -> None:
    secret = "not-the-right-password"
    wrong = tmp_path / "wrong"
    wrong.write_text(secret)

    with pytest.raises(SchemaError) as caught:
        await upgrade(fresh_postgres._replace(password_file=wrong))

    assert secret not in str(caught.value)
    assert "password authentication failed" in str(caught.value)


async def test_password_stays_out_of_the_command_line(
    fresh_postgres: PostgresSettings,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = tmp_path / "arguments"
    stub = tmp_path / "atlas"
    stub.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$@" >> "{recorded}"\necho "{{}}"\n',
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setattr(atlas, "find_atlas", lambda: str(stub))

    await upgrade(fresh_postgres)

    arguments = recorded.read_text()
    assert "migrate" in arguments
    assert fresh_postgres.password_file.read_text() not in arguments
    assert "postgres://" not in arguments


async def test_startup_upgrades_an_enabled_postgres(
    fresh_postgres: PostgresSettings,
) -> None:
    await prepare_postgres(fresh_postgres)

    assert (await schema_status(fresh_postgres)).state is SchemaState.AT_HEAD


async def test_startup_refuses_a_database_ahead_of_the_code(
    fresh_postgres: PostgresSettings,
    root: pathlib.Path,
) -> None:
    await _add_migration(root, "20991231000001", "CREATE TABLE n (id int);")
    await upgrade(fresh_postgres, root=root)

    with pytest.raises(SchemaAheadError, match="doesn't know about") as caught:
        await prepare_postgres(fresh_postgres)

    # Turning Postgres off isn't the answer to old code on a newer database.
    assert "enabled = false" not in str(caught.value)


async def test_fastapi_dependency_opens_a_session(
    postgres_settings: PostgresSettings,
) -> None:
    engine = async_engine(postgres_settings)
    resources = Resources(
        settings=cast("Settings", None),
        clickhouse=cast("ClickHouseClient", None),
        postgres=async_sessions(engine),
        refresh=cast("RefreshManager", None),
        source_icons=cast("SourceIconCache", None),
        assets_dir=pathlib.Path(),
        page_size=1,
    )

    async for session in deps.postgres_session(resources):
        assert await session.scalar(text("SELECT 1")) == 1

    await engine.dispose()
