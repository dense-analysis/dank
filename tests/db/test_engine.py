import pathlib
from unittest.mock import AsyncMock, MagicMock, Mock

import psycopg
import pytest
from sqlalchemy import text

from dank.config import ConfigError, PostgresSettings
from dank.db.engine import (
    async_engine,
    async_sessions,
    create_database,
    postgres_url,
    sync_engine,
    sync_sessions,
)
from dank.db.models import NAMING_CONVENTION, Base

SECRET = "correct-horse-battery-staple"


class ConnectionAttempt(Exception):
    """Stop the SQLAlchemy test at the driver boundary."""


@pytest.fixture
def postgres(memory_path: pathlib.Path) -> PostgresSettings:
    password_file = memory_path / "password"
    password_file.write_text(SECRET)

    return PostgresSettings(
        database="fixture_database", password_file=password_file,
    )


def test_url_never_holds_a_password(postgres: PostgresSettings) -> None:
    url = postgres_url(postgres)

    assert url.password is None
    assert SECRET not in url.render_as_string(hide_password=False)


def test_engines_can_exist_before_the_password_file() -> None:
    settings = PostgresSettings(password_file=pathlib.Path("/nonexistent"))

    assert sync_engine(settings).url.database == "dank"
    assert async_engine(settings).url.database == "dank"


def test_models_name_their_constraints() -> None:
    assert dict(Base.metadata.naming_convention) == NAMING_CONVENTION


def test_sync_engine_connects_with_the_password_file(
    postgres: PostgresSettings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = Mock(side_effect=ConnectionAttempt)
    monkeypatch.setattr(psycopg, "connect", connect)
    engine = sync_engine(postgres)

    try:
        with (
            sync_sessions(engine)() as session,
            pytest.raises(ConnectionAttempt),
        ):
            session.scalar(text("SELECT current_database()"))
    finally:
        engine.dispose()

    assert connect.call_args.kwargs["dbname"] == postgres.database
    assert connect.call_args.kwargs["password"] == SECRET


async def test_async_engine_connects_with_the_password_file(
    postgres: PostgresSettings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = AsyncMock(side_effect=ConnectionAttempt)
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect)
    engine = async_engine(postgres)

    try:
        async with async_sessions(engine)() as session:
            assert not session.sync_session.expire_on_commit

            with pytest.raises(ConnectionAttempt):
                await session.scalar(text("SELECT current_database()"))
    finally:
        await engine.dispose()

    assert connect.call_args.kwargs["dbname"] == postgres.database
    assert connect.call_args.kwargs["password"] == SECRET


def test_connecting_without_the_password_file_says_so(
    postgres: PostgresSettings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = Mock(side_effect=ConnectionAttempt)
    monkeypatch.setattr(psycopg, "connect", connect)
    missing = postgres.password_file.with_name("missing")
    engine = sync_engine(postgres._replace(password_file=missing))

    try:
        with pytest.raises(ConfigError, match=r"postgres\.password_file"):
            engine.connect()
    finally:
        engine.dispose()

    connect.assert_not_called()


@pytest.mark.parametrize(("exists", "racing", "created"), [
    (True, False, False), (False, False, True), (False, True, False),
])
def test_create_database_only_creates_a_missing_one(
    postgres: PostgresSettings, monkeypatch: pytest.MonkeyPatch,
    *, exists: bool, racing: bool, created: bool,
) -> None:
    response = Mock()
    response.fetchone.return_value = (1,) if exists else None
    context = MagicMock()
    manager = Mock(return_value=context)
    connection = context.__enter__.return_value
    connection.execute.side_effect = [
        response, psycopg.errors.DuplicateDatabase() if racing else None,
    ]
    monkeypatch.setattr(psycopg, "connect", manager)

    assert create_database(postgres) is created
    assert connection.execute.call_count == (1 if exists else 2)
    assert manager.call_args.kwargs["dbname"] == "postgres"
    assert manager.call_args.kwargs["autocommit"] is True

    if not exists:
        statement = connection.execute.call_args.args[0]
        assert statement.as_string() == 'CREATE DATABASE "fixture_database"'
