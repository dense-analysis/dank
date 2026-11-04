import pathlib

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

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


def test_url_never_holds_a_password(tmp_path: pathlib.Path) -> None:
    password_file = tmp_path / "password"
    password_file.write_text(SECRET)
    settings = PostgresSettings(password_file=password_file)

    url = postgres_url(settings)

    assert url.password is None
    assert SECRET not in url.render_as_string(hide_password=False)


def test_engines_can_exist_before_the_password_file() -> None:
    settings = PostgresSettings(password_file=pathlib.Path("/nonexistent"))

    assert sync_engine(settings).url.database == "dank"
    assert async_engine(settings).url.database == "dank"


def test_models_name_their_constraints() -> None:
    assert dict(Base.metadata.naming_convention) == NAMING_CONVENTION


@pytest.mark.postgres
def test_sync_engine_connects_with_the_password_file(
    fresh_postgres: PostgresSettings,
) -> None:
    engine = sync_engine(fresh_postgres)

    with sync_sessions(engine)() as session:
        assert session.scalar(text("SELECT current_database()")) == (
            fresh_postgres.database
        )

    engine.dispose()


@pytest.mark.postgres
async def test_async_engine_connects_with_the_password_file(
    fresh_postgres: PostgresSettings,
) -> None:
    engine = async_engine(fresh_postgres)

    async with async_sessions(engine)() as session:
        assert await session.scalar(text("SELECT current_database()")) == (
            fresh_postgres.database
        )

    await engine.dispose()


@pytest.mark.postgres
def test_connecting_without_the_password_file_says_so(
    fresh_postgres: PostgresSettings,
) -> None:
    missing = pathlib.Path("/nonexistent/password")
    engine = sync_engine(fresh_postgres._replace(password_file=missing))

    with pytest.raises(ConfigError, match=r"postgres\.password_file"):
        engine.connect()

    engine.dispose()


@pytest.mark.postgres
def test_create_database_only_creates_a_missing_one(
    fresh_postgres: PostgresSettings,
) -> None:
    assert create_database(fresh_postgres) is False

    settings = fresh_postgres._replace(
        database=f"{fresh_postgres.database}_new",
    )

    try:
        assert create_database(settings) is True
        assert create_database(settings) is False
    finally:
        from tests.postgres import drop_database

        drop_database(settings)


@pytest.mark.postgres
def test_session_work_rolls_back_between_tests_first(
    postgres_session: Session,
) -> None:
    postgres_session.execute(text("CREATE TABLE rollback_probe (id int)"))
    postgres_session.commit()

    assert postgres_session.scalar(
        text("SELECT to_regclass('rollback_probe') IS NOT NULL"),
    )


@pytest.mark.postgres
def test_session_work_rolls_back_between_tests_second(
    postgres_session: Session,
) -> None:
    assert not postgres_session.scalar(
        text("SELECT to_regclass('rollback_probe') IS NOT NULL"),
    )


@pytest.mark.postgres
async def test_async_session_work_rolls_back_between_tests_first(
    async_postgres_session: AsyncSession,
) -> None:
    session = async_postgres_session
    await session.execute(text("CREATE TABLE async_rollback_probe (id int)"))
    await session.commit()

    assert await session.scalar(
        text("SELECT to_regclass('async_rollback_probe') IS NOT NULL"),
    )


@pytest.mark.postgres
async def test_async_session_work_rolls_back_between_tests_second(
    async_postgres_session: AsyncSession,
) -> None:
    assert not await async_postgres_session.scalar(
        text("SELECT to_regclass('async_rollback_probe') IS NOT NULL"),
    )
