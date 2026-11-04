"""Engines and sessions for Postgres, built from Settings."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql
from sqlalchemy import URL, Engine, create_engine, event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from dank.config import PostgresSettings, read_postgres_password

MAINTENANCE_DATABASE = "postgres"


def postgres_url(settings: PostgresSettings) -> URL:
    """The address without a password, so printing it reveals nothing."""
    return URL.create(
        "postgresql+psycopg",
        username=settings.user,
        host=settings.host,
        port=settings.port,
        database=settings.database,
    )


def _read_password_on_connect(
    engine: Engine,
    settings: PostgresSettings,
) -> None:
    # An engine holds no password, and can exist before Postgres or the file
    # does. A new connection fails on its own if either is still missing.
    # https://docs.sqlalchemy.org/en/21/core/engines.html#generating-dynamic-authentication-tokens
    def add_password(
        _dialect: Any,
        _record: Any,
        _args: Any,
        params: dict[str, Any],
    ) -> None:
        params["password"] = read_postgres_password(settings)

    event.listen(engine, "do_connect", add_password)


def sync_engine(settings: PostgresSettings) -> Engine:
    """An engine for CLIs."""
    engine = create_engine(postgres_url(settings), pool_pre_ping=True)
    _read_password_on_connect(engine, settings)

    return engine


def async_engine(settings: PostgresSettings) -> AsyncEngine:
    """An engine for the web app."""
    engine = create_async_engine(postgres_url(settings), pool_pre_ping=True)
    _read_password_on_connect(engine.sync_engine, settings)

    return engine


def sync_sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine)


def async_sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # Keep loaded values readable after commit, which can't lazily reload
    # them outside a greenlet.
    return async_sessionmaker(engine, expire_on_commit=False)


def create_database(settings: PostgresSettings) -> bool:
    """Create the configured database if it's missing, for a new worktree.

    Returns whether it created one.
    """
    with psycopg.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=read_postgres_password(settings),
        dbname=MAINTENANCE_DATABASE,
        autocommit=True,
    ) as connection:
        found = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (settings.database,),
        ).fetchone()

        if found:
            return False

        try:
            connection.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(settings.database),
                ),
            )
        except psycopg.errors.DuplicateDatabase:
            # Another worktree's command got there first.
            return False

    return True
