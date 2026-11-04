"""Throwaway Postgres databases for the postgres test lane."""

from __future__ import annotations

import pathlib
from urllib.parse import urlsplit

import psycopg
from psycopg import sql

from dank.config import PostgresSettings, read_postgres_password
from dank.db.engine import (
    MAINTENANCE_DATABASE,
    create_database as create_configured_database,
)


def server_settings(url: str, directory: pathlib.Path) -> PostgresSettings:
    parts = urlsplit(url)
    password_file = directory / "password"
    password_file.write_text(parts.password or "")

    return PostgresSettings(
        host=parts.hostname or "127.0.0.1",
        port=parts.port or 5432,
        database=MAINTENANCE_DATABASE,
        user=parts.username or "postgres",
        password_file=password_file,
    )


def create_database(settings: PostgresSettings) -> None:
    create_configured_database(settings)


def drop_database(settings: PostgresSettings) -> None:
    with psycopg.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=read_postgres_password(settings),
        dbname=MAINTENANCE_DATABASE,
        autocommit=True,
    ) as connection:
        # FORCE, since a failed test can leave connections behind.
        connection.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(settings.database),
            ),
        )
