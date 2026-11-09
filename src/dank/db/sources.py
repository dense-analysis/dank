"""Sources in Postgres: one row each, and each call its own transaction."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any, NoReturn

import psycopg.errors
from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dank.db.models.sources import DUPLICATE_INDEX, SourceRow
from dank.errors import NotFound
from dank.sources.model import Source, decode_source, encode_config
from dank.sources.service import AlreadyImported, DuplicateSource


class PostgresSourceRepository:
    """The SourceRepository the web app and CLIs share."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get(self, source_id: uuid.UUID) -> Source | None:
        async with self._sessions() as session:
            row = await session.get(SourceRow, source_id)

        return None if row is None else _source(row)

    async def list(self) -> tuple[Source, ...]:
        async with self._sessions() as session:
            rows = await session.scalars(
                select(SourceRow).order_by(SourceRow.position),
            )

            return tuple(_source(row) for row in rows)

    async def add(self, source: Source) -> None:
        try:
            async with self._sessions.begin() as session:
                await session.execute(insert(SourceRow).values(_row(source)))
        except IntegrityError as error:
            _raise_duplicate(error)

    async def add_all(self, sources: Iterable[Source]) -> None:
        rows = [_row(source) for source in sources]

        try:
            async with self._sessions.begin() as session:
                # Stops two imports both finding the table empty. Readers
                # aren't blocked, and other writers wait for the commit.
                await session.execute(
                    text("LOCK TABLE sources IN SHARE ROW EXCLUSIVE MODE"),
                )

                if await session.scalar(select(SourceRow.id).limit(1)):
                    raise AlreadyImported

                if rows:
                    # Rows are inserted in order, so position keeps it.
                    await session.execute(insert(SourceRow), rows)
        except IntegrityError as error:
            _raise_duplicate(error)

    async def update(self, source: Source) -> None:
        try:
            async with self._sessions.begin() as session:
                updated = await session.scalar(
                    update(SourceRow)
                    .where(SourceRow.id == source.id)
                    .values(
                        enabled=source.enabled, config=encode_config(source),
                    )
                    .returning(SourceRow.id),
                )
        except IntegrityError as error:
            _raise_duplicate(error)

        if updated is None:
            raise NotFound("No source has this ID.")


def _row(source: Source) -> dict[str, Any]:
    return {
        "id": source.id,
        "enabled": source.enabled,
        "config": encode_config(source),
    }


def _source(row: SourceRow) -> Source:
    return decode_source(row.id, row.config, enabled=row.enabled)


def _raise_duplicate(error: IntegrityError) -> NoReturn:
    """Raise DuplicateSource for the duplicate index, or the error itself."""
    if (isinstance(error.orig, psycopg.errors.UniqueViolation)
            and error.orig.diag.constraint_name == DUPLICATE_INDEX):
        raise DuplicateSource from error

    raise error
