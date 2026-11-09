"""In-memory stand-ins for the source repository and the sources in use."""

import pathlib
import uuid
from collections.abc import Iterable, Sequence

from dank.config import Settings, SourceConfig
from dank.db.sources import SourcesError
from dank.errors import NotFound
from dank.sources.model import CompiledSources, Source, encode_config
from dank.sources.service import (
    AlreadyImported,
    DuplicateSource,
    SourceRepository,
)


class FakeSourceRepository:
    """Sources in a list, under the same two rules as the Postgres table:
    no two sources with identical settings, and the import only fills an
    empty table."""

    def __init__(self, *sources: Source) -> None:
        self.rows = list(sources)

    async def get(self, source_id: uuid.UUID) -> Source | None:
        return next((row for row in self.rows if row.id == source_id), None)

    async def list(self) -> tuple[Source, ...]:
        return tuple(self.rows)

    async def add(self, source: Source) -> None:
        self._refuse_duplicates([*self.rows, source])
        self.rows.append(source)

    async def add_all(self, sources: Iterable[Source]) -> None:
        if self.rows:
            raise AlreadyImported

        sources = list(sources)
        self._refuse_duplicates(sources)
        self.rows = sources

    async def update(self, source: Source) -> None:
        index = next(
            (n for n, row in enumerate(self.rows) if row.id == source.id),
            None,
        )

        if index is None:
            raise NotFound("No source has this ID.")

        rows = [*self.rows]
        rows[index] = source
        self._refuse_duplicates(rows)
        self.rows = rows

    @staticmethod
    def _refuse_duplicates(rows: Sequence[Source]) -> None:
        configs = [encode_config(row) for row in rows]

        if any(configs.count(config) > 1 for config in configs):
            raise DuplicateSource


# Pyright checks this assignment, so the fake can't drift from the port.
_PORT: type[SourceRepository] = FakeSourceRepository


class FakeSourcesInUse:
    """Stands in for dank.db.sources.sources_in_use, which reads Postgres.

    Give it the sources to answer with, or the error to raise.
    """

    def __init__(self, answer: CompiledSources | SourcesError) -> None:
        self.answer = answer
        self.calls = 0

    async def __call__(self, settings: Settings) -> CompiledSources:
        self.calls += 1

        if isinstance(self.answer, SourcesError):
            raise self.answer

        return self.answer


# One enabled source and one that is turned off.
ON = SourceConfig("on.example", ())
OFF = SourceConfig("off.example", ())
SAVED = CompiledSources(known=(ON, OFF), collectable=(ON,))


def no_logging(*_args: object, **_kwargs: object) -> None:
    """Stands in for configure_logging in a command under test.

    The real one would take over the root logger for the whole test run.
    """


def write_config(directory: pathlib.Path) -> str:
    """A config.toml with the switch on, whose own sources must be ignored."""
    path = directory / "config.toml"
    path.write_text("""
        sources = ["config.example"]
        sources_in_database = true
    """)

    return str(path)
