"""Source use cases, and the repository port they need.

The repository commits each call on its own, as docs/api.md asks: nothing
here changes two things in one transaction.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from typing import NamedTuple, Protocol

from dank.config import Settings
from dank.errors import Conflict, FieldProblem, NotFound
from dank.sources.model import (
    InvalidSource,
    NewSource,
    Source,
    normalized,
    sources_from_settings,
)


class SourceRepository(Protocol):
    async def get(self, source_id: uuid.UUID) -> Source | None: ...

    async def list(self) -> tuple[Source, ...]: ...

    async def add(self, source: Source) -> None:
        """Raises DuplicateSource if another source has its settings."""
        ...

    async def add_all(self, sources: Iterable[Source]) -> None:
        """For the import: all or nothing, and only into an empty table.

        Raises AlreadyImported if any source exists.
        """
        ...

    async def update(self, source: Source) -> None:
        """Raises NotFound, or DuplicateSource."""
        ...


class DuplicateSource(Conflict):
    code = "duplicate_source"

    def __init__(self) -> None:
        super().__init__("You already have a source with these settings.")


class AlreadyImported(Conflict):
    code = "already_imported"

    def __init__(self) -> None:
        super().__init__("Sources were already imported, so nothing changed.")


class ImportResult(NamedTuple):
    imported: int
    # Sources config.toml listed more than once, which were saved once.
    duplicates: int


class SourceService:
    def __init__(
        self,
        repository: SourceRepository,
        new_id: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._repository = repository
        self._new_id = new_id

    async def sources(self) -> tuple[Source, ...]:
        return await self._repository.list()

    async def source(self, source_id: uuid.UUID) -> Source:
        source = await self._repository.get(source_id)

        if source is None:
            raise NotFound("No source has this ID.")

        return source

    async def add(self, new: NewSource) -> Source:
        source = normalized(new).with_id(self._new_id())
        await self._repository.add(source)

        return source

    async def update(self, source: Source) -> Source:
        source = normalized(source)
        await self._repository.update(source)

        return source

    async def import_settings(self, settings: Settings) -> ImportResult:
        """Save config.toml's sources, once per database."""
        problems: list[FieldProblem] = []
        # Keyed by everything but the ID: what the table calls a duplicate.
        unique: dict[tuple[object, ...], Source] = {}
        imported = sources_from_settings(settings, self._new_id)

        for index, source in enumerate(imported):
            try:
                source = normalized(source)
            except InvalidSource as error:
                problems.extend(
                    problem._replace(path=f"sources.{index}.{problem.path}")
                    for problem in error.fields
                )

                continue

            unique.setdefault(tuple(source)[1:], source)

        if problems:
            raise InvalidSource(
                problems,
                "Some sources in config.toml can't be imported, so nothing "
                "was.",
            )

        await self._repository.add_all(unique.values())

        return ImportResult(len(unique), len(imported) - len(unique))
