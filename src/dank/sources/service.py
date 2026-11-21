"""Source use cases, and the repository port they need.

The repository commits each call on its own, as docs/api.md asks: nothing
here changes two things in one transaction.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
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


async def get_source(
    source_id: uuid.UUID, repository: SourceRepository,
) -> Source:
    source = await repository.get(source_id)

    if source is None:
        raise NotFound("No source has this ID.")

    return source


async def add_source(
    new: NewSource, repository: SourceRepository,
) -> Source:
    source = normalized(new).with_id(uuid.uuid4())
    await repository.add(source)

    return source


async def update_source(
    source: Source, repository: SourceRepository,
) -> Source:
    source = normalized(source)
    await repository.update(source)

    return source


async def import_settings(
    settings: Settings, repository: SourceRepository,
) -> ImportResult:
    """Save config.toml's sources, once per database."""
    problems: list[FieldProblem] = []
    # Keyed by everything but the ID: what the table calls a duplicate.
    unique: dict[tuple[object, ...], Source] = {}
    imported = sources_from_settings(settings, uuid.uuid4)

    for index, source in enumerate(imported):
        try:
            source = normalized(source)
        except InvalidSource as error:
            # config.toml may have skipped entries it couldn't read, so
            # the domain says which source this is too.
            problems.extend(
                problem._replace(
                    path=f"sources.{index}.{problem.path}",
                    message=f"{problem.message} ({source.domain})",
                )
                for problem in error.fields
            )

            continue

        unique.setdefault(tuple(source)[1:], source)

    if problems:
        raise InvalidSource(
            problems,
            "Some sources in config.toml can't be imported, so nothing was.",
        )

    await repository.add_all(unique.values())

    return ImportResult(len(unique), len(imported) - len(unique))
