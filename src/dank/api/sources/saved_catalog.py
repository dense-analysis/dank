"""SourceCatalog read from the sources table, when `sources_in_database`
is on."""

from dank.config import SourceConfig
from dank.sources.model import compile_sources
from dank.sources.service import SourceRepository


class SavedSourceCatalog:
    """Reads the table on every call, so an edit applies to the next request
    with nothing to invalidate. Listing 200 sources took a median of 2.46 ms
    in DEN-22's prototype."""

    def __init__(self, repository: SourceRepository) -> None:
        self._repository = repository

    async def sources(self) -> tuple[SourceConfig, ...]:
        return compile_sources(await self._repository.list()).known

    async def collectable(self) -> tuple[SourceConfig, ...]:
        return compile_sources(await self._repository.list()).collectable
