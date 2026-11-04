"""Answering for a source's icon, and the ports it needs."""

from typing import Protocol

from dank.api.source_icons.model import icon_domain
from dank.api.sources.service import SourceCatalog
from dank.errors import NotFound

# Today's wording, for a response the reader never shows.
NO_ICON = "Not Found"


class IconCache(Protocol):
    """Captures an icon on first use and keeps it. None when there is none."""

    async def get(self, domain: str) -> bytes | None: ...


class CollectedDomains(Protocol):
    async def has_posts(self, domain: str) -> bool: ...


class SourceIconService:
    def __init__(
        self,
        catalog: SourceCatalog,
        collected: CollectedDomains,
        icons: IconCache,
    ) -> None:
        self._catalog = catalog
        self._collected = collected
        self._icons = icons

    async def icon(self, requested: str) -> bytes:
        domain = icon_domain(requested)

        # Capturing fetches from the Internet, so only known sources may.
        if domain is None or not await self._known(domain):
            raise NotFound(NO_ICON)

        icon = await self._icons.get(domain)

        if icon is None:
            raise NotFound(NO_ICON)

        return icon

    async def _known(self, domain: str) -> bool:
        sources = await self._catalog.sources()

        return (
            any(source.domain == domain for source in sources)
            or await self._collected.has_posts(domain)
        )
