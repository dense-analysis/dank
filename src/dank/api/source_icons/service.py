"""Answering for a source's icon, and the ports it needs."""

from typing import Protocol

from dank.api.source_icons.model import icon_domain
from dank.api.sources.service import LoadSources
from dank.errors import NotFound

NO_ICON = "Not Found"


class IconCache(Protocol):
    """Captures an icon on first use and keeps it. None when there is none."""

    async def get(self, domain: str) -> bytes | None: ...


class CollectedDomains(Protocol):
    async def has_posts(self, domain: str) -> bool: ...


async def source_icon(
    requested: str,
    load_sources: LoadSources,
    collected: CollectedDomains,
    icons: IconCache,
) -> bytes:
    domain = icon_domain(requested)

    if domain is None:
        raise NotFound(NO_ICON)

    # Capturing fetches from the Internet, so only known sources may.
    sources = (await load_sources()).known
    configured = any(source.domain == domain for source in sources)

    if not configured and not await collected.has_posts(domain):
        raise NotFound(NO_ICON)

    icon = await icons.get(domain)

    if icon is None:
        raise NotFound(NO_ICON)

    return icon
