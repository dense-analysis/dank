"""In-memory stand-ins for the source icon feature's driven adapters."""

from dank.api.source_icons.service import CollectedDomains, IconCache


class FakeCollectedDomains:
    def __init__(self) -> None:
        self.domains: set[str] = set()

    async def has_posts(self, domain: str) -> bool:
        return domain in self.domains


class FakeIconCache:
    def __init__(self) -> None:
        self.icons: dict[str, bytes] = {}

    async def get(self, domain: str) -> bytes | None:
        return self.icons.get(domain)


# Pyright checks these assignments, so the fakes can't drift from the ports.
_DOMAINS: type[CollectedDomains] = FakeCollectedDomains
_ICONS: type[IconCache] = FakeIconCache
