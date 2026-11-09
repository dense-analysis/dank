"""The two places sources come from: config.toml and the sources table."""

import uuid

from dank.api.sources.config_catalog import ConfigSourceCatalog
from dank.api.sources.saved_catalog import SavedSourceCatalog
from dank.config import Settings
from dank.sources.model import Source, Target
from tests.sources.fakes import FakeSourceRepository


def saved(
    number: int,
    domain: str,
    *accounts: Target,
    enabled: bool = True,
) -> Source:
    return Source(uuid.UUID(int=number), enabled, domain, accounts)


async def test_config_sources_are_all_known_and_all_collected(
    settings: Settings,
) -> None:
    catalog = ConfigSourceCatalog(settings)

    assert await catalog.sources() == settings.sources
    assert await catalog.collectable() == settings.sources


async def test_saved_sources_stay_known_when_disabled() -> None:
    catalog = SavedSourceCatalog(FakeSourceRepository(
        saved(1, "x.com", Target("alice"), Target("bob", enabled=False)),
        saved(2, "off.example", enabled=False),
    ))

    assert [
        (source.domain, source.accounts)
        for source in await catalog.sources()
    ] == [("x.com", ("alice", "bob")), ("off.example", ())]


async def test_only_enabled_saved_sources_and_targets_are_collected() -> None:
    catalog = SavedSourceCatalog(FakeSourceRepository(
        saved(1, "x.com", Target("alice"), Target("bob", enabled=False)),
        saved(2, "off.example", enabled=False),
    ))

    assert [
        (source.domain, source.accounts)
        for source in await catalog.collectable()
    ] == [("x.com", ("alice",))]


async def test_saved_sources_are_read_again_each_time() -> None:
    repository = FakeSourceRepository()
    catalog = SavedSourceCatalog(repository)

    assert await catalog.sources() == ()

    await repository.add(saved(1, "new.example"))

    assert [source.domain for source in await catalog.sources()] == [
        "new.example",
    ]
