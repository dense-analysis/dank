"""The two places sources come from: config.toml and the sources table."""

import uuid

from dank.api.sources.deps import source_catalog
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
    sources = await source_catalog(settings, None)()

    assert sources.known == settings.sources
    assert sources.collectable == settings.sources


async def test_only_enabled_saved_sources_and_targets_are_collected(
    settings: Settings,
) -> None:
    repository = FakeSourceRepository(
        saved(1, "x.com", Target("alice"), Target("bob", enabled=False)),
        saved(2, "off.example", enabled=False),
    )
    sources = await source_catalog(settings, repository)()

    assert [
        (source.domain, source.accounts) for source in sources.known
    ] == [("x.com", ("alice", "bob")), ("off.example", ())]
    assert [
        (source.domain, source.accounts) for source in sources.collectable
    ] == [("x.com", ("alice",))]


async def test_saved_sources_are_read_again_each_time(
    settings: Settings,
) -> None:
    repository = FakeSourceRepository()
    load_sources = source_catalog(settings, repository)

    empty = await load_sources()
    assert empty.known == empty.collectable == ()

    await repository.add(saved(1, "new.example"))

    sources = await load_sources()
    assert [source.domain for source in sources.known] == ["new.example"]
