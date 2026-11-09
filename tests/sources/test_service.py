"""SourceService over the in-memory repository."""

import pathlib
import uuid
from collections.abc import Callable

import pytest

from dank.config import Settings, SourceConfig, load_settings
from dank.errors import NotFound
from dank.sources.model import (
    InvalidSource,
    NewSource,
    Source,
    Target,
    compile_sources,
)
from dank.sources.service import (
    AlreadyImported,
    DuplicateSource,
    ImportResult,
    SourceService,
)
from tests.sources.fakes import FakeSourceRepository


def counter() -> Callable[[], uuid.UUID]:
    numbers = iter(range(1, 10_000))

    return lambda: uuid.UUID(int=next(numbers))


def settings_from(tmp_path: pathlib.Path, toml: str) -> Settings:
    config = tmp_path / "config.toml"
    config.write_text(toml)

    return load_settings(config)


@pytest.fixture
def repository() -> FakeSourceRepository:
    return FakeSourceRepository()


@pytest.fixture
def service(repository: FakeSourceRepository) -> SourceService:
    return SourceService(repository, counter())


async def test_adding_normalizes_and_gives_a_new_id(
    service: SourceService, repository: FakeSourceRepository,
) -> None:
    saved = await service.add(NewSource(
        enabled=True, domain=" Example.COM ", tags=("News",),
    ))

    assert saved == Source(
        id=uuid.UUID(int=1), enabled=True, domain="example.com",
        tags=("news",),
    )
    assert repository.rows == [saved]


async def test_an_invalid_source_saves_nothing(
    service: SourceService, repository: FakeSourceRepository,
) -> None:
    with pytest.raises(InvalidSource):
        await service.add(NewSource(enabled=True, domain="not a domain"))

    assert repository.rows == []


async def test_a_duplicate_is_refused_even_when_disabled(
    service: SourceService,
) -> None:
    await service.add(NewSource(enabled=True, domain="example.com"))

    with pytest.raises(DuplicateSource) as caught:
        await service.add(NewSource(enabled=False, domain="EXAMPLE.com"))

    assert caught.value.code == "duplicate_source"


async def test_updating_normalizes_and_keeps_the_id(
    service: SourceService, repository: FakeSourceRepository,
) -> None:
    saved = await service.add(NewSource(enabled=True, domain="x.com"))

    updated = await service.update(saved._replace(
        enabled=False, accounts=(Target(" someone "),),
    ))

    assert updated == saved._replace(
        enabled=False, accounts=(Target("someone"),),
    )
    assert repository.rows == [updated]


async def test_updating_an_unknown_source_is_not_found(
    service: SourceService,
) -> None:
    with pytest.raises(NotFound):
        await service.update(
            Source(id=uuid.uuid4(), enabled=True, domain="example.com"),
        )


async def test_reading_one_source(service: SourceService) -> None:
    saved = await service.add(NewSource(enabled=True, domain="example.com"))

    assert await service.source(saved.id) == saved

    with pytest.raises(NotFound):
        await service.source(uuid.uuid4())


async def test_importing_saves_what_config_toml_collects(
    tmp_path: pathlib.Path,
    service: SourceService,
    repository: FakeSourceRepository,
) -> None:
    settings = settings_from(tmp_path, '''
        sources = [
            "a.example",
            { domain = "x.com", accounts = ["someone"] },
            "A.example",
        ]
    ''')

    result = await service.import_settings(settings)

    assert result == ImportResult(imported=2, duplicates=1)
    assert compile_sources(repository.rows).collectable == (
        SourceConfig("a.example", ()),
        SourceConfig("x.com", ("someone",)),
    )


async def test_importing_twice_is_refused(
    tmp_path: pathlib.Path,
    service: SourceService,
    repository: FakeSourceRepository,
) -> None:
    settings = settings_from(tmp_path, 'sources = ["a.example"]')
    await service.import_settings(settings)

    with pytest.raises(AlreadyImported):
        await service.import_settings(settings)

    assert len(repository.rows) == 1


async def test_importing_nothing_saves_nothing(
    tmp_path: pathlib.Path,
    service: SourceService,
    repository: FakeSourceRepository,
) -> None:
    result = await service.import_settings(settings_from(tmp_path, ""))

    assert result == ImportResult(imported=0, duplicates=0)
    assert repository.rows == []


async def test_an_import_with_a_bad_source_saves_nothing_and_says_where(
    tmp_path: pathlib.Path,
    service: SourceService,
    repository: FakeSourceRepository,
) -> None:
    # config.toml takes any text as a domain; the app wants a host name.
    settings = settings_from(
        tmp_path, 'sources = ["a.example", "https://b.example"]',
    )

    with pytest.raises(InvalidSource) as caught:
        await service.import_settings(settings)

    assert [problem.path for problem in caught.value.fields] == [
        "sources.1.domain",
    ]
    assert repository.rows == []
