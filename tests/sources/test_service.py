"""Source use cases over the in-memory repository."""

import pathlib
import uuid

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
    add_source,
    get_source,
    import_settings,
    update_source,
)
from tests.sources.fakes import FakeSourceRepository


def settings_from(tmp_path: pathlib.Path, toml: str) -> Settings:
    config = tmp_path / "config.toml"
    config.write_text(toml)

    return load_settings(config)


@pytest.fixture
def repository() -> FakeSourceRepository:
    return FakeSourceRepository()


async def test_adding_normalizes_and_gives_a_new_id(
    repository: FakeSourceRepository,
) -> None:
    new = NewSource(
        enabled=True, domain=" Example.COM ", tags=("News",),
    )
    saved = await add_source(new, repository)

    assert saved.id.version == 4
    assert saved == Source(
        id=saved.id, enabled=True, domain="example.com",
        tags=("news",),
    )
    assert repository.rows == [saved]


async def test_an_invalid_source_saves_nothing(
    repository: FakeSourceRepository,
) -> None:
    with pytest.raises(InvalidSource):
        await add_source(
            NewSource(enabled=True, domain="not a domain"), repository,
        )

    assert repository.rows == []


async def test_a_duplicate_is_refused_even_when_disabled(
    repository: FakeSourceRepository,
) -> None:
    new = NewSource(enabled=True, domain="example.com")
    await add_source(new, repository)

    with pytest.raises(DuplicateSource) as caught:
        await add_source(
            new._replace(enabled=False, domain="EXAMPLE.com"), repository,
        )

    assert caught.value.code == "duplicate_source"


async def test_updating_normalizes_and_keeps_the_id(
    repository: FakeSourceRepository,
) -> None:
    saved = await add_source(
        NewSource(enabled=True, domain="x.com"), repository,
    )

    updated = await update_source(
        saved._replace(enabled=False, accounts=(Target(" someone "),)),
        repository,
    )

    assert updated == saved._replace(
        enabled=False, accounts=(Target("someone"),),
    )
    assert repository.rows == [updated]


async def test_updating_an_unknown_source_is_not_found(
    repository: FakeSourceRepository,
) -> None:
    with pytest.raises(NotFound):
        await update_source(
            Source(id=uuid.uuid4(), enabled=True, domain="example.com"),
            repository,
        )


async def test_reading_one_source(repository: FakeSourceRepository) -> None:
    saved = await add_source(
        NewSource(enabled=True, domain="example.com"), repository,
    )

    assert await get_source(saved.id, repository) == saved

    with pytest.raises(NotFound):
        await get_source(uuid.uuid4(), repository)


async def test_importing_saves_what_config_toml_collects(
    tmp_path: pathlib.Path,
    repository: FakeSourceRepository,
) -> None:
    settings = settings_from(tmp_path, '''
        sources = [
            "a.example",
            { domain = "x.com", accounts = ["someone"] },
            "A.example",
        ]
    ''')

    result = await import_settings(settings, repository)

    assert result == ImportResult(imported=2, duplicates=1)
    assert compile_sources(repository.rows).collectable == (
        SourceConfig("a.example", ()),
        SourceConfig("x.com", ("someone",)),
    )


async def test_importing_twice_is_refused(
    tmp_path: pathlib.Path,
    repository: FakeSourceRepository,
) -> None:
    settings = settings_from(tmp_path, 'sources = ["a.example"]')
    await import_settings(settings, repository)

    with pytest.raises(AlreadyImported):
        await import_settings(settings, repository)

    assert len(repository.rows) == 1


async def test_importing_nothing_saves_nothing(
    tmp_path: pathlib.Path,
    repository: FakeSourceRepository,
) -> None:
    result = await import_settings(settings_from(tmp_path, ""), repository)

    assert result == ImportResult(imported=0, duplicates=0)
    assert repository.rows == []


async def test_an_import_with_a_bad_source_saves_nothing_and_says_where(
    tmp_path: pathlib.Path,
    repository: FakeSourceRepository,
) -> None:
    # config.toml takes any text as a domain; the app wants a host name.
    settings = settings_from(
        tmp_path, 'sources = ["a.example", "https://b.example"]',
    )

    with pytest.raises(InvalidSource) as caught:
        await import_settings(settings, repository)

    assert [
        (problem.path, problem.message) for problem in caught.value.fields
    ] == [(
        "sources.1.domain",
        "Use a domain such as example.com. (https://b.example)",
    )]
    assert repository.rows == []
