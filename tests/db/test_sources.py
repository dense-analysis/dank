"""The sources table and its repository, against a real Postgres."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable

import pytest
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from dank.config import (
    HackerNewsFilter,
    HackerNewsSourceConfig,
    PostgresSettings,
    SourceAdapter,
)
from dank.db.engine import async_engine, async_sessions
from dank.db.models.sources import SourceRow
from dank.db.sources import PostgresSourceRepository
from dank.errors import NotFound
from dank.sources.model import (
    Adapter,
    Source,
    StoredSourceError,
    Target,
    encode_config,
)
from dank.sources.service import AlreadyImported, DuplicateSource

pytestmark = pytest.mark.postgres


def source(domain: str, *accounts: str, enabled: bool = True) -> Source:
    return Source(
        uuid.uuid4(), enabled, domain, tuple(Target(a) for a in accounts),
    )


async def outcomes(calls: list[Awaitable[None]]) -> list[str]:
    """What each call did, run all at once: "saved" or the error's name."""
    results = await asyncio.gather(*calls, return_exceptions=True)

    return [
        "saved" if result is None else type(result).__name__
        for result in results
    ]


@pytest.fixture
async def engine(
    postgres_settings: PostgresSettings,
) -> AsyncIterator[AsyncEngine]:
    engine = async_engine(postgres_settings)

    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE sources RESTART IDENTITY"))

    yield engine

    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessions(engine)


@pytest.fixture
def repository(
    sessions: async_sessionmaker[AsyncSession],
) -> PostgresSourceRepository:
    return PostgresSourceRepository(sessions)


async def test_a_saved_source_is_visible_to_an_open_connection(
    engine: AsyncEngine, repository: PostgresSourceRepository,
) -> None:
    async with engine.connect() as other:
        await other.execute(text("SELECT 1"))

        await repository.add(source("example.com"))

        assert await other.scalar(text("SELECT count(*) FROM sources")) == 1


async def test_every_field_round_trips(
    repository: PostgresSourceRepository,
) -> None:
    saved = Source(
        id=uuid.uuid4(),
        enabled=False,
        domain="news.ycombinator.com",
        feeds=(
            Target("https://example.com/café?q=\"a\\b\"", enabled=False),
        ),
        tags=("\U0001f600",),
        adapters=(
            Adapter(SourceAdapter.HACKER_NEWS),
            Adapter(SourceAdapter.RSS),
        ),
        hacker_news=HackerNewsSourceConfig(HackerNewsFilter.TOP10),
    )

    await repository.add(saved)

    assert await repository.get(saved.id) == saved
    assert await repository.list() == (saved,)
    assert await repository.get(uuid.uuid4()) is None


async def test_the_same_add_at_once_saves_one_source(
    repository: PostgresSourceRepository,
) -> None:
    results = await outcomes([
        repository.add(source("x.com", "someone")) for _ in range(20)
    ])

    assert sorted(results) == ["DuplicateSource"] * 19 + ["saved"]
    assert len(await repository.list()) == 1


async def test_a_disabled_copy_is_still_a_duplicate(
    repository: PostgresSourceRepository,
) -> None:
    await repository.add(source("x.com", "someone"))

    with pytest.raises(DuplicateSource):
        await repository.add(source("x.com", "someone", enabled=False))


async def test_a_large_source_is_saved_and_its_copy_refused(
    repository: PostgresSourceRepository,
) -> None:
    # Over the 2,704-byte limit of a plain btree index entry.
    accounts = [f"account_number_{n:06}" for n in range(1_000)]

    await repository.add(source("x.com", *accounts))

    with pytest.raises(DuplicateSource):
        await repository.add(source("x.com", *accounts))


async def test_edits_to_different_sources_at_once_all_stick(
    repository: PostgresSourceRepository,
) -> None:
    sources = [source("x.com", f"a{n}") for n in range(20)]
    await repository.add_all(sources)
    edited = [
        value._replace(tags=(f"tag{n}",)) for n, value in enumerate(sources)
    ]

    results = await outcomes([repository.update(value) for value in edited])

    assert results == ["saved"] * 20
    assert await repository.list() == tuple(edited)


async def test_edits_to_one_source_at_once_leave_one_whole_edit(
    repository: PostgresSourceRepository,
) -> None:
    original = source("x.com", "someone")
    await repository.add(original)
    edits = [
        original._replace(tags=(f"tag{n}",), enabled=bool(n % 2))
        for n in range(20)
    ]

    results = await outcomes([repository.update(value) for value in edits])

    assert results == ["saved"] * 20
    assert await repository.get(original.id) in edits


async def test_editing_follows_the_same_rules(
    repository: PostgresSourceRepository,
) -> None:
    first, second = source("a.example"), source("b.example")
    await repository.add_all([first, second])
    disabled = first._replace(enabled=False)

    with pytest.raises(NotFound):
        await repository.update(source("c.example"))

    with pytest.raises(DuplicateSource):
        await repository.update(second._replace(domain="a.example"))

    await repository.update(disabled)
    await repository.update(disabled)

    assert await repository.list() == (disabled, second)


async def test_sources_keep_the_order_they_were_added_in(
    repository: PostgresSourceRepository,
) -> None:
    imported = [source("x.com", f"a{n}") for n in range(300)]
    await repository.add_all(imported)
    edited = imported[150]._replace(enabled=False)
    added = source("example.com")

    await repository.update(edited)
    await repository.add(added)

    listed = await repository.list()

    assert [value.id for value in listed] == [
        *(value.id for value in imported), added.id,
    ]
    assert listed[150] == edited


async def test_the_import_only_fills_an_empty_table(
    repository: PostgresSourceRepository,
) -> None:
    await repository.add(source("a.example"))

    with pytest.raises(AlreadyImported):
        await repository.add_all([source("b.example")])

    assert [value.domain for value in await repository.list()] == [
        "a.example",
    ]


async def test_imports_at_once_import_once(
    repository: PostgresSourceRepository,
) -> None:
    batches = [
        [source(f"{n}-{m}.example") for m in range(3)] for n in range(10)
    ]

    results = await outcomes([repository.add_all(batch) for batch in batches])
    [winner] = [
        batch for batch, result in zip(batches, results, strict=True)
        if result == "saved"
    ]

    assert sorted(results) == ["AlreadyImported"] * 9 + ["saved"]
    assert await repository.list() == tuple(winner)


async def test_a_failed_import_leaves_nothing_behind(
    repository: PostgresSourceRepository,
) -> None:
    first = source("a.example")

    with pytest.raises(DuplicateSource):
        await repository.add_all(
            [first, source("b.example"), source("a.example")],
        )

    assert await repository.list() == ()

    await repository.add_all([first])

    assert await repository.list() == (first,)


async def test_a_writer_killed_before_committing_leaves_nothing(
    engine: AsyncEngine, repository: PostgresSourceRepository,
) -> None:
    killed = source("example.com")

    async with engine.connect() as writer, engine.connect() as admin:
        await writer.begin()
        await writer.execute(insert(SourceRow).values(
            id=killed.id, enabled=True, config=encode_config(killed),
        ))
        pid = await writer.scalar(text("SELECT pg_backend_pid()"))

        # What Postgres does when a client process dies mid-transaction.
        await admin.execute(
            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid},
        )
        await admin.commit()
        await repository.add(source("example.com"))

        with pytest.raises(Exception):  # noqa: B017
            await writer.commit()

    assert [value.id for value in await repository.list()] != [killed.id]
    assert len(await repository.list()) == 1


async def test_a_row_this_code_cannot_read_fails_loudly(
    engine: AsyncEngine, repository: PostgresSourceRepository,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(insert(SourceRow).values(
            id=uuid.uuid4(), enabled=True, config={"domain": 1},
        ))

    with pytest.raises(StoredSourceError):
        await repository.list()
