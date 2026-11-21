"""The Postgres source adapter against supplied SQLAlchemy driver results."""

import uuid
from typing import NamedTuple
from unittest.mock import MagicMock

import psycopg
import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dank.config import HackerNewsFilter, HackerNewsSourceConfig, SourceAdapter
from dank.db.models.sources import DUPLICATE_INDEX, SourceRow
from dank.db.sources import PostgresSourceRepository
from dank.errors import NotFound
from dank.sources.model import Adapter, Source, StoredSourceError, Target
from dank.sources.service import AlreadyImported, DuplicateSource


class SourceAdapterFixture(NamedTuple):
    repository: PostgresSourceRepository
    sessions: MagicMock
    session: MagicMock


@pytest.fixture
def storage() -> SourceAdapterFixture:
    session = MagicMock(spec=AsyncSession)
    sessions = MagicMock(spec=async_sessionmaker)
    sessions.return_value.__aenter__.return_value = session
    sessions.begin.return_value.__aenter__.return_value = session
    session.scalar.return_value = None

    return SourceAdapterFixture(
        PostgresSourceRepository(sessions), sessions, session,
    )


def source(index: int = 1, *, enabled: bool = True) -> Source:
    return Source(uuid.UUID(int=index), enabled, f"source{index}.example")


async def test_add_opens_a_transaction(storage: SourceAdapterFixture) -> None:
    await storage.repository.add(source())

    storage.sessions.begin.assert_called_once_with()
    storage.sessions.begin.return_value.__aexit__.assert_awaited_once_with(
        None, None, None,
    )
    storage.session.execute.assert_awaited_once()


async def test_every_field_round_trips(storage: SourceAdapterFixture) -> None:
    saved = Source(
        id=uuid.UUID(int=1), enabled=False, domain="news.ycombinator.com",
        feeds=(Target("https://example.com/café?q=\"a\\b\"", enabled=False),),
        tags=("\U0001f600",),
        adapters=(
            Adapter(SourceAdapter.HACKER_NEWS), Adapter(SourceAdapter.RSS),
        ),
        hacker_news=HackerNewsSourceConfig(HackerNewsFilter.TOP10),
    )
    await storage.repository.add(saved)
    statement = storage.session.execute.call_args.args[0]
    row = SourceRow(**statement.compile(dialect=postgresql.dialect()).params)
    storage.session.get.return_value = row
    storage.session.scalars.return_value = [row]

    assert await storage.repository.get(saved.id) == saved
    assert await storage.repository.list() == (saved,)


async def test_unknown_id_returns_none(storage: SourceAdapterFixture) -> None:
    storage.session.get.return_value = None

    assert await storage.repository.get(source().id) is None
    storage.session.get.assert_awaited_once_with(SourceRow, source().id)


@pytest.mark.parametrize("operation", ["add", "import", "update"])
@pytest.mark.parametrize("unique", [False, True])
async def test_writes_translate_only_the_duplicate_source_index(
    storage: SourceAdapterFixture, operation: str, *, unique: bool,
) -> None:
    original = MagicMock(spec=psycopg.errors.UniqueViolation)
    original.diag.constraint_name = DUPLICATE_INDEX if unique else "other"
    error = IntegrityError("write", {}, original)
    expected = DuplicateSource if unique else IntegrityError

    match operation:
        case "add":
            storage.session.execute.side_effect = error
            write = storage.repository.add(source())
        case "import":
            storage.session.execute.side_effect = [None, error]
            write = storage.repository.add_all([source()])
        case _:
            storage.session.scalar.side_effect = error
            write = storage.repository.update(source())

    with pytest.raises(expected):
        await write


async def test_large_configuration_is_sent_as_one_value(
    storage: SourceAdapterFixture,
) -> None:
    targets = tuple(Target(f"account_number_{n:06}") for n in range(1_000))
    value = source()._replace(domain="x.com", accounts=targets)
    await storage.repository.add(value)

    statement = storage.session.execute.call_args.args[0]
    row = statement.compile(dialect=postgresql.dialect()).params
    assert len(row["config"]["accounts"]) == 1_000
    assert row["id"] == value.id


async def test_import_locks_the_table_and_preserves_insert_order(
    storage: SourceAdapterFixture,
) -> None:
    sources = [source(2), source(1)]
    await storage.repository.add_all(sources)

    lock, insert = storage.session.execute.await_args_list
    assert lock.args[0].text == (
        "LOCK TABLE sources IN SHARE ROW EXCLUSIVE MODE"
    )
    assert [row["id"] for row in insert.args[1]] == [s.id for s in sources]
    storage.session.scalar.assert_awaited_once()
    storage.sessions.begin.assert_called_once_with()


async def test_import_refuses_to_overwrite_existing_rows(
    storage: SourceAdapterFixture,
) -> None:
    storage.session.scalar.return_value = source().id

    with pytest.raises(AlreadyImported):
        await storage.repository.add_all([source(2)])

    storage.session.execute.assert_awaited_once()


async def test_empty_import_does_not_insert_a_row(
    storage: SourceAdapterFixture,
) -> None:
    await storage.repository.add_all([])

    storage.session.execute.assert_awaited_once()


async def test_update_sends_the_id_and_encoded_configuration(
    storage: SourceAdapterFixture,
) -> None:
    edited = source(enabled=False)._replace(tags=("retained",))
    storage.session.scalar.return_value = edited.id
    await storage.repository.update(edited)

    statement = storage.session.scalar.call_args.args[0]
    params = statement.compile(dialect=postgresql.dialect()).params
    assert edited.id in params.values()
    assert params["enabled"] is False
    assert params["config"]["tags"] == ["retained"]


async def test_update_reports_a_missing_source(
    storage: SourceAdapterFixture,
) -> None:
    with pytest.raises(NotFound):
        await storage.repository.update(source())


async def test_a_row_this_code_cannot_read_fails_loudly(
    storage: SourceAdapterFixture,
) -> None:
    storage.session.scalars.return_value = [SourceRow(
        id=source().id, enabled=True, config={"domain": 1},
    )]

    with pytest.raises(StoredSourceError):
        await storage.repository.list()
