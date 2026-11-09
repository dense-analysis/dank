"""The sources a command works on: config.toml's, or the sources table's."""

import asyncio
import socket
import uuid

import pytest
from sqlalchemy import insert

from dank.config import PostgresSettings, Settings, SourceConfig, load_settings
from dank.db.atlas import upgrade
from dank.db.engine import async_engine, async_sessions
from dank.db.models.sources import SourceRow
from dank.db.sources import (
    NO_SOURCES,
    PostgresSourceRepository,
    SourcesError,
    sources_in_use,
)
from dank.sources.model import CompiledSources, Source, Target

CONFIG_SOURCES = (SourceConfig("config.example", ()),)


def settings(postgres: PostgresSettings, *, in_database: bool) -> Settings:
    return load_settings("config.example.toml")._replace(
        postgres=postgres,
        sources=CONFIG_SOURCES,
        sources_in_database=in_database,
    )


async def save(postgres: PostgresSettings, *sources: Source) -> None:
    engine = async_engine(postgres)

    try:
        await PostgresSourceRepository(async_sessions(engine)).add_all(sources)
    finally:
        await engine.dispose()


def closed_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))

        return int(listener.getsockname()[1])


@pytest.fixture
def database(fresh_postgres: PostgresSettings) -> PostgresSettings:
    asyncio.run(upgrade(fresh_postgres))

    return fresh_postgres


async def test_config_sources_are_used_while_the_switch_is_off() -> None:
    # Postgres is off, so reading it would fail.
    off = settings(PostgresSettings(enabled=False), in_database=False)

    assert await sources_in_use(off) == CompiledSources(
        CONFIG_SOURCES, CONFIG_SOURCES,
    )


@pytest.mark.postgres
async def test_saved_sources_replace_config_sources(
    database: PostgresSettings,
) -> None:
    await save(
        database,
        Source(
            uuid.uuid4(), enabled=True, domain="x.com",
            accounts=(Target("alice"), Target("bob", enabled=False)),
        ),
        Source(uuid.uuid4(), enabled=False, domain="off.example"),
    )

    sources = await sources_in_use(settings(database, in_database=True))

    assert [(s.domain, s.accounts) for s in sources.known] == [
        ("x.com", ("alice", "bob")), ("off.example", ()),
    ]
    assert [(s.domain, s.accounts) for s in sources.collectable] == [
        ("x.com", ("alice",)),
    ]


@pytest.mark.postgres
async def test_saved_sources_are_ignored_while_the_switch_is_off(
    database: PostgresSettings,
) -> None:
    await save(
        database, Source(uuid.uuid4(), enabled=True, domain="saved.example"),
    )

    sources = await sources_in_use(settings(database, in_database=False))

    assert sources.known == CONFIG_SOURCES


@pytest.mark.postgres
async def test_an_empty_table_says_how_to_fill_it(
    database: PostgresSettings,
) -> None:
    with pytest.raises(SourcesError) as raised:
        await sources_in_use(settings(database, in_database=True))

    assert str(raised.value) == NO_SOURCES == (
        "No sources in the database. Run make sources-import, or add one in "
        "the app."
    )


@pytest.mark.postgres
async def test_the_schema_must_be_at_head(
    fresh_postgres: PostgresSettings,
) -> None:
    with pytest.raises(SourcesError, match="schema is behind"):
        await sources_in_use(settings(fresh_postgres, in_database=True))


@pytest.mark.postgres
async def test_a_row_this_code_cannot_read_is_an_error(
    database: PostgresSettings,
) -> None:
    engine = async_engine(database)

    try:
        async with engine.begin() as connection:
            await connection.execute(insert(SourceRow).values(
                id=uuid.UUID(int=7), enabled=True, config={"domain": 1},
            ))
    finally:
        await engine.dispose()

    with pytest.raises(SourcesError) as raised:
        await sources_in_use(settings(database, in_database=True))

    assert "Stored source 00000000-0000-0000-0000-000000000007" in str(
        raised.value,
    )


@pytest.mark.postgres
async def test_an_outage_is_an_error_and_never_config_sources(
    database: PostgresSettings,
) -> None:
    down = database._replace(port=closed_port())

    with pytest.raises(SourcesError) as raised:
        await sources_in_use(settings(down, in_database=True))

    assert "Postgres" in str(raised.value)
    assert database.password_file.read_text().strip() not in str(raised.value)
