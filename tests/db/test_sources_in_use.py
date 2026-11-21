"""Commands select configured or saved sources without opening a database."""

import pathlib
import uuid
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest
from sqlalchemy.exc import DBAPIError

from dank.config import Settings, SourceConfig, load_settings
from dank.db import (
    atlas,
    sources as source_storage,
)
from dank.db.models.sources import SourceRow
from dank.db.sources import NO_SOURCES, SourcesError, sources_in_use
from dank.sources.model import CompiledSources, Source, Target, encode_config

CONFIG_SOURCES = (SourceConfig("config.example", ()),)


@pytest.fixture
def configured(memory_path: pathlib.Path) -> Settings:
    config = memory_path / "settings.toml"
    config.write_text("")

    return load_settings(config)._replace(
        sources=CONFIG_SOURCES,
        sources_in_database=True,
    )


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    session = AsyncMock()
    engine = AsyncMock()
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = session
    monkeypatch.setattr(atlas, "require_head", AsyncMock())
    monkeypatch.setattr(source_storage, "async_engine", MagicMock(
        return_value=engine,
    ))
    monkeypatch.setattr(source_storage, "async_sessions", MagicMock(
        return_value=factory,
    ))

    return session


def row(source: Source) -> SourceRow:
    return SourceRow(
        id=source.id, enabled=source.enabled, config=encode_config(source),
    )


async def test_config_sources_are_used_while_the_switch_is_off(
    configured: Settings,
) -> None:
    off = configured._replace(sources_in_database=False)

    assert await sources_in_use(off) == CompiledSources(
        CONFIG_SOURCES, CONFIG_SOURCES,
    )


async def test_saved_sources_replace_config_sources(
    configured: Settings, session: AsyncMock,
) -> None:
    session.scalars.return_value = [
        row(Source(
            uuid.UUID(int=1), domain="x.com", enabled=True,
            accounts=(Target("alice"), Target("bob", enabled=False)),
        )),
        row(Source(uuid.UUID(int=2), enabled=False, domain="off.example")),
    ]
    sources = await sources_in_use(configured)

    assert [(s.domain, s.accounts) for s in sources.known] == [
        ("x.com", ("alice", "bob")), ("off.example", ()),
    ]
    assert [(s.domain, s.accounts) for s in sources.collectable] == [
        ("x.com", ("alice",)),
    ]


async def test_saved_sources_are_ignored_while_the_switch_is_off(
    configured: Settings, session: AsyncMock,
) -> None:
    off = configured._replace(sources_in_database=False)
    sources = await sources_in_use(off)

    assert sources.known == CONFIG_SOURCES
    session.scalars.assert_not_awaited()


async def test_an_empty_table_says_how_to_fill_it(
    configured: Settings, session: AsyncMock,
) -> None:
    session.scalars.return_value = []

    with pytest.raises(SourcesError) as raised:
        await sources_in_use(configured)

    assert str(raised.value) == NO_SOURCES == (
        "No sources in the database. Run make sources-import, or add one in "
        "the app."
    )


async def test_the_schema_must_be_at_head(
    configured: Settings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(atlas, "require_head", AsyncMock(
        side_effect=atlas.SchemaError("Postgres schema is behind"),
    ))

    with pytest.raises(SourcesError, match="schema is behind"):
        await sources_in_use(configured)


async def test_a_row_this_code_cannot_read_is_an_error(
    configured: Settings, session: AsyncMock,
) -> None:
    session.scalars.return_value = [SourceRow(
        id=uuid.UUID(int=7), enabled=True, config={"domain": 1},
    )]

    with pytest.raises(SourcesError, match=r"Stored source .*000007"):
        await sources_in_use(configured)


async def test_an_outage_is_an_error_and_never_config_sources(
    configured: Settings, session: AsyncMock,
) -> None:
    session.scalars.side_effect = DBAPIError(
        "SELECT sources", {"password": "synthetic-password"},
        psycopg.OperationalError("connection refused"),
    )

    with pytest.raises(
        SourcesError, match="Postgres: connection refused",
    ) as raised:
        await sources_in_use(configured)

    assert "synthetic-password" not in str(raised.value)
    assert "SELECT" not in str(raised.value)
