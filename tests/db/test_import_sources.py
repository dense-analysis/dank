"""dank-db import-sources, the command behind make sources-import."""

import asyncio
import pathlib

import pytest

from dank.config import PostgresSettings, SourceConfig
from dank.db.__main__ import main
from dank.db.atlas import upgrade
from dank.db.engine import async_engine, async_sessions
from dank.db.sources import PostgresSourceRepository
from dank.sources.model import compile_sources


def write_config(
    path: pathlib.Path, settings: PostgresSettings, sources: str,
) -> pathlib.Path:
    path.write_text(f'''
        {sources}

        [postgres]
        host = "{settings.host}"
        port = {settings.port}
        database = "{settings.database}"
        user = "{settings.user}"
        password_file = "{settings.password_file}"
    ''')

    return path


def run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    config: pathlib.Path,
) -> tuple[int, str, str]:
    monkeypatch.setattr(
        "sys.argv", ["dank-db", "--config", str(config), "import-sources"],
    )

    with pytest.raises(SystemExit) as exited:
        main()

    output = capsys.readouterr()

    return int(exited.value.code or 0), output.out, output.err


def collectable(settings: PostgresSettings) -> tuple[SourceConfig, ...]:
    async def read() -> tuple[SourceConfig, ...]:
        engine = async_engine(settings)

        try:
            repository = PostgresSourceRepository(async_sessions(engine))

            return compile_sources(await repository.list()).collectable
        finally:
            await engine.dispose()

    return asyncio.run(read())


@pytest.fixture
def database(fresh_postgres: PostgresSettings) -> PostgresSettings:
    asyncio.run(upgrade(fresh_postgres))

    return fresh_postgres


def test_postgres_turned_off_is_reported(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = tmp_path / "config.toml"
    config.write_text('sources = ["a.example"]\n[postgres]\nenabled = false')

    code, out, err = run(monkeypatch, capsys, config)

    assert (code, out) == (1, "")
    assert "[postgres] enabled = false" in err


@pytest.mark.postgres
def test_sources_are_imported_once(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
) -> None:
    config = write_config(tmp_path / "config.toml", database, '''
        sources = [
            "a.example",
            { domain = "x.com", accounts = ["someone"] },
            "a.example",
        ]
    ''')

    first = run(monkeypatch, capsys, config)
    second = run(monkeypatch, capsys, config)

    assert first == (0, (
        f"Imported 2 sources into Postgres database {database.database}.\n"
        "Skipped 1 exact duplicate in config.toml.\n"
        "Collection still reads config.toml for now.\n"
    ), "")
    assert second == (
        1, "", "dank-db: Sources were already imported, so nothing changed.\n",
    )
    assert collectable(database) == (
        SourceConfig("a.example", ()),
        SourceConfig("x.com", ("someone",)),
    )


@pytest.mark.postgres
def test_an_empty_list_imports_nothing(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
) -> None:
    config = write_config(tmp_path / "config.toml", database, "sources = []")

    assert run(monkeypatch, capsys, config) == (
        0, "config.toml lists no sources, so nothing was imported.\n", "",
    )
    assert collectable(database) == ()


@pytest.mark.postgres
def test_a_source_the_app_cannot_take_stops_the_import(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
) -> None:
    config = write_config(
        tmp_path / "config.toml", database,
        'sources = ["a.example", "https://b.example"]',
    )

    assert run(monkeypatch, capsys, config) == (1, "", (
        "dank-db: Some sources in config.toml can't be imported, so nothing"
        " was.\n"
        "  sources.1.domain: Use a domain such as example.com.\n"
        "Sources are numbered from 0 in the order config.toml lists them.\n"
    ))
    assert collectable(database) == ()


@pytest.mark.postgres
def test_the_schema_must_be_at_head(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fresh_postgres: PostgresSettings,
) -> None:
    config = write_config(
        tmp_path / "config.toml", fresh_postgres, 'sources = ["a.example"]',
    )

    code, out, err = run(monkeypatch, capsys, config)

    assert (code, out) == (1, "")
    assert "schema is behind" in err
