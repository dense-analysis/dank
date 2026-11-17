"""dank-db import-sources, the command behind make sources-import."""

import pathlib
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import PostgresSettings, SourceConfig
from dank.db import (
    __main__ as cli,
    atlas,
)
from dank.sources.model import compile_sources
from tests.sources.fakes import FakeSourceRepository


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
        cli.main()

    output = capsys.readouterr()

    return int(exited.value.code or 0), output.out, output.err


@pytest.fixture
def database() -> PostgresSettings:
    return PostgresSettings(database="test_sources")


@pytest.fixture
def repository(monkeypatch: pytest.MonkeyPatch) -> FakeSourceRepository:
    repository = FakeSourceRepository()
    monkeypatch.setattr(atlas, "require_head", AsyncMock())
    monkeypatch.setattr(cli, "async_engine", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(cli, "async_sessions", Mock())
    monkeypatch.setattr(
        cli, "PostgresSourceRepository", Mock(return_value=repository),
    )

    return repository


def test_an_import_with_the_switch_on_is_in_use_at_once(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
    repository: FakeSourceRepository,
) -> None:
    config = write_config(memory_path / "config.toml", database, '''
        sources_in_database = true
        sources = ["a.example"]
    ''')

    assert run(monkeypatch, capsys, config) == (0, (
        f"Imported 1 source into Postgres database {database.database}.\n"
        "DANK uses these sources now.\n"
    ), "")
    assert compile_sources(repository.rows).collectable == (
        SourceConfig("a.example", ()),
    )


def test_postgres_turned_off_is_reported(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = memory_path / "config.toml"
    config.write_text('sources = ["a.example"]\n[postgres]\nenabled = false')

    code, out, err = run(monkeypatch, capsys, config)

    assert (code, out) == (1, "")
    assert "[postgres] enabled = false" in err


def test_sources_are_imported_once(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
    repository: FakeSourceRepository,
) -> None:
    config = write_config(memory_path / "config.toml", database, '''
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
        "DANK is set to use the sources in config.toml. To use the ones in "
        "Postgres instead, set this near the top of config.toml, before any "
        "[section], replacing any sources_in_database line already there, "
        "and restart DANK:\n"
        "\n"
        "sources_in_database = true\n"
    ), "")
    assert second == (
        1, "", "dank-db: Sources were already imported, so nothing changed.\n",
    )
    assert compile_sources(repository.rows).collectable == (
        SourceConfig("a.example", ()),
        SourceConfig("x.com", ("someone",)),
    )


def test_an_empty_list_imports_nothing(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
    repository: FakeSourceRepository,
) -> None:
    config = write_config(
        memory_path / "config.toml", database, "sources = []",
    )

    assert run(monkeypatch, capsys, config) == (
        0, "config.toml lists no sources, so nothing was imported.\n", "",
    )
    assert compile_sources(repository.rows).collectable == ()


def test_a_source_the_app_cannot_take_stops_the_import(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
    repository: FakeSourceRepository,
) -> None:
    config = write_config(
        memory_path / "config.toml", database,
        'sources = ["a.example", "https://b.example"]',
    )

    assert run(monkeypatch, capsys, config) == (1, "", (
        "dank-db: Some sources in config.toml can't be imported, so nothing"
        " was.\n"
        "  sources.1.domain: Use a domain such as example.com."
        " (https://b.example)\n"
        "Sources are counted from 0, leaving out any entry config.toml"
        " ignores.\n"
    ))
    assert compile_sources(repository.rows).collectable == ()


def test_the_schema_must_be_at_head(
    memory_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database: PostgresSettings,
) -> None:
    monkeypatch.setattr(atlas, "require_head", AsyncMock(
        side_effect=atlas.SchemaError("Postgres schema is behind"),
    ))
    config = write_config(
        memory_path / "config.toml", database, 'sources = ["a.example"]',
    )

    code, out, err = run(monkeypatch, capsys, config)

    assert (code, out) == (1, "")
    assert "schema is behind" in err
