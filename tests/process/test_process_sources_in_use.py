"""`process` works on every source in use, including ones turned off."""

import pathlib
from typing import Any

import pytest

from dank.config import Settings
from dank.db.sources import NO_SOURCES, SourcesError
from dank.process import (
    __main__ as cli,
    runner,
)
from tests.sources.fakes import (
    OFF,
    ON,
    SAVED,
    FakeSourcesInUse,
    no_logging,
    write_config,
)


@pytest.fixture
def processed(monkeypatch: pytest.MonkeyPatch) -> list[Settings]:
    """The settings each processing run was started with."""
    started: list[Settings] = []

    async def run_process(settings: Settings, **options: Any) -> int:
        started.append(settings)

        return 0

    monkeypatch.setattr(runner, "run_process", run_process)

    return started


@pytest.fixture
def in_use(monkeypatch: pytest.MonkeyPatch) -> FakeSourcesInUse:
    in_use = FakeSourcesInUse(SAVED)
    monkeypatch.setattr(runner, "sources_in_use", in_use)
    monkeypatch.setattr(runner, "configure_logging", no_logging)

    return in_use


def test_process_keeps_sources_that_are_turned_off(
    tmp_path: pathlib.Path,
    processed: list[Settings],
    in_use: FakeSourcesInUse,
) -> None:
    runner.run_process_from_config(write_config(tmp_path))

    assert [settings.sources for settings in processed] == [(ON, OFF)]


def test_process_says_why_it_has_no_sources(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: pathlib.Path,
    processed: list[Settings],
    in_use: FakeSourcesInUse,
) -> None:
    in_use.answer = SourcesError(NO_SOURCES)
    monkeypatch.setattr(
        "sys.argv", ["process", "--config", write_config(tmp_path)],
    )

    with pytest.raises(SystemExit) as exited:
        cli.main()

    assert exited.value.code == 1
    assert capsys.readouterr().err == f"process: {NO_SOURCES}\n"
    assert processed == []
