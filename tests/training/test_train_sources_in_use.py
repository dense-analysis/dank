"""`train` labels posts from every source in use, including ones turned off."""

import pathlib
from typing import Any

import pytest

from dank.config import Settings
from dank.db.sources import NO_SOURCES, SourcesError
from dank.training import __main__ as cli
from tests.sources.fakes import (
    OFF,
    ON,
    SAVED,
    FakeSourcesInUse,
    no_logging,
    write_config,
)


@pytest.fixture
def trained(monkeypatch: pytest.MonkeyPatch) -> list[Settings]:
    """The settings each training run was started with."""
    started: list[Settings] = []

    async def run_train(settings: Settings, **options: Any) -> int:
        started.append(settings)

        return 0

    monkeypatch.setattr(cli, "run_train", run_train)

    return started


@pytest.fixture
def in_use(monkeypatch: pytest.MonkeyPatch) -> FakeSourcesInUse:
    in_use = FakeSourcesInUse(SAVED)
    monkeypatch.setattr(cli, "sources_in_use", in_use)
    monkeypatch.setattr(cli, "configure_logging", no_logging)

    return in_use


def test_train_keeps_sources_that_are_turned_off(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    trained: list[Settings],
    in_use: FakeSourcesInUse,
) -> None:
    monkeypatch.setattr(
        "sys.argv", ["train", "--config", write_config(tmp_path)],
    )

    cli.main()

    assert [settings.sources for settings in trained] == [(ON, OFF)]


def test_train_says_why_it_has_no_sources(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: pathlib.Path,
    trained: list[Settings],
    in_use: FakeSourcesInUse,
) -> None:
    in_use.answer = SourcesError(NO_SOURCES)
    monkeypatch.setattr(
        "sys.argv", ["train", "--config", write_config(tmp_path)],
    )

    with pytest.raises(SystemExit) as exited:
        cli.main()

    assert exited.value.code == 1
    assert capsys.readouterr().err == f"train: {NO_SOURCES}\n"
    assert trained == []
