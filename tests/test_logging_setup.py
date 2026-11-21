import datetime
import logging
import pathlib
import time
from collections.abc import Iterator

import pytest

from dank.config import LoggingSettings
from dank.logging_setup import configure_logging


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers, root.level
    root.handlers = []

    try:
        yield
    finally:
        for handler in root.handlers:
            handler.close()

        root.handlers = handlers
        root.setLevel(level)


@pytest.mark.parametrize("level", ["INFO", "DEBUG"])
def test_console_streams_are_separate_and_file_keeps_both(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    level: str,
) -> None:
    path = tmp_path / "dank.log"
    configure_logging(LoggingSettings(path, level), component="scrape")
    logger = logging.getLogger("dank.scrape.test")
    logger.debug("debug detail")
    logger.info("regular progress")
    logger.warning("HTTP 429 example.test")
    logger.error("download failed")
    captured = capsys.readouterr()

    assert "regular progress" in captured.out
    assert "regular progress" not in captured.err
    assert "HTTP 429" in captured.err
    assert "download failed" in captured.err
    assert "HTTP 429" not in captured.out
    assert "download failed" not in captured.out
    assert ("debug detail" in captured.out) == (level == "DEBUG")
    assert "debug detail" not in captured.err
    saved = path.read_text()
    assert "regular progress" in saved
    assert "HTTP 429" in saved
    assert "download failed" in saved


def test_reconfiguring_logging_does_not_duplicate_console_messages(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = LoggingSettings(tmp_path / "dank.log", "INFO")
    configure_logging(settings, component="scrape")
    configure_logging(settings, component="process")
    logging.getLogger("dank.process.test").info("single progress event")

    assert capsys.readouterr().out.count("single progress event") == 1


@pytest.mark.parametrize("log_name", ["dank.log", "events.txt"])
def test_log_rotates_at_utc_midnight_and_keeps_the_configured_backup_count(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    log_name: str,
) -> None:
    now = datetime.datetime(2026, 11, 20, 23, 59, tzinfo=datetime.UTC)
    monkeypatch.setattr(time, "time", lambda: now.timestamp())
    path = tmp_path / log_name
    configure_logging(
        LoggingSettings(path, "INFO", backup_count=1), component="scrape",
    )
    logger = logging.getLogger("dank.scrape.test")
    logger.info("first day's entry")

    now += datetime.timedelta(minutes=2)
    logger.info("second day's entry")

    first_archive = tmp_path / f"{log_name}.2026-11-20"
    assert "first day's entry" in first_archive.read_text()
    assert "second day's entry" in path.read_text()

    now += datetime.timedelta(days=1)
    logger.info("third day's entry")

    assert not first_archive.exists()
    archive = tmp_path / f"{log_name}.2026-11-21"
    assert "second day's entry" in archive.read_text()
    assert "third day's entry" in path.read_text()
