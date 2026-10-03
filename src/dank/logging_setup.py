from __future__ import annotations

import logging
import pathlib
import sys

from dank.config import LoggingSettings


def configure_logging(
    settings: LoggingSettings,
    *,
    component: str,
) -> pathlib.Path:
    log_path = settings.file_path.expanduser()

    if not log_path.is_absolute():
        log_path = pathlib.Path.cwd() / log_path

    log_path.parent.mkdir(parents=True, exist_ok=True)
    level = _parse_level(settings.level)

    console_format = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S",
    )
    stdout = logging.StreamHandler(sys.stdout)
    stdout.addFilter(lambda record: record.levelno < logging.WARNING)
    stdout.setFormatter(console_format)
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setLevel(logging.WARNING)
    stderr.setFormatter(console_format)

    logging.basicConfig(
        level=level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            stdout,
            stderr,
        ],
        force=True,
    )
    logger = logging.getLogger(__name__)
    logger.info("Configured %s logging at %s", component, log_path)

    return log_path


def _parse_level(value: str) -> int:
    """Get the numeric log level from a string."""
    match value.strip().upper():
        case "CRITICAL":
            return logging.CRITICAL
        case "ERROR":
            return logging.ERROR
        case "WARNING" | "WARN":
            return logging.WARNING
        case "DEBUG":
            return logging.DEBUG
        case _:
            return logging.INFO
