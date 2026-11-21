"""Lock shared by collection and retention for a data directory."""

from __future__ import annotations

import fcntl
import pathlib
from collections.abc import Generator
from contextlib import contextmanager


class CollectionBusyError(RuntimeError):
    """An overlapping collection or scrub holds the required lock."""


@contextmanager
def collection_lock(
    data_dir: pathlib.Path, *, busy_message: str, shared: bool = False,
) -> Generator[None]:
    data_dir.mkdir(parents=True, exist_ok=True)
    operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX

    with (data_dir / "collection.lock").open("a") as lock:
        try:
            fcntl.flock(lock, operation | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CollectionBusyError(busy_message) from error

        yield
