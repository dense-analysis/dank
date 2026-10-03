from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from types import TracebackType
from typing import Self

logger = logging.getLogger(__name__)


class Progress:
    """Report completed work and keep long asynchronous stages visible."""

    def __init__(
        self,
        label: str,
        total: int | None = None,
        *,
        interval: float = 5.0,
    ) -> None:
        self.label = label
        self.total = total
        self.completed = 0
        self.interval = interval
        self._started = 0.0
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        self._started = time.monotonic()
        self._report("started")
        self._task = asyncio.create_task(self._heartbeat())

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._task is not None:
            self._task.cancel()

            with suppress(asyncio.CancelledError):
                await self._task

        if exc_type is not None:
            logger.warning(
                "%s stopped after %.1fs (%d completed)",
                self.label, time.monotonic() - self._started, self.completed,
            )
        else:
            self._report("finished")

    def advance(self) -> None:
        self.completed += 1

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            self._report("still running")

    def _report(self, state: str) -> None:
        elapsed = time.monotonic() - self._started

        if self.total is None:
            logger.info("%s: %s (%.1fs elapsed)", self.label, state, elapsed)
        else:
            logger.info(
                "%s: %d/%d completed (%.1fs elapsed)",
                self.label, self.completed, self.total, elapsed,
            )
