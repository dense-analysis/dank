from __future__ import annotations

import asyncio
from collections.abc import Iterable


async def gather_tasks[T](tasks: Iterable[asyncio.Task[T]]) -> list[T]:
    tasks = list(tasks)

    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        # A failed writer must cancel producers blocked on a full queue.
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        raise
