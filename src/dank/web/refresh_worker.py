"""Child-process entry point for reader collections and their processing."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import suppress
from multiprocessing.connection import Connection

from dank.config import Settings
from dank.process.runner import run_process
from dank.scrape.runner import CollectionBusyError, run_scrape

logger = logging.getLogger(__name__)


def collect_refresh(
    settings: Settings,
    target_post: tuple[str, str] | None,
    connection: Connection,
) -> None:
    # Keep the worker and its browsers in one group for bounded shutdown.
    os.setsid()
    # Spawn owns the model, browser and loop, keeping HTTP reads responsive.
    logging.basicConfig(level=logging.INFO)

    try:
        asyncio.run(_collect(settings, target_post, connection))
    except CollectionBusyError:
        connection.send({
            "status": "failed",
            "message": "Another collection is already running. "
            "Try again when it finishes.",
        })
    except BaseException:
        logger.exception("Reader refresh worker stopped")

        with suppress(BrokenPipeError, EOFError, OSError):
            connection.send({
                "status": "failed",
                "message": "Refresh could not finish. "
                "Saved content is still available.",
            })
    finally:
        connection.close()


async def _collect(
    settings: Settings, target_post: tuple[str, str] | None,
    connection: Connection,
) -> None:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    owner_watch = asyncio.create_task(watch_owner(connection, task))

    try:
        await collect_and_process(settings, target_post, connection)
    finally:
        owner_watch.cancel()

        with suppress(asyncio.CancelledError):
            await owner_watch


async def watch_owner(
    connection: Connection, task: asyncio.Task[object],
) -> None:
    # An exec-based development reload closes the pipe without app cleanup.
    while not task.done():
        await asyncio.sleep(1)

        try:
            connection.send({})
        except (BrokenPipeError, EOFError, OSError):
            task.cancel()

            return


async def collect_and_process(
    settings: Settings, target_post: tuple[str, str] | None,
    connection: Connection,
) -> None:
    metrics = await run_scrape(
        settings, headless=True, target_post=target_post, scoped=True,
    )
    connection.send({
        "status": "processing", "message": "Preparing collected content…",
        "run_id": str(metrics.run_id),
        "posts_saved": metrics.counts()["posts_saved"],
    })

    if metrics.collected_posts:
        await run_process(
            settings, since=metrics.started_at,
            post_keys=metrics.collected_posts,
        )

    status = metrics.status

    if status not in {"completed", "partial", "failed"}:
        status = "failed"

    messages = {
        "completed": "Refresh complete. Updates are ready to view.",
        "partial": "Some content was collected; part of the refresh failed.",
        "failed": "Refresh could not collect content. Try again later.",
    }
    connection.send({"status": status, "message": messages[status]})
