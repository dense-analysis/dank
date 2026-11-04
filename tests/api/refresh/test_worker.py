"""The worker process's side of a refresh: real processes, signals, pipes."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import pathlib
import time
from multiprocessing.connection import Connection
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dank.api.refresh.worker import read_updates, stop_worker
from dank.config import Settings
from dank.scrape.metrics import RunMetrics
from dank.web.refresh_worker import collect_and_process, watch_owner
from tests.api.refresh.processes import (
    cooperative,
    stubborn_group_leader,
    stubborn_in_servers_group,
)

SPAWN = multiprocessing.get_context("spawn")
READY_SECONDS = 30


def gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True

    return False


async def until_gone(pid: int) -> None:
    # A browser left behind is reaped by init, some time after its group dies.
    async with asyncio.timeout(5):
        while not gone(pid):  # noqa: ASYNC110 - another process's lifetime
            await asyncio.sleep(0.01)


def ready_value(receive: Connection) -> Any:
    assert receive.poll(READY_SECONDS)

    return receive.recv()


def updates(receive: Connection) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    while receive.poll():
        result.append(receive.recv())

    return result


async def test_stop_kills_a_stubborn_worker_with_its_browsers() -> None:
    receive, send = SPAWN.Pipe(duplex=False)
    process = SPAWN.Process(target=stubborn_group_leader, args=(send,))
    process.start()
    browser = ready_value(receive)
    worker = process.pid

    assert worker is not None

    await stop_worker(process, stop_seconds=0.1)

    assert gone(worker)

    await until_gone(browser)


async def test_stop_never_signals_the_servers_own_group() -> None:
    receive, send = SPAWN.Pipe(duplex=False)
    process = SPAWN.Process(target=stubborn_in_servers_group, args=(send,))
    process.start()
    worker = ready_value(receive)

    # A signal to this group would end the test run here.
    assert os.getpgid(worker) == os.getpgrp()

    await stop_worker(process, stop_seconds=0.1)

    assert gone(worker)


async def test_stop_waits_for_a_worker_to_clean_up(
    tmp_path: pathlib.Path,
) -> None:
    marker = tmp_path / "cleaned"
    receive, send = SPAWN.Pipe(duplex=False)
    process = SPAWN.Process(target=cooperative, args=(send, str(marker)))
    process.start()
    worker = ready_value(receive)
    started = time.monotonic()
    await stop_worker(process)

    assert marker.read_text() == "cleaned up"
    assert gone(worker)
    assert time.monotonic() - started < READY_SECONDS


async def test_stop_ignores_a_process_that_never_started() -> None:
    await stop_worker(SPAWN.Process(target=print))


def test_updates_skip_heartbeats_and_end_with_the_pipe() -> None:
    receive, send = SPAWN.Pipe(duplex=False)
    send.send({})
    send.send({"status": "completed"})
    send.close()
    reports: list[Any] = []
    read_updates(receive, reports.append)

    assert reports == [{"status": "completed"}]


async def test_worker_cancels_collection_if_parent_closes_pipe() -> None:
    receive, send = SPAWN.Pipe(duplex=False)
    receive.close()
    task = asyncio.create_task(asyncio.Event().wait())
    await asyncio.wait_for(watch_owner(send, task), 2)

    with pytest.raises(asyncio.CancelledError):
        await task


def metrics(status: str, captures: set[tuple[str, str]]) -> RunMetrics:
    return cast("RunMetrics", SimpleNamespace(
        run_id="run", status=status, collected_posts=captures,
        started_at="collection-start",
        counts=lambda: {"posts_saved": len(captures)},
    ))


async def test_worker_processes_only_its_captures_and_preserves_partial_status(
    settings: Settings,
) -> None:
    captures = {("example.com", "one")}
    processed: list[tuple[Settings, dict[str, Any]]] = []
    scraped: list[dict[str, Any]] = []
    receive, send = SPAWN.Pipe(duplex=False)

    async def scrape(selected: Settings, **options: Any) -> RunMetrics:
        scraped.append(options)

        return metrics("partial", captures)

    async def process(selected: Settings, **options: Any) -> None:
        processed.append((selected, options))

    await collect_and_process(
        settings, ("example.com", "one"), send, exclusive=True,
        scrape=scrape, process=process,
    )

    assert scraped == [{
        "headless": True, "target_post": ("example.com", "one"),
        "scoped": True, "exclusive": True,
    }]
    assert processed == [(settings, {
        "since": "collection-start", "post_keys": captures,
    })]

    first, last = updates(receive)

    assert first == {
        "status": "processing", "message": "Preparing collected content…",
        "run_id": "run", "posts_saved": 1,
    }
    assert last == {
        "status": "partial",
        "message": "Some content was collected; part of the refresh failed.",
    }


@pytest.mark.parametrize(("status", "final"), [
    ("completed", "completed"),
    ("failed", "failed"),
    # A run with nothing to do isn't a refresh that finished.
    ("skipped", "failed"),
])
async def test_worker_without_captures_skips_processing(
    settings: Settings, status: str, final: str,
) -> None:
    receive, send = SPAWN.Pipe(duplex=False)

    async def scrape(selected: Settings, **options: Any) -> RunMetrics:
        return metrics(status, set())

    async def process(selected: Settings, **options: Any) -> None:
        raise AssertionError("Processed without captures")

    await collect_and_process(
        settings, None, send, scrape=scrape, process=process,
    )

    assert updates(receive)[-1]["status"] == final
