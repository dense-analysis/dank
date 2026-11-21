"""Refresh worker reporting and shutdown with in-memory process control."""

from __future__ import annotations

import asyncio
import os
import signal
from collections import deque
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dank.api.refresh.model import RefreshPlan
from dank.api.refresh.worker import ProcessWorker, read_updates, stop_worker
from dank.config import Settings
from dank.scrape.metrics import RunMetrics
from dank.web.refresh_worker import collect_and_process, watch_owner


class Process:
    def __init__(
        self, *, pid: int | None = 42, exits_on_terminate: bool = False,
    ) -> None:
        self.pid = pid
        self.exits_on_terminate = exits_on_terminate
        self.started = False
        self.alive = pid is not None
        self.terminated = False
        self.killed = False
        self.closed = False
        self.waits: list[float | None] = []

    def is_alive(self) -> bool:
        return self.alive

    def start(self) -> None:
        self.started = True

    def terminate(self) -> None:
        self.terminated = True

    def join(self, timeout: float | None = None) -> None:
        self.waits.append(timeout)

        if self.terminated and self.exits_on_terminate:
            self.alive = False

    def kill(self) -> None:
        self.killed = True
        self.alive = False

    def close(self) -> None:
        assert not self.alive
        self.closed = True


class Messages:
    def __init__(
        self, *messages: dict[str, Any], closed: bool = False,
    ) -> None:
        self.messages: deque[dict[str, Any]] = deque(messages)
        self.closed = closed

    def send(self, message: dict[str, Any]) -> None:
        if self.closed:
            raise BrokenPipeError

        self.messages.append(message)

    def poll(self) -> bool:
        return bool(self.messages) or self.closed

    def close(self) -> None:
        self.closed = True

    def recv(self) -> dict[str, Any]:
        if not self.messages:
            raise EOFError

        return self.messages.popleft()


async def test_stop_kills_a_stubborn_workers_own_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Process()
    signals: list[tuple[int, int]] = []

    def kill_group(group: int, sig: int) -> None:
        signals.append((group, sig))
        process.alive = False

    monkeypatch.setattr(os, "getpgid", int)
    monkeypatch.setattr(os, "killpg", kill_group)
    await stop_worker(cast(BaseProcess, process), stop_seconds=0.1)

    assert process.terminated and process.closed and not process.killed
    assert signals == [(process.pid, signal.SIGKILL)]
    assert process.waits == [0.1, 5, 0]


async def test_stop_never_signals_the_servers_own_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Process()

    def server_group(pid: int) -> int:
        return 1

    def unexpected_signal(group: int, sig: int) -> None:
        pytest.fail(f"Signalled the server group {group} with {sig}")

    monkeypatch.setattr(os, "getpgid", server_group)
    monkeypatch.setattr(os, "killpg", unexpected_signal)
    await stop_worker(cast(BaseProcess, process), stop_seconds=0.1)

    assert process.killed and process.closed


async def test_stop_waits_for_a_worker_to_clean_up() -> None:
    process = Process(exits_on_terminate=True)
    await stop_worker(cast(BaseProcess, process), stop_seconds=2)

    assert process.terminated and process.closed and not process.killed
    assert process.waits == [2, 0]


async def test_stop_ignores_a_process_that_never_started() -> None:
    process = Process(pid=None)
    await stop_worker(cast(BaseProcess, process))

    assert not process.terminated and not process.closed
    assert not process.waits


def test_updates_skip_heartbeats_and_end_with_the_pipe() -> None:
    receive = Messages({}, {"status": "completed"}, closed=True)
    reports: list[Any] = []
    read_updates(cast(Connection, receive), reports.append)

    assert reports == [{"status": "completed"}]


async def test_collection_drains_the_worker_and_closes_its_resources(
    settings: Settings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    receive = Messages({}, {"status": "completed", "posts_saved": 2})
    send = Messages()
    reports: list[dict[str, Any]] = []
    process = Process()
    process.alive = False
    spawned: dict[str, Any] = {}

    def child(**options: Any) -> Process:
        spawned.update(options)

        return process

    def pipe(*, duplex: bool) -> tuple[Messages, Messages]:
        assert not duplex

        return receive, send

    def context(method: str) -> object:
        assert method == "spawn"

        return SimpleNamespace(Pipe=pipe, Process=child)

    monkeypatch.setattr("multiprocessing.get_context", context)
    plan = RefreshPlan(settings=settings, scope={"kind": "all"}, label="All")
    await ProcessWorker().collect("job", plan, lambda row: reports.append(
        dict(row),
    ))

    assert spawned["name"] == "dank-refresh-job"
    assert spawned["args"] == (settings, None, send)
    assert process.started
    assert reports == [{"status": "completed", "posts_saved": 2}]
    assert process.closed and receive.closed and send.closed


async def test_worker_cancels_collection_if_parent_closes_pipe() -> None:
    send = Messages(closed=True)
    task = asyncio.create_task(asyncio.Event().wait())
    await watch_owner(cast(Connection, send), task, heartbeat_seconds=0)

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
    send = Messages()

    async def scrape(selected: Settings, **options: Any) -> RunMetrics:
        scraped.append(options)

        return metrics("partial", captures)

    async def process(selected: Settings, **options: Any) -> None:
        processed.append((selected, options))

    await collect_and_process(
        settings, ("example.com", "one"), cast(Connection, send),
        exclusive=True, scrape=scrape, process=process,
    )

    assert scraped == [{
        "headless": True, "target_post": ("example.com", "one"),
        "scoped": True, "exclusive": True,
    }]
    assert processed == [(settings, {
        "since": "collection-start", "post_keys": captures,
    })]

    first, last = send.messages

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
    send = Messages()

    async def scrape(selected: Settings, **options: Any) -> RunMetrics:
        return metrics(status, set())

    async def process(selected: Settings, **options: Any) -> None:
        raise AssertionError("Processed without captures")

    await collect_and_process(
        settings, None, cast(Connection, send), scrape=scrape, process=process,
    )

    assert send.messages[-1]["status"] == final
