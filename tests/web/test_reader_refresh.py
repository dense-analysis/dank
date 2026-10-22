from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from dank.config import SourceConfig, load_settings
from dank.web import reader_refresh as api
from dank.web.reader_refresh_scope import RefreshPlan


def settings(tmp_path: Path):
    return load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path, sources=(SourceConfig("example.com", ()),),
    )


def plan(tmp_path: Path, kind: str = "all") -> RefreshPlan:
    return RefreshPlan({"kind": kind}, kind, settings(tmp_path), None)


async def test_duplicate_scope_is_reused_and_other_scopes_are_bounded(
    tmp_path: Path,
):
    manager = api.RefreshManager(settings(tmp_path))
    first = manager.submit(plan(tmp_path))
    assert manager.submit(plan(tmp_path)) is first
    assert manager.pending.qsize() == 1

    for index in range(api.MAX_PENDING_JOBS - 1):
        manager.submit(plan(tmp_path, f"scope-{index}"))

    with pytest.raises(web.HTTPTooManyRequests):
        manager.submit(plan(tmp_path, "overflow"))

    assert len(manager.jobs) == api.MAX_PENDING_JOBS


async def test_queue_keeps_reads_available_and_runs_jobs_serially(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    entered = asyncio.Event()
    release = asyncio.Event()
    visited: list[str] = []

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        visited.append(selected.label)
        entered.set()
        await release.wait()
        manager.update(job, {"status": "completed"})

    monkeypatch.setattr(manager, "collect", collect)
    first = manager.submit(plan(tmp_path))
    second = manager.submit(plan(tmp_path, "reddit"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(entered.wait(), 1)
    assert first["status"] == "running"
    assert second["status"] == "queued"
    assert visited == ["all"]
    release.set()
    await asyncio.wait_for(manager.pending.join(), 1)
    assert visited == ["all", "reddit"]
    assert all(job["status"] == "completed" for job in manager.jobs)
    await manager.close()


async def test_worker_failure_does_not_stop_next_refresh(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        if selected.label == "all":
            raise RuntimeError("private database connection details")

        manager.update(job, {"status": "partial", "posts_saved": 2})

    monkeypatch.setattr(manager, "collect", collect)
    manager.submit(plan(tmp_path))
    manager.submit(plan(tmp_path, "reddit"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(manager.pending.join(), 1)
    assert [job["status"] for job in manager.jobs] == ["failed", "partial"]
    assert "private" not in json.dumps(manager.jobs)
    assert all(job["finished_at"] for job in manager.jobs)
    await manager.close()


async def test_restart_preserves_results_and_marks_unfinished_jobs_interrupted(
    tmp_path: Path,
):
    manager = api.RefreshManager(settings(tmp_path))
    complete = manager.submit(plan(tmp_path))
    manager.update(complete, {"status": "completed", "posts_saved": 3})
    manager.submit(plan(tmp_path, "reddit"))
    replacement = api.RefreshManager(settings(tmp_path))
    replacement.restore()
    assert [job["status"] for job in replacement.jobs] == [
        "completed", "interrupted",
    ]
    assert replacement.jobs[0]["posts_saved"] == 3
    assert replacement.pending.empty()
    assert replacement.jobs[1]["finished_at"]
    assert "settings" not in replacement.path.read_text()


async def test_shutdown_interrupts_active_and_queued_jobs(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def collect(job: Any, selected: Any):
        entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(manager, "collect", collect)
    manager.submit(plan(tmp_path))
    manager.submit(plan(tmp_path, "reddit"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(entered.wait(), 1)
    await manager.close()
    assert stopped.is_set()
    assert all(job["status"] == "interrupted" for job in manager.jobs)


async def test_history_retention_does_not_remove_active_jobs(tmp_path: Path):
    manager = api.RefreshManager(settings(tmp_path))

    for index in range(api.MAX_HISTORY_JOBS + 5):
        job = manager.submit(plan(tmp_path, str(index)))
        manager.pending.get_nowait()
        manager.pending.task_done()
        manager.update(job, {"status": "completed"})

    assert len(manager.jobs) == api.MAX_HISTORY_JOBS
    assert manager.jobs[-1]["scope"] == {"kind": "24"}


@pytest.mark.parametrize(
    "headers,body,status",
    [
        ({"Origin": "https://foreign.example"},
         {"scope": {"kind": "all"}}, 403),
        ({"Sec-Fetch-Site": "cross-site"}, {"scope": {"kind": "all"}}, 403),
        ({"Content-Type": "text/plain"}, {"scope": {"kind": "all"}}, 415),
        ({}, {}, 400),
        ({}, {"scope": {}, "url": "https://foreign.example"}, 400),
    ],
)
async def test_post_rejects_untrusted_or_malformed_requests(
    tmp_path: Path, monkeypatch: Any, headers: Any, body: Any, status: int,
):
    resolver = AsyncMock()
    monkeypatch.setattr(api, "resolve_scope", resolver)
    app = web.Application()
    app[api.REFRESH_MANAGER] = api.RefreshManager(settings(tmp_path))
    app.router.add_post("/api/reader/refresh", api.handle_refresh)

    request_headers = {"Content-Type": "application/json", **headers}
    request = make_mocked_request(
        "POST", "/api/reader/refresh", app=app, headers=request_headers,
    )
    monkeypatch.setattr(type(request), "json", AsyncMock(return_value=body))

    with pytest.raises(web.HTTPException) as error:
        await api.handle_refresh(request)

    assert error.value.status == status

    resolver.assert_not_called()


async def test_post_returns_immediately_and_get_never_collects(
    tmp_path: Path, monkeypatch: Any,
):
    resolver = AsyncMock(return_value=plan(tmp_path))
    monkeypatch.setattr(api, "resolve_scope", resolver)
    app = web.Application()
    app[api.REFRESH_MANAGER] = api.RefreshManager(settings(tmp_path))
    app["state"] = SimpleNamespace(settings=settings(tmp_path))
    app["clickhouse"] = AsyncMock()
    app.router.add_post("/api/reader/refresh", api.handle_refresh)
    app.router.add_get("/api/reader/refresh", api.handle_refresh)

    request = make_mocked_request("GET", "/api/reader/refresh", app=app)
    response = await api.handle_refresh(request)
    assert json.loads(response.text or "{}") == {"jobs": []}
    resolver.assert_not_called()
    request = make_mocked_request(
        "POST", "/api/reader/refresh", app=app,
        headers={"Content-Type": "application/json"},
    )
    monkeypatch.setattr(type(request), "json", AsyncMock(
        return_value={"scope": {"kind": "all"}},
    ))
    response = await api.handle_refresh(request)
    assert response.status == 202
    job = json.loads(response.text or "{}")["job"]
    assert job["status"] == "queued"
    request = make_mocked_request("GET", "/api/reader/refresh", app=app)
    response = await api.handle_refresh(request)
    assert json.loads(response.text or "{}")["jobs"] == [job]
    assert resolver.await_count == 1


@pytest.mark.parametrize("owns_group", [True, False])
async def test_forced_shutdown_cleans_only_worker_owned_processes(
    monkeypatch: Any, *, owns_group: bool,
):
    from unittest.mock import Mock

    process = Mock(pid=12345)
    process.is_alive.return_value = True
    kill_group = Mock()
    def group_id(pid: int) -> int:
        return pid if owns_group else 1

    monkeypatch.setattr(api.os, "getpgid", group_id)
    monkeypatch.setattr(api.os, "killpg", kill_group)
    await api.stop_worker(process)
    process.terminate.assert_called_once()

    if owns_group:
        kill_group.assert_called_once_with(12345, api.signal.SIGKILL)
        process.kill.assert_not_called()
    else:
        kill_group.assert_not_called()
        process.kill.assert_called_once()

    process.close.assert_called_once()


async def test_https_proxy_requests_still_require_matching_origin(
    tmp_path: Path, monkeypatch: Any,
):
    resolver = AsyncMock(return_value=plan(tmp_path))
    monkeypatch.setattr(api, "resolve_scope", resolver)
    app = web.Application()
    app[api.REFRESH_MANAGER] = api.RefreshManager(settings(tmp_path))
    app["state"] = SimpleNamespace(settings=settings(tmp_path))
    app["clickhouse"] = AsyncMock()
    request = make_mocked_request(
        "POST", "/api/reader/refresh", app=app,
        headers={"Content-Type": "application/json", "Host": "dank.local",
                 "X-Forwarded-Proto": "https", "Origin": "https://dank.local"},
    )
    monkeypatch.setattr(type(request), "json", AsyncMock(
        return_value={"scope": {"kind": "all"}},
    ))
    assert (await api.handle_refresh(request)).status == 202
    request = make_mocked_request(
        "POST", "/api/reader/refresh", app=app,
        headers={"Content-Type": "application/json", "Host": "dank.local",
                 "X-Forwarded-Proto": "https", "Origin": "https://evil.local"},
    )

    with pytest.raises(web.HTTPForbidden):
        await api.handle_refresh(request)

    assert resolver.await_count == 1


async def test_worker_cancels_collection_if_parent_reload_closes_pipe():
    from unittest.mock import Mock

    from dank.web.refresh_worker import watch_owner

    task = asyncio.create_task(asyncio.Event().wait())
    connection = Mock()
    connection.send.side_effect = BrokenPipeError()
    await asyncio.wait_for(watch_owner(connection, task), 2)

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_worker_processes_only_its_captures_and_preserves_partial_status(
    tmp_path: Path, monkeypatch: Any,
):
    from unittest.mock import Mock

    from dank.web import refresh_worker as worker

    captures = {("example.com", "one")}
    metrics = SimpleNamespace(
        run_id="run", status="partial", collected_posts=captures,
        started_at="collection-start", counts=lambda: {"posts_saved": 1},
    )
    scrape = AsyncMock(return_value=metrics)
    process = AsyncMock()
    monkeypatch.setattr(worker, "run_scrape", scrape)
    monkeypatch.setattr(worker, "run_process", process)
    connection = Mock()
    selected = settings(tmp_path)
    await worker.collect_and_process(selected, None, connection)
    process.assert_awaited_once_with(
        selected, since="collection-start", post_keys=captures,
    )
    updates = [call.args[0] for call in connection.send.call_args_list]
    assert [update["status"] for update in updates] == [
        "processing", "partial",
    ]
    assert updates[0]["posts_saved"] == 1


def source_plan(tmp_path: Path, domain: str, *, kind: str = "rss"):
    selected = settings(tmp_path)._replace(
        sources=(SourceConfig(domain, ()),),
    )

    return RefreshPlan({"kind": kind, "domains": [domain]}, domain, selected)


async def test_independent_sources_run_together_without_waiting(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    entered = {domain: asyncio.Event() for domain in (
        "blog.example", "news.ycombinator.com",
    )}
    release = asyncio.Event()

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        entered[selected.label].set()
        await release.wait()
        manager.update(job, {"status": "completed"})

    monkeypatch.setattr(manager, "collect", collect)
    first = manager.submit(source_plan(tmp_path, "blog.example"))
    second = manager.submit(source_plan(
        tmp_path, "news.ycombinator.com", kind="hacker_news",
    ))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(asyncio.gather(
        *(event.wait() for event in entered.values()),
    ), 1)
    assert first["status"] == second["status"] == "running"
    release.set()
    await asyncio.wait_for(manager.pending.join(), 1)
    await manager.close()


async def test_overlap_waits_but_unrelated_work_passes_the_queue(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    first_entered = asyncio.Event()
    other_entered = asyncio.Event()
    release = asyncio.Event()

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        (first_entered if selected.label == "reddit.com"
         else other_entered).set()
        await release.wait()
        manager.update(job, {"status": "completed"})

    monkeypatch.setattr(manager, "collect", collect)
    first = manager.submit(source_plan(tmp_path, "reddit.com", kind="reddit"))
    post = source_plan(tmp_path, "reddit.com", kind="post")._replace(
        scope={"kind": "post", "domain": "reddit.com", "post_id": "one"},
        target_post=("reddit.com", "one"),
    )
    queued = manager.submit(post)
    other = manager.submit(source_plan(tmp_path, "blog.example"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(asyncio.gather(
        first_entered.wait(), other_entered.wait(),
    ), 1)
    assert first["status"] == other["status"] == "running"
    assert queued["status"] == "queued"
    assert "reddit.com" in queued["message"]
    release.set()
    await asyncio.wait_for(manager.pending.join(), 1)
    await manager.close()


async def test_queued_all_is_a_barrier_and_cannot_be_starved(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    names = ("first.example", "all", "last.example")
    entered = {name: asyncio.Event() for name in names}
    release = {name: asyncio.Event() for name in names}

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        entered[selected.label].set()
        await release[selected.label].wait()
        manager.update(job, {"status": "completed"})

    monkeypatch.setattr(manager, "collect", collect)
    first = manager.submit(source_plan(tmp_path, names[0]))
    whole = manager.submit(plan(tmp_path))
    last = manager.submit(source_plan(tmp_path, names[2]))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(entered[names[0]].wait(), 1)
    assert first["status"] == "running"
    assert whole["status"] == last["status"] == "queued"
    release[names[0]].set()
    await asyncio.wait_for(entered["all"].wait(), 1)
    assert whole["status"] == "running"
    assert last["status"] == "queued"
    release["all"].set()
    await asyncio.wait_for(entered[names[2]].wait(), 1)
    release[names[2]].set()
    await asyncio.wait_for(manager.pending.join(), 1)
    await manager.close()


async def test_parallel_workers_are_bounded_and_cancel_together(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    entered = asyncio.Event()
    running = 0
    cancelled = 0

    async def collect(job: Any, selected: Any):
        nonlocal running, cancelled
        running += 1

        if running == api.MAX_CONCURRENT_JOBS:
            entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            cancelled += 1

    monkeypatch.setattr(manager, "collect", collect)

    for index in range(api.MAX_PENDING_JOBS):
        manager.submit(source_plan(tmp_path, f"source{index}.example"))

    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(entered.wait(), 1)
    assert running == api.MAX_CONCURRENT_JOBS
    assert sum(job["status"] == "queued" for job in manager.jobs) == (
        api.MAX_PENDING_JOBS - api.MAX_CONCURRENT_JOBS
    )
    await asyncio.wait_for(manager.close(), 1)
    assert cancelled == api.MAX_CONCURRENT_JOBS
    assert all(job["status"] == "interrupted" for job in manager.jobs)
    await asyncio.wait_for(manager.pending.join(), 1)


async def test_external_collection_busy_retries_without_failing_job(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    attempts = 0

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        nonlocal attempts
        attempts += 1
        manager.update(job, {
            "status": "queued" if attempts == 1 else "completed",
            "message": ("Another collection is running"
                        if attempts == 1 else "Done"),
        })

    monkeypatch.setattr(manager, "collect", collect)
    monkeypatch.setattr(api, "BUSY_RETRY_SECONDS", 0.01)
    job = manager.submit(source_plan(tmp_path, "blog.example"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(manager.pending.join(), 1)
    assert attempts == 2
    assert job["status"] == "completed"
    assert len(manager.jobs) == 1
    await manager.close()


async def test_history_keeps_long_running_job_while_other_jobs_finish(
    tmp_path: Path,
):
    manager = api.RefreshManager(settings(tmp_path))
    running = manager.submit(source_plan(tmp_path, "slow.example"))
    manager.pending.get_nowait()
    manager.update(running, {"status": "running"})

    for index in range(api.MAX_HISTORY_JOBS + 5):
        job = manager.submit(source_plan(tmp_path, f"source{index}.example"))
        manager.pending.get_nowait()
        manager.pending.task_done()
        manager.update(job, {"status": "completed"})

    assert running in manager.jobs
    assert len(manager.jobs) == api.MAX_HISTORY_JOBS
    manager.pending.task_done()


async def test_close_during_external_retry_drains_waiting_and_running_jobs(
    tmp_path: Path, monkeypatch: Any,
):
    manager = api.RefreshManager(settings(tmp_path))
    busy = asyncio.Event()
    independent = asyncio.Event()

    async def collect(job: dict[str, Any], selected: RefreshPlan):
        if selected.label == "busy.example":
            manager.update(job, {"status": "queued", "message": "Busy"})
            busy.set()

            return

        independent.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(manager, "collect", collect)
    monkeypatch.setattr(api, "BUSY_RETRY_SECONDS", 60)
    manager.submit(source_plan(tmp_path, "busy.example"))
    manager.worker = asyncio.create_task(manager.run())
    await asyncio.wait_for(busy.wait(), 1)
    manager.submit(source_plan(tmp_path, "busy.example", kind="post"))
    other = manager.submit(source_plan(tmp_path, "independent.example"))
    await asyncio.wait_for(independent.wait(), 1)
    assert other["status"] == "running"
    assert sum(job["status"] == "queued" for job in manager.jobs) == 2
    await asyncio.wait_for(manager.close(), 1)
    await asyncio.wait_for(manager.pending.join(), 1)
    assert not manager.running and not manager.waiting
    assert not manager.retry_after
    assert all(job["status"] == "interrupted" for job in manager.jobs)
