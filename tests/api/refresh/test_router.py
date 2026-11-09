"""Refresh jobs over HTTP: the real router, service and scheduler."""

import asyncio
import json
import pathlib
import uuid
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api import deps
from dank.api.refresh.deps import refresh_queue
from dank.api.refresh.manager import RefreshManager, Report, running_refresh
from dank.api.refresh.model import (
    MAX_CONCURRENT_JOBS,
    MAX_HISTORY_JOBS,
    MAX_PENDING_JOBS,
    RefreshPlan,
)
from dank.api.sources.deps import saved_sources
from dank.config import Settings
from dank.sources.model import Source, Target
from tests.api.client import http_client
from tests.api.refresh.fakes import FakeRefreshTargets, FakeWorker
from tests.sources.fakes import FakeSourceRepository

PATH = "/api/reader/refresh"
QUEUE_FULL = "The refresh queue is full. Try again when a job finishes."


def source(domain: str) -> dict[str, Any]:
    return {"kind": "rss", "domains": [domain]}


async def start(client: AsyncClient, scope: dict[str, Any]) -> dict[str, Any]:
    response = await client.post(PATH, json={"scope": scope})

    assert response.status_code == 202

    return response.json()["job"]


async def jobs(client: AsyncClient) -> list[dict[str, Any]]:
    response = await client.get(PATH)

    assert response.status_code == 200

    return response.json()["jobs"]


async def statuses(client: AsyncClient) -> dict[str, str]:
    return {job["label"]: job["status"] for job in await jobs(client)}


async def finished(manager: RefreshManager) -> None:
    await asyncio.wait_for(manager.pending.join(), 1)


def saved(number: int, domain: str, *, enabled: bool = True) -> Source:
    return Source(uuid.UUID(int=number), enabled, domain)


def switch_to(
    app: FastAPI, settings: Settings, repository: FakeSourceRepository,
) -> None:
    """Turn `sources_in_database` on, with this table behind it."""
    switched = settings._replace(sources_in_database=True)
    app.dependency_overrides[deps.settings] = lambda: switched
    app.dependency_overrides[saved_sources] = lambda: repository


async def test_post_returns_immediately_and_get_never_collects(
    client: AsyncClient, worker: FakeWorker,
) -> None:
    release = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        await release.wait()
        report({"status": "completed"})

    worker.script = hold

    assert await jobs(client) == []

    job = await start(client, {"kind": "all"})

    assert job["scope"] == {"kind": "all"}
    assert job["label"] == "All sources"
    assert job["status"] == "queued"
    assert job["message"] == "Waiting for collection…"
    assert job["finished_at"] is None
    assert job["posts_saved"] == 0
    assert job["run_id"] is None
    assert [item["id"] for item in await jobs(client)] == [job["id"]]

    release.set()


async def test_scope_comes_back_normalized_with_absent_keys_left_out(
    client: AsyncClient,
) -> None:
    job = await start(client, {
        "kind": "reddit", "community": "r/PYTHON",
        "domains": ["reddit.com", "reddit.com"], "tags": [],
    })

    assert job["scope"] == {
        "kind": "reddit", "community": "python", "domains": ["reddit.com"],
    }


async def test_duplicate_scope_is_reused_and_other_scopes_are_bounded(
    client: AsyncClient, worker: FakeWorker,
) -> None:
    release = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        await release.wait()
        report({"status": "completed"})

    worker.script = hold
    first = await start(client, source("source0.example"))

    repeat = await start(client, source("source0.example"))

    assert repeat["id"] == first["id"]

    for index in range(1, MAX_PENDING_JOBS):
        await start(client, source(f"source{index}.example"))

    full = await client.post(PATH, json={"scope": source("blog.example")})

    assert full.status_code == 429
    assert full.json() == {
        "error": QUEUE_FULL, "code": "too_many_requests", "fields": [],
    }
    assert full.headers["cache-control"] == "no-store"
    assert len(await jobs(client)) == MAX_PENDING_JOBS

    release.set()


async def test_queue_keeps_reads_available_and_runs_overlapping_jobs_serially(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    visited: list[str] = []

    async def hold(plan: RefreshPlan, report: Report) -> None:
        visited.append(plan.label)
        entered.set()
        await release.wait()
        report({"status": "completed"})

    worker.script = hold
    await start(client, {"kind": "all"})
    await start(client, {"kind": "reddit"})
    await asyncio.wait_for(entered.wait(), 1)

    assert await statuses(client) == {
        "All sources": "running", "Reddit": "queued",
    }
    assert visited == ["All sources"]

    release.set()
    await finished(manager)

    assert visited == ["All sources", "Reddit"]
    assert await statuses(client) == {
        "All sources": "completed", "Reddit": "completed",
    }


async def test_worker_failure_does_not_stop_next_refresh(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    async def fail_first(plan: RefreshPlan, report: Report) -> None:
        if plan.label == "All sources":
            raise RuntimeError("private database connection details")

        report({"status": "partial", "posts_saved": 2})

    worker.script = fail_first
    await start(client, {"kind": "all"})
    await start(client, {"kind": "reddit"})
    await finished(manager)
    saved = await jobs(client)

    assert [job["status"] for job in saved] == ["partial", "failed"]
    assert saved[0]["posts_saved"] == 2
    assert saved[1]["message"] == (
        "Refresh could not finish. Saved content is still available."
    )
    assert "private" not in json.dumps(saved)
    assert all(job["finished_at"] for job in saved)


async def test_worker_that_exits_without_a_result_fails_its_job(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    async def exit_silently(plan: RefreshPlan, report: Report) -> None:
        report({"status": "processing"})

    worker.script = exit_silently
    await start(client, {"kind": "all"})
    await finished(manager)

    assert await statuses(client) == {"All sources": "failed"}


async def test_worker_reports_reach_the_job(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def report_progress(plan: RefreshPlan, report: Report) -> None:
        report({
            "status": "processing", "message": "Preparing collected content…",
            "run_id": "run", "posts_saved": 3,
        })
        entered.set()
        await release.wait()
        report({"status": "completed", "message": "Refresh complete."})

    worker.script = report_progress
    await start(client, {"kind": "all"})
    await asyncio.wait_for(entered.wait(), 1)
    [running] = await jobs(client)

    assert running["status"] == "processing"
    assert running["message"] == "Preparing collected content…"
    assert running["run_id"] == "run"
    assert running["posts_saved"] == 3
    assert running["finished_at"] is None

    release.set()
    await finished(manager)
    [done] = await jobs(client)

    assert done["status"] == "completed"
    assert done["run_id"] == "run"
    assert done["finished_at"]


async def test_restart_preserves_results_and_marks_unfinished_jobs_interrupted(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    manager: RefreshManager,
    worker: FakeWorker,
) -> None:
    await start(client, source("blog.example"))
    await finished(manager)
    entered = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        entered.set()
        await asyncio.Event().wait()

    worker.script = hold
    await start(client, {"kind": "reddit"})
    await asyncio.wait_for(entered.wait(), 1)

    # A crash leaves the file as it is now. The first manager never closes.
    async with running_refresh(settings, FakeWorker()) as replacement:
        app.dependency_overrides[refresh_queue] = lambda: replacement

        async with http_client(app) as restarted:
            interrupted, completed = await jobs(restarted)

        path = pathlib.Path(settings.data_dir) / "reader-refresh.json"

        assert completed["status"] == "completed"
        assert interrupted["status"] == "interrupted"
        assert interrupted["finished_at"]
        assert interrupted["message"] == (
            "Refresh was interrupted when DANK restarted. "
            "You can start it again."
        )
        assert replacement.pending.empty()
        assert "settings" not in path.read_text()


async def test_shutdown_interrupts_active_and_queued_jobs(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    worker.script = hold
    await start(client, {"kind": "all"})
    await start(client, {"kind": "reddit"})
    await asyncio.wait_for(entered.wait(), 1)
    await manager.close()
    saved = await jobs(client)

    assert stopped.is_set()
    assert [job["status"] for job in saved] == ["interrupted", "interrupted"]
    assert all(job["message"] == "Refresh was interrupted." for job in saved)


async def test_history_retention_keeps_the_newest_jobs(
    client: AsyncClient, manager: RefreshManager,
) -> None:
    for index in range(MAX_HISTORY_JOBS + 5):
        await start(client, source(f"source{index}.example"))
        await finished(manager)

    saved = await jobs(client)

    assert len(saved) == MAX_HISTORY_JOBS
    assert saved[0]["scope"] == source("source24.example")


async def test_history_keeps_long_running_job_while_other_jobs_finish(
    client: AsyncClient, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_then_complete(plan: RefreshPlan, report: Report) -> None:
        if plan.label == "blog.example":
            entered.set()
            await release.wait()

        report({"status": "completed"})

    worker.script = slow_then_complete
    slow = await start(client, source("blog.example"))
    await asyncio.wait_for(entered.wait(), 1)

    for index in range(MAX_HISTORY_JOBS + 5):
        job = await start(client, source(f"source{index}.example"))

        async with asyncio.timeout(1):
            while job["status"] != "completed":
                await asyncio.sleep(0)
                job = next(
                    item for item in await jobs(client)
                    if item["id"] == job["id"]
                )

    saved = await jobs(client)

    assert len(saved) == MAX_HISTORY_JOBS
    assert next(
        job["status"] for job in saved if job["id"] == slow["id"]
    ) == "running"

    release.set()


@pytest.mark.parametrize(("headers", "status", "code"), [
    ({"origin": "https://foreign.example"}, 403, "forbidden"),
    ({"sec-fetch-site": "cross-site"}, 403, "forbidden"),
    ({"content-type": "text/plain"}, 415, "unsupported_media_type"),
])
async def test_post_rejects_untrusted_requests(
    client: AsyncClient, headers: dict[str, str], status: int, code: str,
) -> None:
    response = await client.post(
        PATH,
        content=json.dumps({"scope": {"kind": "all"}}),
        headers={"content-type": "application/json", **headers},
    )

    assert (response.status_code, response.json()["code"]) == (status, code)
    assert await jobs(client) == []


async def test_malformed_json_is_rejected_before_the_origin_is_checked(
    client: AsyncClient,
) -> None:
    # FastAPI parses a JSON body before it runs dependencies. Nothing has
    # happened by then, so the answer changes but the protection doesn't.
    response = await client.post(
        PATH,
        content=b"{not json",
        headers={
            "content-type": "application/json",
            "origin": "https://foreign.example",
        },
    )

    assert (response.status_code, response.json()["code"]) == (
        400, "invalid_request",
    )
    assert await jobs(client) == []


async def test_corrupt_history_starts_empty_and_keeps_working(
    app: FastAPI, settings: Settings,
) -> None:
    path = pathlib.Path(settings.data_dir) / "reader-refresh.json"
    path.write_text("{not json")

    async with running_refresh(settings, FakeWorker()) as manager:
        app.dependency_overrides[refresh_queue] = lambda: manager

        async with http_client(app) as client:
            assert await jobs(client) == []

            await start(client, source("blog.example"))
            await finished(manager)

            assert await statuses(client) == {"blog.example": "completed"}


async def test_saved_history_is_answered_as_it_was_written(
    app: FastAPI, settings: Settings,
) -> None:
    saved = {
        "id": "4cdecd31-4320-4843-9097-7d76b83a80d4",
        "scope": {"kind": "x", "account": "jdm4nc"},
        "label": "@jdm4nc",
        "status": "completed",
        "message": "Refresh complete. Updates are ready to view.",
        "created_at": "2026-10-26T20:13:11.066599+00:00",
        "finished_at": "2026-10-26T20:13:34.714449+00:00",
        "posts_saved": 28,
        "run_id": "55f77cb8-e60b-4be0-8e3d-9130d6d579b7",
    }
    path = pathlib.Path(settings.data_dir) / "reader-refresh.json"
    path.write_text(json.dumps([saved]))

    async with running_refresh(settings, FakeWorker()) as manager:
        app.dependency_overrides[refresh_queue] = lambda: manager

        async with http_client(app) as client:
            assert await jobs(client) == [saved]

    assert json.loads(path.read_text()) == [saved]


@pytest.mark.parametrize(("body", "paths"), [
    ({}, ["body.scope"]),
    ({"scope": {"kind": "all"}, "url": "https://foreign.example"},
     ["body.url"]),
    ({"scope": {}, "url": "https://foreign.example"},
     ["body.scope", "body.url"]),
    ({"scope": None}, ["body.scope"]),
    ({"scope": {"kind": "unsupported"}}, ["body.scope"]),
    ({"scope": {"kind": "all", "community": "python"}},
     ["body.scope.all.community"]),
    ({"scope": {"kind": "all", "domains": "publisher.test"}},
     ["body.scope.all.domains"]),
    ({"scope": {"kind": "post", "domain": "publisher.test"}},
     ["body.scope.post.post_id"]),
    ({"scope": {"kind": "rss", "blog_feed": "unknown"}},
     ["body.scope.rss.blog_feed"]),
    ({"scope": {"kind": "all", "platforms": ["stories"]}},
     ["body.scope.all.platforms.0"]),
    # Closed sets match exactly, unlike aiohttp; see docs/api.md.
    ({"scope": {"kind": "all", "platforms": ["Reddit"]}},
     ["body.scope.all.platforms.0"]),
    ({"scope": {"kind": "rss", "blog_feed": " sources "}},
     ["body.scope.rss.blog_feed"]),
    ([], ["body"]),
])
async def test_malformed_bodies_name_their_fields(
    client: AsyncClient, body: object, paths: list[str],
) -> None:
    response = await client.post(PATH, json=body)

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"
    assert [item["path"] for item in response.json()["fields"]] == paths
    assert await jobs(client) == []


@pytest.mark.parametrize(("scope", "message"), [
    ({"kind": "reddit", "community": "https://reddit.com/r/python"},
     "Invalid Reddit community"),
    ({"kind": "all", "domains": ["unknown.test"]},
     "Refresh includes a source that is no longer configured"),
    ({"kind": "all", "tags": ["unknown"]},
     "Refresh includes a tag that is no longer configured"),
    ({"kind": "x"}, "No configured sources match this refresh context"),
    ({"kind": "post", "domain": "blog.example", "post_id": "unknown"},
     "This post has no saved collection target"),
    ({"kind": "reddit", "community": None},
     "Refresh community must be a non-empty string"),
    # urlsplit's own wording, from a value it can't read.
    ({"kind": "youtube", "channel_id": "http://["}, "Invalid IPv6 URL"),
])
async def test_invalid_scopes_keep_their_wording_in_the_envelope(
    client: AsyncClient, scope: dict[str, Any], message: str,
) -> None:
    response = await client.post(PATH, json={"scope": scope})

    assert (response.status_code, response.json()) == (400, {
        "error": message, "code": "invalid_input", "fields": [],
    })
    assert await jobs(client) == []


async def test_saved_post_can_be_refreshed(
    client: AsyncClient, targets: FakeRefreshTargets,
) -> None:
    targets.posts.add(("linked.test", "article"))
    job = await start(client, {
        "kind": "post", "domain": "linked.test", "post_id": "article",
    })

    assert job["label"] == "Post"
    assert job["scope"] == {
        "kind": "post", "domain": "linked.test", "post_id": "article",
    }


async def test_https_proxy_requests_still_require_matching_origin(
    client: AsyncClient,
) -> None:
    # What the proxy sends once it has checked the https:// origin.
    proxied = {"host": "dank.local", "x-forwarded-proto": "https"}
    accepted = await client.post(
        PATH,
        json={"scope": {"kind": "all"}},
        headers={**proxied, "origin": "http://dank.local"},
    )
    foreign = await client.post(
        PATH,
        json={"scope": {"kind": "reddit"}},
        headers={**proxied, "origin": "https://evil.local"},
    )

    assert accepted.status_code == 202
    assert foreign.status_code == 403
    assert [job["id"] for job in await jobs(client)] == [
        accepted.json()["job"]["id"],
    ]


async def test_independent_sources_run_together_without_waiting(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    domains = ("blog.example", "source0.example")
    entered = {domain: asyncio.Event() for domain in domains}
    release = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        entered[plan.label].set()
        await release.wait()
        report({"status": "completed"})

    worker.script = hold

    for domain in domains:
        await start(client, source(domain))

    await asyncio.wait_for(asyncio.gather(
        *(event.wait() for event in entered.values()),
    ), 1)

    assert set((await statuses(client)).values()) == {"running"}

    release.set()
    await finished(manager)


async def test_overlap_waits_but_unrelated_work_passes_the_queue(
    client: AsyncClient,
    manager: RefreshManager,
    targets: FakeRefreshTargets,
    worker: FakeWorker,
) -> None:
    targets.posts.add(("reddit.com", "one"))
    first_entered = asyncio.Event()
    other_entered = asyncio.Event()
    release = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        (first_entered if plan.label == "Reddit" else other_entered).set()
        await release.wait()
        report({"status": "completed"})

    worker.script = hold
    await start(client, {"kind": "reddit"})
    await asyncio.wait_for(first_entered.wait(), 1)
    queued = await start(client, {
        "kind": "post", "domain": "reddit.com", "post_id": "one",
    })
    await start(client, source("blog.example"))
    await asyncio.wait_for(other_entered.wait(), 1)
    current = {job["label"]: job for job in await jobs(client)}

    assert current["Reddit"]["status"] == "running"
    assert current["blog.example"]["status"] == "running"
    assert current["Post"]["id"] == queued["id"]
    assert current["Post"]["status"] == "queued"
    assert current["Post"]["message"] == (
        "Waiting for Reddit to finish; these refreshes share a source."
    )

    release.set()
    await finished(manager)


async def test_queued_all_is_a_barrier_and_cannot_be_starved(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    labels = ("blog.example", "All sources", "source0.example")
    entered = {label: asyncio.Event() for label in labels}
    release = {label: asyncio.Event() for label in labels}

    async def hold(plan: RefreshPlan, report: Report) -> None:
        entered[plan.label].set()
        await release[plan.label].wait()
        report({"status": "completed"})

    worker.script = hold
    await start(client, source("blog.example"))
    await asyncio.wait_for(entered["blog.example"].wait(), 1)
    await start(client, {"kind": "all"})
    await start(client, source("source0.example"))
    await asyncio.sleep(0)
    current = await statuses(client)

    assert current["blog.example"] == "running"
    assert current["All sources"] == current["source0.example"] == "queued"

    release["blog.example"].set()
    await asyncio.wait_for(entered["All sources"].wait(), 1)
    current = await statuses(client)

    assert current["All sources"] == "running"
    assert current["source0.example"] == "queued"

    release["All sources"].set()
    await asyncio.wait_for(entered["source0.example"].wait(), 1)
    release["source0.example"].set()
    await finished(manager)


async def test_parallel_workers_are_bounded_and_cancel_together(
    client: AsyncClient, manager: RefreshManager, worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    running = 0
    cancelled = 0

    async def hold(plan: RefreshPlan, report: Report) -> None:
        nonlocal running, cancelled
        running += 1

        if running == MAX_CONCURRENT_JOBS:
            entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            cancelled += 1

    worker.script = hold

    for index in range(MAX_PENDING_JOBS):
        await start(client, source(f"source{index}.example"))

    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.sleep(0)

    assert running == MAX_CONCURRENT_JOBS
    assert list((await statuses(client)).values()).count("queued") == (
        MAX_PENDING_JOBS - MAX_CONCURRENT_JOBS
    )

    await asyncio.wait_for(manager.close(), 1)

    assert cancelled == MAX_CONCURRENT_JOBS
    assert set((await statuses(client)).values()) == {"interrupted"}

    await finished(manager)


async def test_external_collection_busy_retries_without_failing_job(
    app: FastAPI, settings: Settings,
) -> None:
    worker = FakeWorker()
    attempts = 0

    async def busy_once(plan: RefreshPlan, report: Report) -> None:
        nonlocal attempts
        attempts += 1
        report({
            "status": "queued" if attempts == 1 else "completed",
            "message": (
                "Another collection is running" if attempts == 1 else "Done"
            ),
        })

    worker.script = busy_once

    async with running_refresh(
        settings, worker, busy_retry_seconds=0.01,
    ) as manager:
        app.dependency_overrides[refresh_queue] = lambda: manager

        async with http_client(app) as client:
            job = await start(client, source("blog.example"))
            await finished(manager)
            saved = await jobs(client)

    assert attempts == 2
    assert [item["id"] for item in saved] == [job["id"]]
    assert saved[0]["status"] == "completed"


async def test_close_during_external_retry_drains_waiting_and_running_jobs(
    app: FastAPI, settings: Settings, targets: FakeRefreshTargets,
) -> None:
    targets.posts.add(("blog.example", "one"))
    worker = FakeWorker()
    busy = asyncio.Event()
    independent = asyncio.Event()

    async def busy_or_hold(plan: RefreshPlan, report: Report) -> None:
        if plan.label == "blog.example":
            report({"status": "queued", "message": "Busy"})
            busy.set()

            return

        independent.set()
        await asyncio.Event().wait()

    worker.script = busy_or_hold

    async with running_refresh(
        settings, worker, busy_retry_seconds=60,
    ) as manager:
        app.dependency_overrides[refresh_queue] = lambda: manager

        async with http_client(app) as client:
            await start(client, source("blog.example"))
            await asyncio.wait_for(busy.wait(), 1)
            await start(client, {
                "kind": "post", "domain": "blog.example", "post_id": "one",
            })
            await start(client, source("source0.example"))
            await asyncio.wait_for(independent.wait(), 1)
            current = await statuses(client)

            assert current["source0.example"] == "running"
            assert list(current.values()).count("queued") == 2

            await asyncio.wait_for(manager.close(), 1)
            await finished(manager)

            assert not manager.running
            assert not manager.waiting
            assert not manager.retry_after
            assert set((await statuses(client)).values()) == {"interrupted"}


async def test_head_and_cache_headers_follow_the_platform(
    client: AsyncClient,
) -> None:
    get = await client.get(PATH)
    head = await client.head(PATH)

    assert get.headers["cache-control"] == "no-store"
    assert (head.status_code, head.content) == (200, b"")


async def test_openapi_describes_the_scope_kinds_and_statuses(
    client: AsyncClient,
) -> None:
    document = (await client.get("/api/openapi.json")).json()
    schemas = document["components"]["schemas"]
    post = document["paths"][PATH]["post"]

    assert set(post["responses"]) >= {"202"}
    assert set(schemas["RefreshRequest"]["properties"]) == {"scope"}
    assert schemas["RefreshRequest"]["required"] == ["scope"]
    assert set(
        schemas["RefreshRequest"]["properties"]["scope"]["discriminator"][
            "mapping"
        ],
    ) == {"all", "rss", "reddit", "x", "hacker_news", "youtube", "post"}
    assert set(schemas["RefreshJobOut"]["properties"]["status"]["enum"]) == {
        "queued", "running", "processing", "completed", "partial", "failed",
        "interrupted",
    }


async def test_the_switch_collects_enabled_saved_sources_and_no_others(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    manager: RefreshManager,
    worker: FakeWorker,
) -> None:
    collected: list[list[str]] = []

    async def record(plan: RefreshPlan, report: Report) -> None:
        collected.append([item.domain for item in plan.settings.sources])
        report({"status": "completed"})

    worker.script = record
    switch_to(app, settings, FakeSourceRepository(
        saved(1, "saved.example"), saved(2, "off.example", enabled=False),
    ))

    await start(client, {"kind": "all"})
    await finished(manager)
    disabled = await client.post(PATH, json={"scope": source("off.example")})
    config_only = await client.post(
        PATH, json={"scope": source("blog.example")},
    )

    assert collected == [["saved.example"]]
    assert (disabled.status_code, config_only.status_code) == (400, 400)
    assert disabled.json()["error"] == (
        "Refresh includes a source that is no longer configured"
    )


async def test_jobs_keep_their_sources_and_a_request_after_an_edit_is_new(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    manager: RefreshManager,
    worker: FakeWorker,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    collected: list[list[tuple[str, tuple[str, ...]]]] = []

    async def hold(plan: RefreshPlan, report: Report) -> None:
        collected.append([
            (item.domain, item.accounts) for item in plan.settings.sources
        ])
        entered.set()
        await release.wait()
        report({"status": "completed"})

    x = Source(
        uuid.UUID(int=1), enabled=True, domain="x.com",
        accounts=(Target("alice"), Target("bob")),
    )
    repository = FakeSourceRepository(x, saved(2, "blog.example"))
    switch_to(app, settings, repository)
    worker.script = hold
    running = await start(client, {"kind": "all"})
    await asyncio.wait_for(entered.wait(), 1)
    waiting = await start(client, {"kind": "x"})

    await repository.update(x._replace(
        accounts=(Target("alice"), Target("bob", enabled=False)),
    ))
    await repository.update(saved(2, "blog.example", enabled=False))
    after = await start(client, {"kind": "all"})
    after_x = await start(client, {"kind": "x"})
    repeat = await start(client, {"kind": "all"})

    assert len({
        running["id"], waiting["id"], after["id"], after_x["id"],
    }) == 4
    assert repeat["id"] == after["id"]

    release.set()
    await finished(manager)

    assert collected == [
        [("x.com", ("alice", "bob")), ("blog.example", ())],
        [("x.com", ("alice", "bob"))],
        [("x.com", ("alice",))],
        [("x.com", ("alice",))],
    ]


async def test_an_edit_to_another_source_still_joins_the_active_job(
    app: FastAPI,
    client: AsyncClient,
    settings: Settings,
    worker: FakeWorker,
) -> None:
    release = asyncio.Event()

    async def hold(plan: RefreshPlan, report: Report) -> None:
        await release.wait()
        report({"status": "completed"})

    repository = FakeSourceRepository(
        saved(1, "blog.example"), saved(2, "other.example"),
    )
    switch_to(app, settings, repository)
    worker.script = hold
    first = await start(client, source("blog.example"))

    await repository.update(saved(2, "other.example", enabled=False))
    repeat = await start(client, source("blog.example"))

    assert repeat["id"] == first["id"]

    release.set()
