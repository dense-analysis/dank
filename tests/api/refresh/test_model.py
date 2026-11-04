"""The rules for a refresh job's history."""

from typing import Any

import pytest

from dank.api.refresh.model import (
    MAX_HISTORY_JOBS,
    RESTART_MESSAGE,
    RefreshJob,
    restored_jobs,
    retained_jobs,
)

NOW = "2026-11-04T00:00:00+00:00"


def saved(**fields: Any) -> dict[str, Any]:
    """A job as reader-refresh.json holds it."""
    return {
        "id": "one",
        "scope": {"kind": "x", "account": "alice"},
        "label": "@alice",
        "status": "completed",
        "message": "Refresh complete. Updates are ready to view.",
        "created_at": "2026-10-26T20:13:11.066599+00:00",
        "finished_at": "2026-10-26T20:13:34.714449+00:00",
        "posts_saved": 28,
        "run_id": "55f77cb8-e60b-4be0-8e3d-9130d6d579b7",
        **fields,
    }


def job(index: int, status: str = "completed") -> RefreshJob:
    return RefreshJob(**saved(id=str(index), status=status))


def test_finished_jobs_come_back_as_they_were() -> None:
    assert restored_jobs([saved()], NOW) == (RefreshJob(**saved()),)


@pytest.mark.parametrize("status", ["queued", "running", "processing"])
def test_active_jobs_come_back_interrupted(status: str) -> None:
    [restored] = restored_jobs(
        [saved(status=status, finished_at=None)], NOW,
    )

    assert restored.status == "interrupted"
    assert restored.message == RESTART_MESSAGE
    assert restored.finished_at == NOW


@pytest.mark.parametrize("item", [
    "not a job",
    {"id": "only"},
    saved(id=1),
    saved(label=None),
    saved(message=None),
    saved(created_at=None),
    saved(status="cancelled"),
    saved(status=["completed"]),
    saved(scope="all"),
    saved(scope={"kind": "stories"}),
    saved(scope={"kind": "all", "community": "python"}),
    saved(finished_at=1),
    saved(posts_saved="28"),
    saved(posts_saved=-1),
    saved(posts_saved=True),
    saved(run_id=1),
])
def test_jobs_the_api_cannot_describe_are_left_out(item: object) -> None:
    assert restored_jobs([item, saved(id="kept")], NOW) == (
        RefreshJob(**saved(id="kept")),
    )


def test_saved_fields_the_api_does_not_know_are_ignored() -> None:
    assert restored_jobs([saved(extra="field")], NOW) == (
        RefreshJob(**saved()),
    )


def test_only_the_newest_saved_jobs_come_back() -> None:
    items = [saved(id=str(index)) for index in range(MAX_HISTORY_JOBS + 5)]
    restored = restored_jobs(items, NOW)

    assert len(restored) == MAX_HISTORY_JOBS
    assert restored[-1].id == str(MAX_HISTORY_JOBS + 4)


def test_history_that_is_not_a_list_is_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid refresh history"):
        restored_jobs({"jobs": []}, NOW)


def test_retention_keeps_the_newest_finished_jobs() -> None:
    jobs = [job(index) for index in range(MAX_HISTORY_JOBS + 5)]

    assert retained_jobs(jobs) == tuple(jobs[5:])


def test_retention_never_drops_an_active_job() -> None:
    active = job(0, "running")
    jobs = [active, *(job(index) for index in range(1, MAX_HISTORY_JOBS + 5))]
    retained = retained_jobs(jobs)

    assert retained[0] == active
    assert len(retained) == MAX_HISTORY_JOBS
    assert retained[-1] == jobs[-1]
