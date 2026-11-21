import asyncio
import logging
from types import SimpleNamespace

import pytest

from dank import progress as progress_module
from dank.progress import Progress

HEARTBEAT_INTERVAL = 0.005


@pytest.fixture
def heartbeat_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> asyncio.Queue[asyncio.Future[None]]:
    waits: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue()

    async def sleep(delay: float) -> None:
        assert delay == HEARTBEAT_INTERVAL
        timer = asyncio.get_running_loop().create_future()
        waits.put_nowait(timer)
        await timer

    monkeypatch.setattr(progress_module, "asyncio", SimpleNamespace(
        sleep=sleep,
        create_task=asyncio.create_task,
        CancelledError=asyncio.CancelledError,
    ))

    return waits


async def test_progress_reports_while_work_is_pending_and_stops_after_exit(
    caplog: pytest.LogCaptureFixture,
    heartbeat_waits: asyncio.Queue[asyncio.Future[None]],
) -> None:
    caplog.set_level(logging.INFO, logger="dank.progress")

    progress = Progress(
        "Fetching test articles", 2, interval=HEARTBEAT_INTERVAL,
    )

    async with asyncio.timeout(1), progress:
        progress.advance()
        first = await heartbeat_waits.get()
        first.set_result(None)
        following = await heartbeat_waits.get()

        assert any("1/2 completed" in row.message for row in caplog.records)
        progress.advance()

    assert "2/2 completed" in caplog.records[-1].message
    assert following.cancelled()


async def test_cancelled_stage_is_not_reported_as_finished(
    caplog: pytest.LogCaptureFixture,
    heartbeat_waits: asyncio.Queue[asyncio.Future[None]],
) -> None:
    caplog.set_level(logging.INFO, logger="dank.progress")
    blocked = asyncio.Event()

    async def wait_forever() -> None:
        async with Progress(
            "Discovering test feeds", interval=HEARTBEAT_INTERVAL,
        ):
            await blocked.wait()

    async with asyncio.timeout(1):
        task = asyncio.create_task(wait_forever())
        timer = await heartbeat_waits.get()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert any("stopped" in row.message for row in caplog.records)
    assert not any("finished" in row.message for row in caplog.records)
    assert timer.cancelled()
