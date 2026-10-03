import asyncio
import logging

import pytest

from dank.progress import Progress


async def test_progress_reports_while_work_is_pending_and_stops_after_exit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="dank.progress")

    progress = Progress("Fetching test articles", 2, interval=0.005)

    async with progress:
        progress.advance()
        await asyncio.sleep(0.025)
        assert any("1/2 completed" in row.message for row in caplog.records)
        progress.advance()

    assert "2/2 completed" in caplog.records[-1].message
    count = len(caplog.records)
    await asyncio.sleep(0.025)
    assert len(caplog.records) == count


async def test_cancelled_stage_is_not_reported_as_finished(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="dank.progress")
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def wait_forever() -> None:
        async with Progress("Discovering test feeds", interval=0.005):
            entered.set()
            await blocked.wait()

    task = asyncio.create_task(wait_forever())
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert any("stopped" in row.message for row in caplog.records)
    assert not any("finished" in row.message for row in caplog.records)
    count = len(caplog.records)
    await asyncio.sleep(0.02)
    assert len(caplog.records) == count
