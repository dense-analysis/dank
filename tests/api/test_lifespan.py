"""Exercise the application's lifespan with an in-memory driver."""

from pathlib import Path

import clickhouse_connect
import pytest

from dank.api import lifespan
from dank.api.app import create_app
from dank.api.lifespan import Resources
from dank.config import Settings
from dank.storage.clickhouse import ClickHouseClient


class Driver:
    closed = 0

    async def close(self) -> None:
        self.closed += 1


class Prepare:
    def __init__(self) -> None:
        self.clients: list[ClickHouseClient] = []
        self.error: RuntimeError | None = None

    async def __call__(self, client: ClickHouseClient) -> None:
        self.clients.append(client)

        if self.error is not None:
            raise self.error


@pytest.fixture
def driver(monkeypatch: pytest.MonkeyPatch) -> Driver:
    driver = Driver()

    async def connect(**_options: object) -> Driver:
        return driver

    monkeypatch.setattr(clickhouse_connect, "get_async_client", connect)

    return driver


@pytest.fixture
def prepare(monkeypatch: pytest.MonkeyPatch) -> Prepare:
    prepare = Prepare()
    monkeypatch.setattr(lifespan, "prepare_clickhouse", prepare)

    return prepare


async def test_startup_prepares_storage_and_shutdown_stops_refresh(
    settings: Settings,
    memory_path: Path,
    driver: Driver,
    prepare: Prepare,
) -> None:
    app = create_app(settings._replace(data_dir=memory_path))

    async with app.router.lifespan_context(app) as state:
        assert state is not None
        resources: Resources = state["resources"]
        assert prepare.clients == [resources.clickhouse]
        assert resources.assets_dir.is_dir()
        assert resources.source_icons.directory.parent == resources.assets_dir
        scheduler = resources.refresh.scheduler
        assert scheduler is not None
        assert not scheduler.done()
        assert driver.closed == 0

    assert scheduler.done()
    assert driver.closed == 1


async def test_failed_startup_closes_the_driver(
    settings: Settings,
    memory_path: Path,
    driver: Driver,
    prepare: Prepare,
) -> None:
    prepare.error = RuntimeError("migration failed")
    app = create_app(settings._replace(data_dir=memory_path))

    with pytest.raises(RuntimeError, match="migration failed"):
        async with app.router.lifespan_context(app):
            pytest.fail("app started even though its schema was unavailable")

    assert driver.closed == 1


async def test_each_running_app_owns_its_collection_gates(
    settings: Settings,
    memory_path: Path,
    driver: Driver,
    prepare: Prepare,
) -> None:
    configured = settings._replace(data_dir=memory_path)
    first = create_app(configured)
    second = create_app(configured)

    async with (
        first.router.lifespan_context(first) as one,
        second.router.lifespan_context(second) as other,
    ):
        assert one is not None
        assert other is not None
        ours: Resources = one["resources"]
        theirs: Resources = other["resources"]

        assert ours.metadata_gate is not ours.transcript_gate
        assert ours.metadata_gate.lock is not theirs.metadata_gate.lock
        assert ours.metadata_gate.limiter is not theirs.metadata_gate.limiter
        assert ours.transcript_gate.lock is not theirs.transcript_gate.lock
        assert (
            ours.transcript_gate.limiter is not theirs.transcript_gate.limiter
        )
