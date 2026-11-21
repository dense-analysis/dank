"""The real scheduler for route tests, with fakes behind its ports."""

import pathlib
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI

from dank.api.refresh.deps import refresh_queue, refresh_targets
from dank.api.refresh.manager import RefreshManager, running_refresh
from dank.config import Settings, SourceConfig
from tests.api.refresh.fakes import FakeRefreshTargets, FakeWorker


@pytest.fixture
def settings(settings: Settings, memory_path: pathlib.Path) -> Settings:
    return settings._replace(
        data_dir=memory_path,
        sources=(
            SourceConfig("blog.example", ()),
            SourceConfig("news.ycombinator.com", ()),
            SourceConfig("youtube.com", ("Alice",)),
            SourceConfig("reddit.com", (), feed_urls=(
                "https://www.reddit.com/r/Python/",
            )),
            *(
                SourceConfig(f"source{index}.example", ())
                for index in range(30)
            ),
        ),
    )


@pytest.fixture
def targets() -> FakeRefreshTargets:
    return FakeRefreshTargets()


@pytest.fixture
def worker() -> FakeWorker:
    return FakeWorker()


@pytest.fixture
async def manager(
    settings: Settings, worker: FakeWorker,
) -> AsyncIterator[RefreshManager]:
    async with running_refresh(settings, worker) as manager:
        yield manager


@pytest.fixture
def app(
    app: FastAPI, targets: FakeRefreshTargets, manager: RefreshManager,
) -> FastAPI:
    app.dependency_overrides[refresh_targets] = lambda: targets
    app.dependency_overrides[refresh_queue] = lambda: manager

    return app
