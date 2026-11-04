"""The real scheduler for route tests, with fakes behind its ports."""

import multiprocessing
import pathlib
from collections.abc import AsyncIterator, Iterator

import pytest
from fastapi import FastAPI

from dank.api.refresh.deps import refresh_queue, refresh_targets
from dank.api.refresh.manager import RefreshManager, running_refresh
from dank.config import Settings, SourceConfig
from tests.api.refresh.fakes import FakeRefreshTargets, FakeWorker
from tests.api.refresh.processes import serve_feeds


@pytest.fixture
def settings(settings: Settings) -> Settings:
    return settings._replace(sources=(
        SourceConfig("blog.example", ()),
        SourceConfig("news.ycombinator.com", ()),
        SourceConfig("youtube.com", ("Alice",)),
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/Python/",
        )),
        *(SourceConfig(f"source{index}.example", ()) for index in range(30)),
    ))


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


@pytest.fixture
def feed_port(tmp_path: pathlib.Path) -> Iterator[int]:
    """A feed server on this machine, for jobs run by real workers."""
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    server = context.Process(
        target=serve_feeds,
        args=(send, str(tmp_path / "feed-entered")),
        daemon=True,
    )
    server.start()

    try:
        assert receive.poll(30)

        yield receive.recv()
    finally:
        server.kill()
        server.join()
