"""A fake behind the CollectionStats port for sources route tests."""

import pytest
from fastapi import FastAPI

from dank.api.sources.deps import collection_stats
from tests.api.sources.fakes import FakeCollectionStats


@pytest.fixture
def stats() -> FakeCollectionStats:
    return FakeCollectionStats()


@pytest.fixture
def app(app: FastAPI, stats: FakeCollectionStats) -> FastAPI:
    # Only the ClickHouse adapter is swapped. The router, service, domain
    # and config catalog under test are the real ones.
    app.dependency_overrides[collection_stats] = lambda: stats

    return app
