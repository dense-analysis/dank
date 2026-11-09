"""The in-memory repository behind the source settings routes."""

import pytest
from fastapi import FastAPI

from dank.api.source_settings.deps import source_repository
from tests.sources.fakes import FakeSourceRepository


@pytest.fixture
def repository() -> FakeSourceRepository:
    return FakeSourceRepository()


@pytest.fixture
def app(app: FastAPI, repository: FakeSourceRepository) -> FastAPI:
    # Only the Postgres adapter is swapped. The router, service and model
    # under test are the real ones.
    app.dependency_overrides[source_repository] = lambda: repository

    return app
