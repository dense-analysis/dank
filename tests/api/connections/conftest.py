"""A fake behind the ConnectionStore port for connections route tests."""

import pytest
from fastapi import FastAPI

from dank.api.connections.deps import connection_store
from tests.api.connections.fakes import FakeConnectionStore


@pytest.fixture
def store() -> FakeConnectionStore:
    return FakeConnectionStore()


@pytest.fixture
def app(app: FastAPI, store: FakeConnectionStore) -> FastAPI:
    app.dependency_overrides[connection_store] = lambda: store

    return app
