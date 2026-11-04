"""A fake behind the ViewerStore port for the viewer's route tests."""

import pytest
from fastapi import FastAPI

from dank.api.viewer.deps import page_size, viewer_store
from tests.api.viewer.fakes import FakeViewerStore


@pytest.fixture
def store() -> FakeViewerStore:
    return FakeViewerStore()


@pytest.fixture
def app(app: FastAPI, store: FakeViewerStore) -> FastAPI:
    app.dependency_overrides[viewer_store] = lambda: store
    app.dependency_overrides[page_size] = lambda: 50

    return app
