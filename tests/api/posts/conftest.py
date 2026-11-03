"""Fakes behind the posts feature's ports for its route tests."""

import pytest
from fastapi import FastAPI

from dank.api.posts.deps import clock, post_store, search_embeddings
from tests.api.posts.fakes import NOW, FakePostStore, FakeSearchEmbeddings


@pytest.fixture
def store() -> FakePostStore:
    return FakePostStore()


@pytest.fixture
def embeddings() -> FakeSearchEmbeddings:
    return FakeSearchEmbeddings()


@pytest.fixture
def app(
    app: FastAPI, store: FakePostStore, embeddings: FakeSearchEmbeddings,
) -> FastAPI:
    # Only the driven adapters are swapped. The router, service, domain and
    # config catalog under test are the real ones.
    app.dependency_overrides[post_store] = lambda: store
    app.dependency_overrides[search_embeddings] = lambda: embeddings
    app.dependency_overrides[clock] = lambda: lambda: NOW

    return app
