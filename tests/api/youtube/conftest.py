"""Fakes behind the YouTube feature's ports for its route tests."""

import asyncio

import pytest
from fastapi import FastAPI

from dank.api.posts.deps import clock
from dank.api.youtube.deps import (
    collection_limits,
    collection_locks,
    video_collector,
    youtube_store,
)
from dank.api.youtube.service import CollectionLimits, CollectionLocks
from tests.api.youtube.fakes import NOW, FakeVideoCollector, FakeYouTubeStore


@pytest.fixture
def store() -> FakeYouTubeStore:
    return FakeYouTubeStore()


@pytest.fixture
def collector() -> FakeVideoCollector:
    return FakeVideoCollector()


@pytest.fixture
def locks() -> CollectionLocks:
    return CollectionLocks(asyncio.Lock(), asyncio.Lock())


@pytest.fixture
def app(
    app: FastAPI,
    store: FakeYouTubeStore,
    collector: FakeVideoCollector,
    locks: CollectionLocks,
) -> FastAPI:
    # Only the driven adapters are swapped. The router, service, domain and
    # config catalog under test are the real ones.
    app.dependency_overrides[youtube_store] = lambda: store
    app.dependency_overrides[video_collector] = lambda: collector
    app.dependency_overrides[collection_locks] = lambda: locks
    app.dependency_overrides[clock] = lambda: lambda: NOW
    # Long enough that only a test holding a collection open runs them out.
    app.dependency_overrides[collection_limits] = lambda: CollectionLimits(
        metadata_wait=5, transcript_lock_wait=5, transcript_lookup=5,
    )

    return app
