"""Fakes behind the source icon feature's ports for its route tests."""

import pytest
from fastapi import FastAPI

from dank.api.source_icons.deps import collected_domains, icon_cache
from tests.api.source_icons.fakes import FakeCollectedDomains, FakeIconCache


@pytest.fixture
def collected() -> FakeCollectedDomains:
    return FakeCollectedDomains()


@pytest.fixture
def icons() -> FakeIconCache:
    return FakeIconCache()


@pytest.fixture
def app(
    app: FastAPI, collected: FakeCollectedDomains, icons: FakeIconCache,
) -> FastAPI:
    app.dependency_overrides[collected_domains] = lambda: collected
    app.dependency_overrides[icon_cache] = lambda: icons

    return app
