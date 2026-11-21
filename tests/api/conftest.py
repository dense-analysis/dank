"""The real app over httpx, with driven adapters supplied by test fixtures."""

from __future__ import annotations

import pathlib
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from fastapi import FastAPI

from dank.api import deps
from dank.api.app import create_app
from dank.config import (
    BrowserSettings,
    ClickHouseSettings,
    LoggingSettings,
    PostgresSettings,
    Settings,
    SourceConfig,
    XSettings,
)
from tests.api.client import http_client


@pytest.fixture(scope="session")
def api_settings() -> Settings:
    return Settings(
        clickhouse=ClickHouseSettings(
            "database.test", 8123, "dank", "default", "",
            secure=False, use_http=True,
        ),
        x=XSettings(),
        data_dir=pathlib.Path("/fixture/data"),
        max_asset_bytes=10 * 1024 * 1024,
        feed_staleness_days=14,
        browser=BrowserSettings(None, None, None),
        logging=LoggingSettings(pathlib.Path("/fixture/dank.log"), "INFO"),
        postgres=PostgresSettings(enabled=False),
        sources=(
            SourceConfig("politics.example", (), tags=("politics",)),
            SourceConfig("empty.example", (), tags=("culture",)),
        ),
    )


@pytest.fixture
def settings(api_settings: Settings) -> Settings:
    return api_settings


@pytest.fixture(scope="session")
def route_app(api_settings: Settings) -> FastAPI:
    return create_app(api_settings)


@pytest.fixture
def app(route_app: FastAPI, settings: Settings) -> Iterator[FastAPI]:
    # ASGITransport doesn't run the lifespan, so nothing connects to
    # ClickHouse. Feature conftests swap their driven adapters for fakes.
    route_app.dependency_overrides[deps.settings] = lambda: settings

    try:
        yield route_app
    finally:
        route_app.dependency_overrides.clear()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with http_client(app) as client:
        yield client
