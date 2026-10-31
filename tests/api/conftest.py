"""The app and an HTTP client for every API test, and the ClickHouse lane.

Route tests use the real app over httpx and swap only driven adapters.
Tests marked clickhouse get a throwaway database on the server named by
--clickhouse-url, and are skipped without it.
"""

from __future__ import annotations

import pathlib
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI

from dank.api import deps
from dank.api.app import create_app
from dank.config import (
    ClickHouseSettings,
    Settings,
    SourceConfig,
    load_settings,
)
from dank.storage.clickhouse import ClickHouseClient
from dank.web.startup import prepare_clickhouse
from tests.api.clickhouse import schema_statements, server_settings
from tests.api.client import http_client

ROOT = pathlib.Path(__file__).parents[2]


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Settings:
    return load_settings(ROOT / "config.example.toml")._replace(
        data_dir=tmp_path,
        sources=(
            SourceConfig("politics.example", (), tags=("politics",)),
            SourceConfig("empty.example", (), tags=("culture",)),
        ),
    )


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    app = create_app(settings, page_size=50)
    # ASGITransport doesn't run the lifespan, so nothing connects to
    # ClickHouse. Feature conftests swap their driven adapters for fakes.
    app.dependency_overrides[deps.settings] = lambda: settings

    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with http_client(app) as client:
        yield client


@pytest.fixture
async def clickhouse_settings(
    request: pytest.FixtureRequest,
) -> AsyncIterator[ClickHouseSettings]:
    """A throwaway database with schema.sql applied, dropped afterwards."""
    url = str(request.config.getoption("clickhouse_url"))

    if not url:
        pytest.skip("needs --clickhouse-url")

    server = server_settings(url)
    database = "dank_test_" + uuid.uuid4().hex[:12]

    async with ClickHouseClient(server) as admin:
        await admin.execute(f"CREATE DATABASE {database}")

        try:
            throwaway = server._replace(database=database)

            async with ClickHouseClient(throwaway) as client:
                for statement in schema_statements(database):
                    await client.execute(statement)

            yield throwaway
        finally:
            await admin.execute(f"DROP DATABASE IF EXISTS {database} SYNC")


@pytest.fixture
async def clickhouse(
    clickhouse_settings: ClickHouseSettings,
) -> AsyncIterator[ClickHouseClient]:
    """A client for the throwaway database, after the servers' startup."""
    async with ClickHouseClient(clickhouse_settings) as client:
        await prepare_clickhouse(client)

        yield client
