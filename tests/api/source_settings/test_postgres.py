"""The routes over the real repository and table."""

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import text

from dank.api.source_settings.deps import source_repository
from dank.config import PostgresSettings
from dank.db.engine import async_engine, async_sessions
from dank.db.sources import PostgresSourceRepository
from tests.api.client import BASE_URL

pytestmark = pytest.mark.postgres

PATH = "/api/reader/source-settings"
SAME_ORIGIN = {"origin": BASE_URL}
BODY = {
    "enabled": True,
    "domain": "x.com",
    "accounts": [{"value": "someone", "enabled": True}],
    "feeds": [],
    "tags": [],
    "adapters": None,
    "reddit": None,
    "hacker_news": None,
}


@pytest.fixture
async def app(
    app: FastAPI, postgres_settings: PostgresSettings,
) -> AsyncIterator[FastAPI]:
    engine = async_engine(postgres_settings)

    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE sources RESTART IDENTITY"))

    repository = PostgresSourceRepository(async_sessions(engine))
    app.dependency_overrides[source_repository] = lambda: repository

    yield app

    await engine.dispose()


async def test_sources_are_saved_listed_and_edited(
    client: AsyncClient,
) -> None:
    added = await client.post(PATH, json=BODY, headers=SAME_ORIGIN)
    again = await client.post(PATH, json=BODY, headers=SAME_ORIGIN)
    source_id = added.json()["id"]
    edited = await client.put(
        f"{PATH}/{source_id}", json={**BODY, "enabled": False},
        headers=SAME_ORIGIN,
    )

    assert added.status_code == 201
    assert (again.status_code, again.json()["code"]) == (
        409, "duplicate_source",
    )
    assert edited.json() == {**added.json(), "enabled": False}
    assert (await client.get(PATH)).json() == {"sources": [edited.json()]}
    assert (await client.get(f"{PATH}/{source_id}")).json() == edited.json()
