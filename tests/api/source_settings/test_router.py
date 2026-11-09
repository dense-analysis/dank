"""/api/reader/source-settings: HTTP in, JSON out."""

import uuid
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.source_settings.deps import source_repository
from dank.sources.model import Source, StoredSourceError
from tests.api.client import BASE_URL, http_client
from tests.sources.fakes import FakeSourceRepository

PATH = "/api/reader/source-settings"
SAME_ORIGIN = {"origin": BASE_URL}


def body(**changes: Any) -> dict[str, Any]:
    return {
        "enabled": True,
        "domain": "reddit.com",
        "accounts": [],
        "feeds": [{"value": "r/LocalLLaMA", "enabled": True}],
        "tags": ["AI"],
        "adapters": None,
        "reddit": {"sort": "top", "time_filter": "day"},
        "hacker_news": None,
        **changes,
    }


async def add(client: AsyncClient, **changes: Any) -> dict[str, Any]:
    response = await client.post(
        PATH, json=body(**changes), headers=SAME_ORIGIN,
    )
    assert response.status_code == 201

    return response.json()


async def test_there_are_no_sources_until_one_is_added(
    client: AsyncClient,
) -> None:
    response = await client.get(PATH)

    assert (response.status_code, response.json()) == (200, {"sources": []})
    assert response.headers["cache-control"] == "no-store"


async def test_adding_answers_the_normalized_source_with_its_id(
    client: AsyncClient,
) -> None:
    response = await client.post(PATH, json=body(), headers=SAME_ORIGIN)
    saved = response.json()

    assert response.status_code == 201
    assert saved == {
        "id": saved["id"],
        "enabled": True,
        "domain": "reddit.com",
        "accounts": [],
        "feeds": [
            {"value": "https://www.reddit.com/r/LocalLLaMA/", "enabled": True},
        ],
        "tags": ["ai"],
        "adapters": None,
        "reddit": {"sort": "top", "time_filter": "day"},
        "hacker_news": None,
    }
    assert uuid.UUID(saved["id"]).version == 4
    assert (await client.get(PATH)).json() == {"sources": [saved]}
    assert (await client.get(f"{PATH}/{saved['id']}")).json() == saved


async def test_editing_replaces_the_whole_source(client: AsyncClient) -> None:
    saved = await add(client)
    edit = body(
        enabled=False,
        feeds=[
            {"value": "r/LocalLLaMA", "enabled": False},
            {"value": "r/programming", "enabled": True},
        ],
        tags=[],
    )

    response = await client.put(
        f"{PATH}/{saved['id']}", json=edit, headers=SAME_ORIGIN,
    )
    again = await client.put(
        f"{PATH}/{saved['id']}", json=edit, headers=SAME_ORIGIN,
    )

    assert response.status_code == 200
    assert response.json() == again.json() == {
        **saved,
        "enabled": False,
        "feeds": [
            {
                "value": "https://www.reddit.com/r/LocalLLaMA/",
                "enabled": False,
            },
            {
                "value": "https://www.reddit.com/r/programming/",
                "enabled": True,
            },
        ],
        "tags": [],
    }
    assert (await client.get(PATH)).json() == {"sources": [response.json()]}


async def test_a_broken_rule_names_each_field(client: AsyncClient) -> None:
    response = await client.post(PATH, json=body(
        accounts=[{"value": "someone", "enabled": True}],
        feeds=[{"value": "programming", "enabled": True}],
    ), headers=SAME_ORIGIN)

    assert (response.status_code, response.json()) == (400, {
        "error": "This source needs attention.",
        "code": "invalid_source",
        "fields": [
            {
                "path": "accounts",
                "code": "invalid_platform_options",
                "message": "Reddit sources follow communities, not accounts.",
            },
            {
                "path": "feeds.0.value",
                "code": "invalid_url",
                "message": "Use r/community or a Reddit community URL.",
            },
        ],
    })


async def test_a_duplicate_is_refused(client: AsyncClient) -> None:
    await add(client)

    response = await client.post(
        PATH, json=body(enabled=False, tags=["ai"]), headers=SAME_ORIGIN,
    )

    assert (response.status_code, response.json()) == (409, {
        "error": "You already have a source with these settings.",
        "code": "duplicate_source",
        "fields": [],
    })


async def test_an_edit_into_a_duplicate_is_refused(
    client: AsyncClient,
) -> None:
    await add(client)
    other = await add(client, domain="x.com", feeds=[], reddit=None)

    response = await client.put(
        f"{PATH}/{other['id']}", json=body(), headers=SAME_ORIGIN,
    )

    assert response.json()["code"] == "duplicate_source"


async def test_an_unknown_id_is_not_found(client: AsyncClient) -> None:
    missing = f"{PATH}/{uuid.uuid4()}"

    read = await client.get(missing)
    edit = await client.put(missing, json=body(), headers=SAME_ORIGIN)

    assert (read.status_code, read.json()["code"]) == (404, "not_found")
    assert (edit.status_code, edit.json()["error"]) == (
        404, "No source has this ID.",
    )


async def test_an_id_must_be_a_uuid(client: AsyncClient) -> None:
    response = await client.get(f"{PATH}/not-an-id")

    assert (response.status_code, response.json()["code"]) == (
        400, "invalid_request",
    )


@pytest.mark.parametrize("missing", ["enabled", "accounts", "adapters"])
async def test_every_field_is_required(
    client: AsyncClient, missing: str,
) -> None:
    sent = body()
    del sent[missing]

    response = await client.post(PATH, json=sent, headers=SAME_ORIGIN)

    assert response.status_code == 400
    assert [field["path"] for field in response.json()["fields"]] == [
        f"body.{missing}",
    ]


@pytest.mark.parametrize("extra", [
    {"id": str(uuid.uuid4())},
    {"password": "hunter2"},
    {"onepassword": {"item": "x"}},
    {"profile_dir": "/tmp/profile"},
])
async def test_unknown_keys_are_refused(
    client: AsyncClient, extra: dict[str, Any],
) -> None:
    response = await client.post(
        PATH, json=body(**extra), headers=SAME_ORIGIN,
    )

    assert response.status_code == 400
    assert response.json()["fields"][0]["code"] == "extra_forbidden"


async def test_lists_have_a_size_limit(client: AsyncClient) -> None:
    accounts = [{"value": f"a{n}", "enabled": True} for n in range(1_001)]

    response = await client.post(PATH, json=body(
        domain="x.com", accounts=accounts, feeds=[], reddit=None,
    ), headers=SAME_ORIGIN)

    assert response.status_code == 400
    assert response.json()["fields"][0]["path"] == "body.accounts"


async def test_writes_need_the_same_origin(client: AsyncClient) -> None:
    response = await client.post(
        PATH, json=body(), headers={"origin": "https://evil.test"},
    )

    assert response.status_code == 403


async def test_a_stored_source_this_code_cannot_read_answers_503(
    app: FastAPI, caplog: pytest.LogCaptureFixture,
) -> None:
    class Unreadable(FakeSourceRepository):
        async def list(self) -> tuple[Source, ...]:
            raise StoredSourceError("Stored source 1 can't be read")

    app.dependency_overrides[source_repository] = lambda: Unreadable()

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get(PATH)

    assert (response.status_code, response.json()["code"]) == (
        503, "unavailable",
    )
    assert "can't be read" in caplog.text
