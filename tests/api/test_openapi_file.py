"""The reader's checked-in schema matches what the app describes."""

from collections.abc import Callable
from functools import cache
from typing import Any

import pytest
from fastapi import FastAPI

from dank.api import openapi
from dank.api.app import create_app
from dank.config import Settings
from tests.api.client import http_client

type RenderSchema = Callable[[], tuple[FastAPI, str]]


@pytest.fixture(scope="module")
def render_schema() -> RenderSchema:
    @cache
    def render() -> tuple[FastAPI, str]:
        apps: list[FastAPI] = []

        def capture_app(settings: Settings) -> FastAPI:
            app = create_app(settings)
            apps.append(app)

            return app

        # Generate the CLI output once and exercise the endpoint on that app.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(openapi, "create_app", capture_app)
            rendered = openapi.schema_text()

        [app] = apps

        return app, rendered

    return render


def test_the_checked_in_schema_is_current(render_schema: RenderSchema) -> None:
    _, rendered = render_schema()
    # On failure, run `make openapi` and commit both generated files.
    assert openapi.SCHEMA_FILE.read_text(encoding="utf-8") == rendered


@pytest.fixture
def document(render_schema: RenderSchema) -> dict[str, Any]:
    app, _ = render_schema()

    return app.openapi()


async def test_every_route_returns_a_strict_model(
    render_schema: RenderSchema, document: dict[str, Any],
) -> None:
    app, _ = render_schema()

    async with http_client(app) as client:
        response = await client.get("/api/openapi.json")

    assert response.status_code == 200
    assert response.json() == document
    # Checked through OpenAPI, the contract the reader's types come from:
    # a named schema with additionalProperties false is an ApiModel.
    schemas = document["components"]["schemas"]
    operations = [
        (path, operation)
        for path, item in document["paths"].items()
        for operation in item.values()
    ]

    assert operations

    for path, operation in operations:
        success = next(
            response for status, response in operation["responses"].items()
            if status.startswith("2")
        )

        assert path.startswith("/api/"), path

        # The one route that isn't JSON. The reader only links to it.
        if path == "/api/reader/source-icon":
            assert set(success["content"]) == {"image/png"}

            continue

        ref = success["content"]["application/json"]["schema"]["$ref"]
        assert schemas[ref.rsplit("/", 1)[1]]["additionalProperties"] is False


def test_openapi_describes_connections(document: dict[str, Any]) -> None:
    schemas = document["components"]["schemas"]

    assert schemas["ConnectionsResponse"]["additionalProperties"] is False
    assert schemas["ConnectionOut"]["properties"]["direction"]["enum"] == [
        "incoming", "outgoing", "related",
    ]


def test_openapi_describes_filters_and_each_post_shape(
    document: dict[str, Any],
) -> None:
    schemas = document["components"]["schemas"]
    posts = document["paths"]["/api/reader/posts"]["get"]
    parameters = {item["name"]: item for item in posts["parameters"]}

    assert parameters["domain"]["schema"]["type"] == "array"
    assert parameters["limit"]["schema"]["maximum"] == 100
    assert parameters["mode"]["schema"]["enum"] == [
        "words", "meaning", "combined",
    ]
    assert "conversation" in schemas["XDetailOut"]["properties"]
    assert "conversation" not in schemas["XOut"]["properties"]
    assert "comments_items" not in schemas["HackerNewsOut"]["properties"]
    assert schemas["PostOut"]["additionalProperties"] is False


def test_openapi_describes_the_scope_kinds_and_statuses(
    document: dict[str, Any],
) -> None:
    schemas = document["components"]["schemas"]
    post = document["paths"]["/api/reader/refresh"]["post"]

    assert set(post["responses"]) >= {"202"}
    assert set(schemas["RefreshRequest"]["properties"]) == {"scope"}
    assert schemas["RefreshRequest"]["required"] == ["scope"]
    assert set(
        schemas["RefreshRequest"]["properties"]["scope"]["discriminator"][
            "mapping"
        ],
    ) == {"all", "rss", "reddit", "x", "hacker_news", "youtube", "post"}
    assert set(schemas["RefreshJobOut"]["properties"]["status"]["enum"]) == {
        "queued", "running", "processing", "completed", "partial", "failed",
        "interrupted",
    }


def test_openapi_describes_each_source_kind(document: dict[str, Any]) -> None:
    schemas = document["components"]["schemas"]
    union = schemas["SourcesResponse"]["properties"]["sources"]["items"]

    assert union["discriminator"]["mapping"] == {
        "rss": "#/components/schemas/FeedSourceOut",
        "hacker_news": "#/components/schemas/FeedSourceOut",
        "youtube": "#/components/schemas/FeedSourceOut",
        "x": "#/components/schemas/XSourceOut",
        "reddit": "#/components/schemas/RedditSourceOut",
    }
    assert schemas["XSourceOut"]["additionalProperties"] is False
