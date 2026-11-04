"""Rules every route inherits from the platform."""

import pytest
from fastapi import APIRouter, FastAPI
from httpx import AsyncClient

from dank.api.app import create_app
from dank.api.schemas import ApiModel
from dank.config import Settings
from dank.errors import Conflict, NotFound
from tests.api.client import BASE_URL, http_client

SAME_ORIGIN = {"origin": BASE_URL}


class Probe(ApiModel):
    n: int


# DEN-46 has no unsafe or failing route yet, so a test-only router
# exercises the guard and the error handlers.
probe = APIRouter(prefix="/api/probe")


@probe.get("")
async def read_probe() -> Probe:
    return Probe(n=1)


@probe.post("")
async def write_probe(body: Probe) -> Probe:
    return body


@probe.get("/missing")
async def missing_probe() -> Probe:
    raise NotFound("No such probe")


@probe.get("/stale")
async def stale_probe() -> Probe:
    raise Conflict("The probe changed")


@probe.get("/broken")
async def broken_probe() -> Probe:
    raise RuntimeError("ClickHouse is down")


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(probe)

    return app


async def test_unknown_paths_and_methods_use_the_envelope(
    client: AsyncClient,
) -> None:
    missing = await client.get("/api/nope")
    wrong = await client.delete("/api/probe")

    assert (missing.status_code, missing.json()) == (404, {
        "error": "Not Found", "code": "not_found", "fields": [],
    })
    assert (wrong.status_code, wrong.json()["code"]) == (
        405, "method_not_allowed",
    )


async def test_pages_and_files_answer_errors_in_plain_text(
    client: AsyncClient,
) -> None:
    missing = await client.get("/nope")
    wrong = await client.delete("/")

    assert (missing.status_code, missing.text) == (404, "Not Found")
    assert missing.headers["content-type"] == "text/plain; charset=utf-8"
    assert wrong.status_code == 405
    assert wrong.headers["content-type"] == "text/plain; charset=utf-8"


@pytest.mark.parametrize("path", [
    "/post/", "/assets", "/static", "/api/probe/", "/api/reader/sources/",
])
async def test_a_trailing_slash_is_a_different_path_and_never_redirects(
    client: AsyncClient, path: str,
) -> None:
    # An automatic redirect would name this server's scheme, which behind the
    # HTTPS proxy is plain HTTP.
    response = await client.get(path)

    assert response.status_code == 404


async def test_domain_errors_choose_their_status(client: AsyncClient) -> None:
    missing = await client.get("/api/probe/missing")
    stale = await client.get("/api/probe/stale")

    assert (missing.status_code, missing.json()) == (404, {
        "error": "No such probe", "code": "not_found", "fields": [],
    })
    assert (stale.status_code, stale.json()["code"]) == (409, "conflict")


async def test_unexpected_errors_answer_503_and_are_logged(
    app: FastAPI, caplog: pytest.LogCaptureFixture,
) -> None:
    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get("/api/probe/broken")

    assert (response.status_code, response.json()) == (503, {
        "error": "The collection is unavailable. Please try again.",
        "code": "unavailable",
        "fields": [],
    })
    # Starlette answers these outside every middleware, ApiHeaders included.
    assert response.headers["cache-control"] == "no-store"
    assert "Request failed: /api/probe/broken" in caplog.text


async def test_routes_without_the_lifespan_fail_loudly(
    app: FastAPI,
) -> None:
    # No override for the ClickHouse adapter, and no lifespan to open it.
    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get("/api/reader/sources")

    assert response.status_code == 503


async def test_validation_errors_name_each_field(client: AsyncClient) -> None:
    response = await client.post(
        "/api/probe", json={"n": "many", "extra": 1}, headers=SAME_ORIGIN,
    )

    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"
    assert [
        (field["path"], field["code"]) for field in response.json()["fields"]
    ] == [
        ("body.n", "int_parsing"),
        ("body.extra", "extra_forbidden"),
    ]


@pytest.mark.parametrize("headers", [
    {"origin": "https://evil.test"},
    {"origin": "https://dank.test"},
    {"sec-fetch-site": "cross-site"},
])
async def test_unsafe_methods_need_the_same_origin(
    client: AsyncClient, headers: dict[str, str],
) -> None:
    response = await client.post("/api/probe", json={"n": 1}, headers=headers)

    assert (response.status_code, response.json()["code"]) == (
        403, "forbidden",
    )


async def test_unsafe_methods_need_a_json_body(client: AsyncClient) -> None:
    response = await client.post(
        "/api/probe", data={"n": "1"}, headers=SAME_ORIGIN,
    )

    assert (response.status_code, response.json()["code"]) == (
        415, "unsupported_media_type",
    )


@pytest.mark.parametrize("headers", [
    # A same-origin fetch from a browser, or a script with no Origin.
    SAME_ORIGIN,
    {"sec-fetch-site": "same-origin", **SAME_ORIGIN},
    {},
    # What the local HTTPS proxy sends once it has checked the real
    # https:// origin: an http:// Origin, plus its forwarded scheme.
    {**SAME_ORIGIN, "x-forwarded-proto": "https"},
])
async def test_same_origin_writes_pass_including_through_the_proxy(
    client: AsyncClient, headers: dict[str, str],
) -> None:
    response = await client.post("/api/probe", json={"n": 1}, headers=headers)

    assert (response.status_code, response.json()) == (200, {"n": 1})


async def test_head_answers_like_get_without_a_body(
    client: AsyncClient,
) -> None:
    get = await client.get("/api/probe")
    head = await client.head("/api/probe")

    assert head.status_code == 200
    assert head.content == b""
    assert head.headers["content-length"] == get.headers["content-length"]
    assert head.headers["cache-control"] == "no-store"


def test_every_route_returns_a_strict_model(settings: Settings) -> None:
    # Checked through OpenAPI, the contract the reader's types come from:
    # a named schema with additionalProperties false is an ApiModel.
    document = create_app(settings, page_size=50).openapi()
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
