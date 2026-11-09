"""/reader/: the built app, its bundles and its client-side routes."""

import pathlib

import pytest
from httpx import AsyncClient

from dank.api.app import create_app
from dank.api.files.model import client_route
from dank.api.files.static import FileRoots
from dank.config import Settings
from tests.api.client import http_client
from tests.api.files.conftest import BUNDLE, INDEX

NO_CACHE = "no-store, no-cache, must-revalidate, max-age=0"


@pytest.mark.parametrize("path", ["/", "/reader"])
async def test_the_root_and_bare_path_redirect_without_naming_a_scheme(
    client: AsyncClient, path: str,
) -> None:
    response = await client.get(path)

    # Relative, because behind the HTTPS proxy this server sees plain HTTP
    # and an absolute address would send a phone back to it.
    assert response.status_code == 302
    assert response.headers["location"] == "/reader/"


@pytest.mark.parametrize("path", [
    "/reader/",
    "/reader/index.html",
    # Deep links load the app, which reads the path itself.
    "/reader/saved/politics",
    "/reader/post",
    "/reader/assets",
    # The rule is the last segment's extension, not the directories'.
    "/reader/v1.2/saved",
])
async def test_routes_and_the_entry_page_load_the_app_uncached(
    client: AsyncClient, path: str,
) -> None:
    response = await client.get(path)

    assert response.status_code == 200
    assert response.text == INDEX
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    assert response.headers["cache-control"] == NO_CACHE
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["expires"] == "0"


@pytest.mark.parametrize("path", ["/post", "/static/app.css"])
async def test_the_retired_viewer_pages_are_gone(
    client: AsyncClient, path: str,
) -> None:
    response = await client.get(path)

    assert response.status_code == 404


async def test_the_healthcheck_can_use_head(client: AsyncClient) -> None:
    # Caddy's healthcheck is `wget --spider /reader/`, which sends HEAD.
    response = await client.head("/reader/")

    assert (response.status_code, response.content) == (200, b"")
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    assert response.headers["content-length"] == str(len(INDEX))
    assert response.headers["cache-control"] == NO_CACHE


async def test_revalidating_the_entry_page_stays_uncached(
    client: AsyncClient,
) -> None:
    etag = (await client.get("/reader/")).headers["etag"]

    response = await client.get("/reader/", headers={"if-none-match": etag})

    # A 304 has no content type for the HTML rule to catch.
    assert "no-store" in response.headers["cache-control"]


async def test_bundles_are_files_with_validators_and_ranges(
    client: AsyncClient,
) -> None:
    path = "/reader/assets/index-abc.js"

    bundle = await client.get(path)
    fresh = await client.get(
        path, headers={"if-none-match": bundle.headers["etag"]},
    )
    part = await client.get(path, headers={"range": "bytes=0-6"})

    assert bundle.status_code == 200
    assert bundle.text == BUNDLE
    assert "javascript" in bundle.headers["content-type"]
    assert "cache-control" not in bundle.headers
    assert fresh.status_code == 304
    assert (part.status_code, part.text) == (206, BUNDLE[:7])


@pytest.mark.parametrize("path", [
    # A missing bundle must never answer with HTML.
    "/reader/assets/index-old.js",
    "/reader/missing.css",
    "/reader/saved/politics.png",
    # Traversal has no extension here, so only its own check stops it.
    "/reader/%2e%2e/secret",
    "/reader/assets/%2e%2e/%2e%2e/secret",
    "/reader/..%2fsecret",
])
async def test_missing_files_and_traversal_get_a_plain_404(
    client: AsyncClient, tmp_path: pathlib.Path, path: str,
) -> None:
    (tmp_path / "secret").write_text("secret")

    response = await client.get(path)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("text/plain")
    assert b"secret" not in response.content


async def test_bundles_and_data_assets_stay_apart(
    client: AsyncClient, assets: pathlib.Path,
) -> None:
    (assets / "data.png").write_bytes(b"data")

    data = await client.get("/assets/data.png")
    not_a_bundle = await client.get("/reader/assets/data.png")
    not_data = await client.get("/assets/assets/index-abc.js")
    not_data_either = await client.get("/assets/index-abc.js")

    assert data.status_code == 200
    assert not_a_bundle.status_code == 404
    assert not_data.status_code == 404
    assert not_data_either.status_code == 404


@pytest.mark.parametrize("path", ["/reader/", "/reader/saved", "/reader/a.js"])
async def test_an_unbuilt_reader_is_a_plain_404(
    settings: Settings,
    roots: FileRoots,
    tmp_path: pathlib.Path,
    path: str,
) -> None:
    app = create_app(settings, roots=roots._replace(reader=tmp_path / "no"))

    async with http_client(app) as client:
        response = await client.get(path)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("text/plain")


async def test_the_app_is_read_only(client: AsyncClient) -> None:
    response = await client.post("/reader/saved", json={})

    assert response.status_code == 405


@pytest.mark.parametrize(("path", "expected"), [
    (".", True),
    ("saved/politics", True),
    ("v1.2/saved", True),
    ("missing.js", False),
    ("assets/index-old.css", False),
    ("..", False),
    ("../secret", False),
])
def test_only_extensionless_paths_inside_the_build_are_routes(
    *, path: str, expected: bool,
) -> None:
    assert client_route(path) is expected
