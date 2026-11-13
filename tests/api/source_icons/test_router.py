"""GET /api/reader/source-icon over HTTP, with fakes behind its ports."""

import uuid
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api import deps
from dank.api.source_icons.deps import collected_domains
from dank.api.sources.deps import saved_sources
from dank.config import Settings
from dank.sources.model import Source
from tests.api.client import http_client
from tests.api.source_icons.fakes import FakeCollectedDomains, FakeIconCache
from tests.sources.fakes import FakeSourceRepository

PATH = "/api/reader/source-icon"
PNG = b"\x89PNG icon"
NOT_FOUND: dict[str, Any] = {
    "error": "Not Found", "code": "not_found", "fields": [],
}


async def test_a_configured_source_gets_a_cached_png(
    client: AsyncClient, icons: FakeIconCache,
) -> None:
    icons.icons["politics.example"] = PNG

    response = await client.get(PATH, params={"domain": "politics.example"})

    assert response.status_code == 200
    assert response.content == PNG
    assert response.headers["content-type"] == "image/png"
    assert response.headers["cache-control"] == "public, max-age=86400"


async def test_a_collected_domain_gets_an_icon_without_being_configured(
    client: AsyncClient,
    collected: FakeCollectedDomains,
    icons: FakeIconCache,
) -> None:
    collected.domains.add("old.example")
    icons.icons["old.example"] = PNG

    response = await client.get(PATH, params={"domain": "old.example"})

    assert response.status_code == 200


async def test_the_domain_is_normalized_before_any_lookup(
    client: AsyncClient, icons: FakeIconCache,
) -> None:
    icons.icons["politics.example"] = PNG

    response = await client.get(
        PATH, params={"domain": " Politics.EXAMPLE ", "unknown": "ignored"},
    )

    assert response.status_code == 200


async def test_an_unknown_domain_never_starts_a_capture(
    client: AsyncClient, icons: FakeIconCache,
) -> None:
    # The cache would answer, so only the known-source check can stop this.
    icons.icons["stranger.example"] = PNG

    response = await client.get(PATH, params={"domain": "stranger.example"})

    assert (response.status_code, response.json()) == (404, NOT_FOUND)
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("params", [
    {},
    {"domain": ""},
    {"domain": "reddit.com"},
    {"domain": "127.0.0.1"},
    {"domain": "../politics.example"},
])
async def test_platforms_and_malformed_domains_are_not_found(
    client: AsyncClient,
    collected: FakeCollectedDomains,
    icons: FakeIconCache,
    params: dict[str, str],
) -> None:
    domain = params.get("domain", "")
    collected.domains.add(domain)
    icons.icons[domain] = PNG

    response = await client.get(PATH, params=params)

    assert (response.status_code, response.json()) == (404, NOT_FOUND)


async def test_a_failed_capture_is_not_found_and_not_cached(
    client: AsyncClient,
) -> None:
    response = await client.get(PATH, params={"domain": "politics.example"})

    assert (response.status_code, response.json()) == (404, NOT_FOUND)
    assert response.headers["cache-control"] == "no-store"


async def test_head_keeps_the_cache_header_without_a_body(
    client: AsyncClient, icons: FakeIconCache,
) -> None:
    icons.icons["politics.example"] = PNG

    response = await client.head(PATH, params={"domain": "politics.example"})

    assert (response.status_code, response.content) == (200, b"")
    assert response.headers["cache-control"] == "public, max-age=86400"


async def test_a_database_failure_is_unavailable(app: FastAPI) -> None:
    class Broken:
        async def has_posts(self, domain: str) -> bool:
            raise RuntimeError("ClickHouse is down")

    app.dependency_overrides[collected_domains] = lambda: Broken()

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get(PATH, params={"domain": "old.example"})

    assert (response.status_code, response.json()["code"]) == (
        503, "unavailable",
    )
    assert response.headers["cache-control"] == "no-store"


async def test_saved_sources_get_icons_before_they_have_posts(
    app: FastAPI, client: AsyncClient, settings: Settings,
    icons: FakeIconCache,
) -> None:
    # With the switch on, a disabled saved source is still known, and a
    # source only config.toml lists is not.
    switched = settings._replace(sources_in_database=True)
    app.dependency_overrides[deps.settings] = lambda: switched
    app.dependency_overrides[saved_sources] = lambda: FakeSourceRepository(
        Source(uuid.UUID(int=1), enabled=False, domain="saved.example"),
    )
    icons.icons["saved.example"] = PNG
    icons.icons["politics.example"] = PNG

    saved = await client.get(PATH, params={"domain": "saved.example"})
    config = await client.get(PATH, params={"domain": "politics.example"})

    assert (saved.status_code, config.status_code) == (200, 404)
