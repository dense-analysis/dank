"""aiohttp requests read the sources table when `sources_in_database` is on.

tests/api/sources covers the catalogs themselves, which both servers share.
"""

from __future__ import annotations

import json
import pathlib
import uuid
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from dank.api.sources.saved_catalog import SavedSourceCatalog
from dank.config import Settings, SourceConfig, load_settings
from dank.sources.model import Source
from dank.storage.clickhouse import QueryResult
from dank.web import reader_api
from dank.web.app import create_app
from dank.web.reader_source_icons import SOURCE_ICON_CACHE
from dank.web.sources import SOURCE_CATALOG, current_settings
from tests.sources.fakes import FakeSourceRepository

PNG = b"\x89PNG\r\n\x1a\n"


class FakeClickHouse:
    """Answers every query with no rows, and remembers what was asked."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        self.calls.append((query, params or {}))

        return QueryResult([])


class FakeIcons:
    async def get(self, domain: str) -> bytes:
        return PNG


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Settings:
    return load_settings("config.example.toml")._replace(
        data_dir=tmp_path,
        sources=(SourceConfig("config.example", (), tags=("toml",)),),
    )


@pytest.fixture
def clickhouse() -> FakeClickHouse:
    return FakeClickHouse()


@pytest.fixture
def app(settings: Settings, clickhouse: FakeClickHouse) -> web.Application:
    """An app with the switch on, one enabled saved source and one disabled."""
    app = create_app(
        settings._replace(sources_in_database=True), page_size=50,
    )
    app["clickhouse"] = clickhouse
    app[SOURCE_CATALOG] = SavedSourceCatalog(FakeSourceRepository(
        Source(
            uuid.UUID(int=1), enabled=True, domain="saved.example",
            tags=("saved",),
        ),
        Source(
            uuid.UUID(int=2), enabled=False, domain="off.example",
            tags=("saved",),
        ),
    ))

    return app


async def get(app: web.Application, path: str) -> web.Response:
    request = make_mocked_request("GET", path, app=app)
    match = await app.router.resolve(request)
    match.add_app(app)
    request._match_info = match  # pyright: ignore[reportPrivateUsage]
    response = await reader_api.reader_errors(request, match.handler)
    assert isinstance(response, web.Response)

    return response


def listed(response: web.Response) -> list[str]:
    return [
        source["domain"]
        for source in json.loads(response.text or "")["sources"]
        if source["configured"]
    ]


async def test_the_sidebar_lists_config_sources_while_the_switch_is_off(
    settings: Settings, clickhouse: FakeClickHouse,
) -> None:
    app = create_app(settings, page_size=50)
    app["clickhouse"] = clickhouse

    assert listed(await get(app, "/api/reader/sources")) == ["config.example"]


async def test_the_sidebar_lists_saved_sources_and_keeps_disabled_ones(
    app: web.Application,
) -> None:
    assert listed(await get(app, "/api/reader/sources")) == [
        "off.example", "saved.example",
    ]


async def test_the_sidebar_says_where_its_sources_come_from(
    app: web.Application, settings: Settings, clickhouse: FakeClickHouse,
) -> None:
    config_app = create_app(settings, page_size=50)
    config_app["clickhouse"] = clickhouse

    config = await get(config_app, "/api/reader/sources")
    saved = await get(app, "/api/reader/sources")

    assert (
        json.loads(config.text or "")["in_database"],
        json.loads(saved.text or "")["in_database"],
    ) == (False, True)


async def test_a_tag_selects_saved_sources(
    app: web.Application, clickhouse: FakeClickHouse,
) -> None:
    await get(app, "/api/reader/posts?tag=saved")

    assert [
        params["tag_domains"]
        for _, params in clickhouse.calls if "tag_domains" in params
    ] == [["off.example", "saved.example"]]


async def test_a_config_tag_selects_nothing(
    app: web.Application, clickhouse: FakeClickHouse,
) -> None:
    await get(app, "/api/reader/posts?tag=toml")

    assert [
        params["tag_domains"]
        for _, params in clickhouse.calls if "tag_domains" in params
    ] == [[]]


async def test_a_saved_source_has_an_icon_before_it_has_posts(
    app: web.Application, clickhouse: FakeClickHouse,
) -> None:
    app[SOURCE_ICON_CACHE] = FakeIcons()  # pyright: ignore[reportCallIssue, reportArgumentType]

    saved = await get(app, "/api/reader/source-icon?domain=saved.example")
    config = await get(app, "/api/reader/source-icon?domain=config.example")

    assert saved.status == 200
    # Only config.example needed a look for posts, and it has none.
    assert config.status == 404
    assert [params for _, params in clickhouse.calls] == [
        {"domain": "config.example"},
    ]


async def test_settings_for_a_request_carry_the_saved_sources(
    app: web.Application, settings: Settings,
) -> None:
    request = make_mocked_request("GET", "/", app=app)
    current = await current_settings(request)

    assert [source.domain for source in current.sources] == [
        "saved.example", "off.example",
    ]
    assert current.youtube == settings.youtube


async def test_the_switch_never_falls_back_to_config(
    settings: Settings, clickhouse: FakeClickHouse,
) -> None:
    app = create_app(
        settings._replace(sources_in_database=True), page_size=50,
    )
    app["clickhouse"] = clickhouse

    response = await get(app, "/api/reader/sources")

    assert response.status == 503
