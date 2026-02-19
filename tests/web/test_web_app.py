from __future__ import annotations

import datetime
import pathlib
from typing import Any, NamedTuple, cast

import pytest

from dank.web import app as web_app  # pyright: ignore[reportPrivateUsage]


class _DummyResult(NamedTuple):
    rows: list[dict[str, Any]]


class _DummyClickHouse:
    def __init__(self) -> None:
        self.query = ""
        self.params: dict[str, Any] = {}

    async def fetch_json(
        self,
        query: str,
        params: dict[str, Any],
    ) -> _DummyResult:
        self.query = query
        self.params = params

        return _DummyResult(rows=[])


def test_parse_days_back_clamps_values() -> None:
    assert web_app._parse_days_back(None) == 0  # pyright: ignore[reportPrivateUsage]
    assert web_app._parse_days_back("-10") == 0  # pyright: ignore[reportPrivateUsage]
    assert web_app._parse_days_back("14") == 14  # pyright: ignore[reportPrivateUsage]


def test_days_back_zero_shows_infinity_and_no_time_filter() -> None:
    assert web_app._days_back_label(0) == "∞"  # pyright: ignore[reportPrivateUsage]
    assert web_app._minimum_created_at(0) is None  # pyright: ignore[reportPrivateUsage]


def test_render_index_uses_jinja_template_and_root_header_link() -> None:
    templates = web_app._template_environment()  # pyright: ignore[reportPrivateUsage]
    body = web_app._render_index(  # pyright: ignore[reportPrivateUsage]
        templates,
        posts=[],
        assets={},
        assets_dir=pathlib.Path("/tmp"),
        limit=50,
        search_text="",
        domain_filter="x.com",
        account_filter="dense",
        days_back=7,
        has_previous_page=False,
        has_next_page=False,
    )

    assert '<h1><a href="/">DANK</a></h1>' in body
    assert 'name="domain"' in body
    assert 'name="account"' in body
    assert 'name="days_back"' in body


@pytest.mark.asyncio
async def test_search_posts_filters_before_age_weighting(
    monkeypatch: Any,
) -> None:
    clickhouse = _DummyClickHouse()
    now = datetime.datetime.now(datetime.UTC)

    async def fake_search_embedding(
        clickhouse_client: Any,
        *,
        search_text: str,
    ) -> tuple[float, ...] | None:
        assert clickhouse_client is clickhouse
        assert search_text == "status update"

        return (0.1, 0.2)

    monkeypatch.setattr(
        web_app,
        "_search_embedding",
        fake_search_embedding,
    )

    await web_app._search_posts(  # pyright: ignore[reportPrivateUsage]
        cast(Any, clickhouse),
        search_text="status update",
        limit=10,
        domain_filter="x.com",
        account_filter="dense",
        min_created_at=now - datetime.timedelta(days=3),
    )

    assert "WHERE distance <= %(maximum_distance)s" in clickhouse.query
    assert "positionCaseInsensitive(author, %(account_filter)s) > 0" in (
        clickhouse.query
    )
    assert "created_at >= %(min_created_at)s" in clickhouse.query
    assert clickhouse.params["maximum_distance"] == (
        web_app.SEARCH_MAXIMUM_DISTANCE
    )


@pytest.mark.asyncio
async def test_search_embedding_uses_cache_when_available(
    monkeypatch: Any,
) -> None:
    class _DummyEmbedder:
        model_name = "model-a"

        def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
            raise AssertionError(
                "Should not recompute embeddings on cache hit",
            )

    async def fake_load_cached_embedding(
        clickhouse_client: Any,
        *,
        model_name: str,
        search_text: str,
    ) -> tuple[float, ...] | None:
        assert model_name == "model-a"
        assert search_text == "hello"
        return (0.4, 0.6)

    async def fake_store_cached_embedding(**kwargs: Any) -> None:
        raise AssertionError("Should not write cache on cache hit")

    monkeypatch.setattr(
        web_app,
        "get_embedding_model",
        lambda: _DummyEmbedder(),
    )
    monkeypatch.setattr(
        web_app,
        "_load_cached_embedding",
        fake_load_cached_embedding,
    )
    monkeypatch.setattr(
        web_app,
        "_store_cached_embedding",
        fake_store_cached_embedding,
    )

    result = await web_app._search_embedding(  # pyright: ignore[reportPrivateUsage]
        clickhouse_client=cast(Any, object()),
        search_text="hello",
    )

    assert result == (0.4, 0.6)
