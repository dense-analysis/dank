import asyncio
import datetime
from typing import Any, NamedTuple, cast

import pytest

from dank.model import Post
from dank.process.runner import (
    parse_age_window,
    process_source_posts,
)


class _Result(NamedTuple):
    rows: list[dict[str, Any]]


class _DummyClient:
    def __init__(self) -> None:
        self.query = ""
        self.params: dict[str, object] = {}

    async def fetch_json(
        self,
        query: str,
        params: dict[str, object] | None = None,
    ) -> _Result:
        self.query = query
        self.params = params or {}

        return _Result(rows=[])


class _DummyEmbedder:
    def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
        return [() for _ in items]


class _InsertClient:
    def __init__(self) -> None:
        self.query = ""
        self.params: dict[str, object] = {}
        self.table = ""
        self.rows: list[dict[str, Any]] = []

    async def fetch_json(
        self,
        query: str,
        params: dict[str, object] | None = None,
    ) -> _Result:
        self.query = query
        self.params = params or {}

        return _Result(
            rows=[
                {
                    "domain": "x.com",
                    "post_id": "1",
                    "url": "https://x.com/i/status/1",
                    "post_created_at": None,
                    "scraped_at": datetime.datetime(
                        2026,
                        2,
                        1,
                        tzinfo=datetime.UTC,
                    ),
                    "source": "x",
                    "request_url": "https://x.com/i/api/graphql/Example",
                    "payload": "{}",
                },
            ],
        )

    async def insert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
    ) -> None:
        self.table = table
        self.rows = rows


def test_parse_age_window_seconds() -> None:
    assert parse_age_window("30s") == datetime.timedelta(seconds=30)
    assert parse_age_window("15") == datetime.timedelta(seconds=15)


def test_parse_age_window_minutes() -> None:
    assert parse_age_window("10m") == datetime.timedelta(minutes=10)
    assert parse_age_window("5min") == datetime.timedelta(minutes=5)


def test_parse_age_window_hours() -> None:
    assert parse_age_window("2h") == datetime.timedelta(hours=2)
    assert parse_age_window("1hour") == datetime.timedelta(hours=1)


@pytest.mark.parametrize("value", ["", "0", "-5m", "3d", "abc"])
def test_parse_age_window_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_age_window(value)


def test_process_source_posts_filters_by_scraped_at() -> None:
    client = _DummyClient()
    since = datetime.datetime.now(datetime.UTC)

    converted = asyncio.run(
        process_source_posts(
            cast(Any, client),
            "x.com",
            lambda _row: None,
            since=since,
            embedder=cast(Any, _DummyEmbedder()),
        ),
    )

    assert converted == 0
    assert "AND scraped_at >= %(since)s" in client.query
    assert "coalesce(post_created_at, scraped_at)" not in client.query


def test_insert_posts_writes_embedding_arrays() -> None:
    client = _InsertClient()

    class _TupleEmbedder:
        def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
            return [(float(index),) for index, _ in enumerate(items, start=1)]

    def _converter(_raw: Any) -> Post:
        return Post(
            domain="x.com",
            post_id="1",
            url="https://x.com/i/status/1",
            created_at=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
            updated_at=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
            author="alice",
            title="title",
            title_embedding=(),
            html="html",
            html_embedding=(),
            source="x",
        )

    converted = asyncio.run(
        process_source_posts(
            cast(Any, client),
            "x.com",
            _converter,
            since=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
            embedder=cast(Any, _TupleEmbedder()),
        ),
    )

    assert converted == 1
    assert client.table == "posts"
    assert client.rows
    assert client.rows[0]["title_embedding"] == [1.0]
    assert client.rows[0]["html_embedding"] == [1.0]
