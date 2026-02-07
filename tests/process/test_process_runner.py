import asyncio
import datetime
from typing import Any, NamedTuple, cast

import pytest

from dank.process.runner import parse_age_window, process_source_posts


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
    def embed_texts(self, items: list[str]) -> list[list[float]]:
        return [[] for _ in items]


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
