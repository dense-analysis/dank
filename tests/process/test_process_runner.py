import datetime
import re
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import SourceConfig
from dank.model import Post
from dank.process import runner
from dank.process.rss import convert_raw_post
from dank.process.runner import (
    parse_age_window,
    process_source_assets,
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


class _DummyAssetClient:
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

    async def insert_rows(
        self,
        _table: str,
        _rows: list[dict[str, Any]],
    ) -> None:
        return


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


async def test_process_source_posts_filters_by_scraped_at() -> None:
    client = _DummyClient()
    since = datetime.datetime.now(datetime.UTC)

    converted = await process_source_posts(
        cast(Any, client),
        "x.com",
        lambda _row: None,
        since=since,
        embedder=cast(Any, _DummyEmbedder()),
    )

    assert converted == 0
    assert "AND scraped_at >= %(since)s" in client.query
    assert "coalesce(post_created_at, scraped_at)" not in client.query


async def test_process_source_posts_only_selects_unprocessed_posts() -> None:
    client = _DummyClient()
    since = datetime.datetime.now(datetime.UTC)

    converted = await process_source_posts(
        cast(Any, client),
        "x.com",
        lambda _row: None,
        since=since,
        embedder=cast(Any, _DummyEmbedder()),
    )

    assert converted == 0
    assert "LIMIT 1 BY post_id" in client.query
    assert "LEFT JOIN" in client.query
    assert "raw.scraped_at > processed.updated_at" in client.query
    assert client.params["reprocess"] is False


async def test_reprocess_keeps_domain_and_age_bounds() -> None:
    client = _DummyClient()
    since = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)

    await process_source_posts(
        cast(Any, client), "publisher.test", lambda _row: None,
        since=since, embedder=cast(Any, _DummyEmbedder()), reprocess=True,
    )

    assert "WHERE %(reprocess)s OR" in client.query
    assert "WHERE domain = %(domain)s AND scraped_at >= %(since)s" in (
        client.query
    )
    assert "LIMIT 1 BY post_id" in client.query
    assert client.params == {
        "domain": "publisher.test", "since": since, "reprocess": True,
    }


async def test_process_run_scopes_reprocessing_to_selected_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = MagicMock()
    settings.sources = (
        SourceConfig(domain="www.theregister.com", accounts=()),
        SourceConfig(domain="other.test", accounts=()),
    )
    posts = AsyncMock(return_value=1)
    assets = AsyncMock(return_value=0)
    monkeypatch.setattr(runner, "process_source_posts", posts)
    monkeypatch.setattr(runner, "process_source_assets", assets)
    monkeypatch.setattr(runner, "ClickHouseClient", MagicMock())

    assert await runner.run_process(
        settings, age="168h", reprocess=True,
        domain_regex=re.compile(r"^www\.theregister\.com$"),
    ) == 1

    posts.assert_awaited_once()
    assert posts.call_args.args[1] == "www.theregister.com"
    assert posts.call_args.kwargs["reprocess"] is True
    assets.assert_awaited_once()
    assert assets.call_args.args[1] == "www.theregister.com"
    assert "reprocess" not in assets.call_args.kwargs


async def test_insert_posts_writes_embedding_arrays() -> None:
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

    converted = await process_source_posts(
        cast(Any, client),
        "x.com",
        _converter,
        since=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
        embedder=cast(Any, _TupleEmbedder()),
    )

    assert converted == 1
    assert client.table == "posts"
    assert client.rows
    assert client.rows[0]["title_embedding"] == [1.0]
    assert client.rows[0]["html_embedding"] == [1.0]


async def test_process_source_assets_only_selects_unprocessed_assets() -> None:
    client = _DummyAssetClient()
    since = datetime.datetime.now(datetime.UTC)

    converted = await process_source_assets(
        cast(Any, client),
        "x.com",
        lambda _raw: None,
        since=since,
    )

    assert converted == 0
    assert "LIMIT 1 BY post_id, url" in client.query
    assert "LEFT JOIN" in client.query
    assert "raw.scraped_at > processed.updated_at" in client.query


@pytest.mark.parametrize(("feed_author", "expected"), [
    ("", "Known Writer"),
    ("New Writer", "New Writer"),
])
async def test_reprocessing_retains_author_when_new_capture_omits_it(
    feed_author: str, expected: str,
) -> None:
    class Client(_InsertClient):
        async def fetch_json(
            self, query: str, params: dict[str, object] | None = None,
        ) -> _Result:
            result = await super().fetch_json(query, params)
            result.rows[0].update({
                "payload": f"<item><title>Article</title>"
                f"<author>{feed_author}</author>"
                "<description>Feed text.</description></item>",
                "previous_author": "Known Writer",
            })

            return result

    client = Client()
    await process_source_posts(
        cast(Any, client), "publisher.test", convert_raw_post,
        since=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
        embedder=cast(Any, _DummyEmbedder()), reprocess=True,
    )

    assert client.rows[0]["author"] == expected
    assert "FROM posts FINAL" in client.query
    assert client.rows[0]["updated_at"] == datetime.datetime(
        2026, 2, 1, tzinfo=datetime.UTC,
    )


async def test_embeddings_use_readable_text_instead_of_page_code() -> None:
    client = _InsertClient()
    calls: list[list[str]] = []

    class _SpyEmbedder:
        def embed_texts(self, items: list[str]) -> list[tuple[float, ...]]:
            calls.append(items)

            return [(1.0,) for _ in items]

    def converter(_raw: Any) -> Post:
        now = datetime.datetime.now(datetime.UTC)

        return Post(
            domain="example.com", post_id="1", url="https://example.com/1",
            created_at=now, updated_at=now, author="author", title="Title",
            title_embedding=(), html_embedding=(), source="rss",
            html="<head><style>tracking css</style></head>"
                 "<p>Readable <strong>article</strong>.</p>",
        )

    await process_source_posts(
        cast(Any, client), "example.com", converter,
        since=datetime.datetime.now(datetime.UTC),
        embedder=cast(Any, _SpyEmbedder()),
    )
    assert calls == [["Title"], ["Readable article."]]
