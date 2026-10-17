from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import aiohttp
import pytest

from dank.config import SourceConfig, load_settings
from dank.scrape import runner
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.reddit import totals
from dank.scrape.reddit.totals import (
    _parse_total,  # pyright: ignore[reportPrivateUsage]
    scrape_community_totals,
)
from dank.scrape.runner import (
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    _refresh_reddit_totals,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult

UPDATED = 1791710798


def archive_payload(
    community: str = 'conspiracy', count: object = 1866004,
    updated: object = UPDATED,
) -> str:
    return json.dumps({'data': [{
        'display_name': community,
        '_meta': {'num_posts': count, 'num_posts_updated_at': updated},
    }]})


def test_estimate_preserves_original_payload_and_source_date() -> None:
    payload = archive_payload('Conspiracy')
    estimate = _parse_total(payload, 'CONSPIRACY', 'https://archive.test')
    assert estimate.community == 'conspiracy'
    assert estimate.total_posts == 1866004
    assert estimate.updated_at == dt.datetime(
        2026, 10, 11, 9, 26, 38, tzinfo=dt.UTC,
    )
    assert estimate.scraped_at > estimate.updated_at
    assert estimate.request_url == 'https://archive.test'
    assert estimate.payload == payload


@pytest.mark.parametrize('count', [True, -1, 1.5, '12', None, 2**53])
def test_invalid_counts_cannot_become_estimates(count: object) -> None:
    with pytest.raises(ValueError, match='post count'):
        _parse_total(archive_payload(count=count), 'conspiracy', '')


@pytest.mark.parametrize('updated', [True, 0, -1, 1.5, '12', None, 2**53])
def test_missing_invalid_or_future_source_dates_are_rejected(
    updated: object,
) -> None:
    with pytest.raises(ValueError, match='update date'):
        _parse_total(archive_payload(updated=updated), 'conspiracy', '')


@pytest.mark.parametrize('payload', [
    '<html>temporarily unavailable</html>', '{}', '[]', '{"data": []}',
    '{"data": [null]}', '{"data": [{"display_name": "conspiracy"}]}',
    archive_payload('another_community'),
    json.dumps({'data': [json.loads(archive_payload())['data'][0]] * 2}),
])
def test_unmatched_or_incomplete_metadata_is_not_used(payload: str) -> None:
    with pytest.raises(ValueError):
        _parse_total(payload, 'conspiracy', '')


def test_successful_zero_count_is_preserved() -> None:
    estimate = _parse_total(archive_payload(count=0), 'conspiracy', '')
    assert estimate.total_posts == 0


async def test_lookup_deduplicates_and_continues_after_bad_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    @asynccontextmanager
    async def response(client: Any, url: str) -> AsyncGenerator[Any]:
        calls.append(url)
        name = parse_qs(urlsplit(url).query)['subreddit'][0]
        result = cast(Any, type('Response', (), {})())
        result.raise_for_status = lambda: None
        result.text = AsyncMock(return_value=(
            '{}' if name == 'missing' else archive_payload(name)
        ))
        yield result

    monkeypatch.setattr(totals, 'http_response', response)
    stats = SourceMetrics(1, SourceConfig('reddit.com', ()))
    token = current_source.set(stats)

    try:
        estimates = [item async for item in scrape_community_totals(
            cast(Any, None), ('missing', 'Conspiracy', 'CONSPIRACY'),
        )]
    finally:
        current_source.reset(token)

    assert [item.community for item in estimates] == ['conspiracy']
    assert len(calls) == 2
    assert parse_qs(urlsplit(calls[1]).query) == {
        'subreddit': ['conspiracy'], 'limit': ['1'],
    }
    assert stats.counts['parse_failures'] == 1


@pytest.mark.parametrize('error', [
    aiohttp.ClientConnectionError('offline'), TimeoutError('slow archive'),
    aiohttp.ClientResponseError(
        cast(Any, SimpleNamespace(real_url='https://archive.test')), (),
        status=429,
    ),
])
async def test_failed_refresh_does_not_write_or_clear_saved_estimate(
    monkeypatch: pytest.MonkeyPatch, error: Exception,
) -> None:
    @asynccontextmanager
    async def response(*args: Any) -> AsyncGenerator[Any]:
        raise error
        yield  # pragma: no cover

    monkeypatch.setattr(totals, 'http_response', response)
    client = AsyncMock()
    await _refresh_reddit_totals(client, cast(Any, None), ('conspiracy',))
    client.insert_rows.assert_not_awaited()


async def test_optional_lookup_has_a_timeout_and_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def response(*args: Any) -> AsyncGenerator[Any]:
        await asyncio.Event().wait()
        yield  # pragma: no cover

    monkeypatch.setattr(totals, 'http_response', response)
    monkeypatch.setattr(totals, 'LOOKUP_TIMEOUT_SECONDS', 0.01)
    assert [item async for item in scrape_community_totals(
        cast(Any, None), ('conspiracy',),
    )] == []

    @asynccontextmanager
    async def cancelled(*args: Any) -> AsyncGenerator[Any]:
        raise asyncio.CancelledError
        yield  # pragma: no cover

    monkeypatch.setattr(totals, 'http_response', cancelled)

    with pytest.raises(asyncio.CancelledError):
        await anext(scrape_community_totals(cast(Any, None), ('conspiracy',)))


async def test_normal_collection_saves_totals_before_listing_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    total = _parse_total(archive_payload(), 'conspiracy', 'https://archive.test')
    lookups: list[tuple[str, ...]] = []

    async def discover(
        http: Any, communities: tuple[str, ...],
    ) -> AsyncGenerator[Any]:
        lookups.append(communities)

        for name in communities:
            yield total._replace(community=name)

    async def broken_listing(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        # A later browser error cannot discard the enrichment already saved.
        client.insert_rows.assert_awaited_once()
        raise RuntimeError('listing unavailable')
        yield  # pragma: no cover

    monkeypatch.setattr(runner, 'scrape_community_totals', discover)
    monkeypatch.setattr(runner, 'scrape_reddit_communities', broken_listing)
    source = SourceConfig('reddit.com', (), feed_urls=(
        'https://www.reddit.com/r/Conspiracy/',
    ))
    settings = load_settings('config.example.toml')._replace(
        media_download_types=(),
    )
    seen: set[str] = set()

    for _ in range(2):
        with pytest.raises(RuntimeError, match='listing unavailable'):
            async for _ in _discover_source_batches(
                settings, source, client, cast(Any, None), cast(Any, None),
                feed_staleness=dt.timedelta(days=1), batch_size=50,
                totals_seen=seen,
            ):
                pass

    assert lookups == [('conspiracy',), ()]
    table, rows = client.insert_rows.call_args.args
    assert table == 'reddit_community_totals'
    assert rows == [{**total._asdict(), 'source': 'arctic_shift'}]


async def test_archive_failure_does_not_stop_listing_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def response(*args: Any) -> AsyncGenerator[Any]:
        raise TimeoutError('archive unavailable')
        yield  # pragma: no cover

    async def listing(*args: Any, **kwargs: Any) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([], [])

    monkeypatch.setattr(totals, 'http_response', response)
    monkeypatch.setattr(runner, 'scrape_reddit_communities', listing)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    source = SourceConfig('reddit.com', (), feed_urls=(
        'https://www.reddit.com/r/conspiracy/',
    ))
    batches = [batch async for batch in _discover_source_batches(
        load_settings('config.example.toml'), source, client,
        cast(Any, None), cast(Any, None),
        feed_staleness=dt.timedelta(days=1), batch_size=50,
    )]
    assert batches == [ScrapeBatch([], [])]
    client.insert_rows.assert_not_awaited()
