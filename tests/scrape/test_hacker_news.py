from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest

from dank.config import (
    HackerNewsFilter,
    HackerNewsSettings,
    HackerNewsSourceConfig,
    SourceConfig,
)
from dank.model import RawPost
from dank.scrape import hacker_news
from dank.scrape.hacker_news import (
    HCKRNEWS_URL,
    HN_API_URL,
    _day_posts,  # pyright: ignore[reportPrivateUsage]
    _group_entries,  # pyright: ignore[reportPrivateUsage]
    _parse_homepage,  # pyright: ignore[reportPrivateUsage]
    scrape_hacker_news,
    scrape_hacker_news_details,
)
from dank.scrape.metrics import SourceMetrics, current_source

DAY = dt.datetime(2026, 10, 15, tzinfo=dt.UTC)


@pytest.fixture(autouse=True)
def no_retry_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hacker_news, 'retry_sleep', AsyncMock())


def entry(
    post_id: int, points: int, hours: float, day: dt.datetime = DAY,
) -> dict[str, Any]:
    return {
        'id': str(post_id), 'date': int((day + dt.timedelta(
            hours=hours,
        )).timestamp()),
        'points': points, 'comments': 2, 'link_text': f'Story {post_id}',
        'link': f'https://example.test/{post_id}', 'type': 'story',
    }


def homepage(entries: list[dict[str, Any]]) -> str:
    return '<ul class="entries">' + ''.join(
        f'<li class="entry row" id="{raw["id"]}">'
        f'<a class="hn story" data-date="{raw["date"]}">'
        f'<span class="comments">{raw["comments"]}</span>'
        f'<span class="points homepage">{raw["points"]}</span></a>'
        f'<a class="link story" href="{raw["link"]}">'
        f'{raw["link_text"]} <span class="source">(example.test)</span>'
        '</a></li>' for raw in entries
    ) + '</ul>'


def raw_post() -> RawPost:
    grouped = _group_entries([entry(1, 100, 12)])

    return _day_posts(grouped[DAY.date()], 'all', HCKRNEWS_URL)[0]


def mock_http(
    monkeypatch: pytest.MonkeyPatch, responses: dict[str, Any],
) -> list[str]:
    calls: list[str] = []

    @asynccontextmanager
    async def response(
        client: Any, url: str, **kwargs: Any,
    ) -> AsyncGenerator[Any]:
        calls.append(url)
        body = responses[url]

        if isinstance(body, Exception):
            raise body

        result = cast(Any, type('Response', (), {})())
        result.raise_for_status = lambda: None
        result.text = AsyncMock(return_value=(
            body if isinstance(body, str) else json.dumps(body)
        ))
        yield result

    monkeypatch.setattr(hacker_news, 'http_response', response)

    return calls


def test_homepage_preserves_original_html_and_decodes_title() -> None:
    document = homepage([entry(12, 42, 1)]).replace(
        'Story 12', 'A &amp; B <em>work</em>',
    )
    parsed = _parse_homepage(document)
    assert len(parsed) == 1
    assert parsed[0]['link_text'] == 'A & B work'
    assert parsed[0]['id'] == '12'
    assert parsed[0]['homepage'] is True
    assert parsed[0]['html'] == document.removeprefix(
        '<ul class="entries">',
    ).removesuffix('</ul>')
    post = _day_posts(
        _group_entries(parsed)[DAY.date()], 'all', HCKRNEWS_URL,
    )[0]
    assert post.url == 'https://news.ycombinator.com/item?id=12'
    assert post.post_created_at == DAY + dt.timedelta(hours=1)
    assert post.source == 'hacker_news'
    assert 'comments' not in json.loads(post.payload)


def test_missing_homepage_structure_fails_explicitly() -> None:
    with pytest.raises(ValueError, match='no entries'):
        _parse_homepage('<html>Service temporarily unavailable</html>')


def test_daily_filters_include_ties_and_keep_chronological_order() -> None:
    rows = [entry(index + 1, 100 - index, index) for index in range(24)]
    rows[10]['points'] = rows[9]['points']
    grouped = _group_entries(rows)[DAY.date()]
    top = _day_posts(grouped, 'top10', HCKRNEWS_URL)
    assert len(top) == 11
    assert [post.post_id for post in top] == [str(n) for n in range(11, 0, -1)]
    assert json.loads(top[0].payload)['hacker_news'] == {
        'day': '2026-10-15', 'rank': 10,
        'filters': ['top10', 'top20', 'top50', 'all'],
    }
    assert len(_day_posts(grouped, 'top20', HCKRNEWS_URL)) == 20
    assert len(_day_posts(grouped, 'top50', HCKRNEWS_URL)) == 12
    assert len(_day_posts(grouped, 'all', HCKRNEWS_URL)) == 24


def test_partial_day_cutoff_matches_hckrnews_observed_hours() -> None:
    rows = [entry(index + 1, 100 - index, index / 2) for index in range(24)]
    grouped = _group_entries(rows)[DAY.date()]
    assert len(_day_posts(grouped, 'top10', HCKRNEWS_URL)) == 5
    assert len(_day_posts(grouped, 'top20', HCKRNEWS_URL)) == 10
    assert len(_day_posts(grouped, 'top50', HCKRNEWS_URL)) == 6


def test_zero_hour_span_and_zero_points_match_source_cutoff() -> None:
    grouped = _group_entries([entry(1, 3, 1), entry(2, 0, 1.1)])[DAY.date()]
    assert [post.post_id for post in _day_posts(
        grouped, 'top50', HCKRNEWS_URL,
    )] == ['1']
    assert len(_day_posts(grouped, 'all', HCKRNEWS_URL)) == 2


def test_utc_groups_deduplicate_without_rewriting_raw_fields() -> None:
    first = entry(1, 2, 23.9)
    second = entry(2, 3, 24)
    grouped = _group_entries([first, second, first, None, {'id': 'x'}])
    assert list(grouped) == [DAY.date(), (DAY + dt.timedelta(days=1)).date()]
    assert len(grouped[DAY.date()]) == 1
    payload = json.loads(_day_posts(
        grouped[DAY.date()], 'all', HCKRNEWS_URL,
    )[0].payload)
    assert payload['hckrnews'] == first


async def test_listing_reads_live_days_then_archive_without_comment_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = dt.datetime.now(dt.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    yesterday = today - dt.timedelta(days=1)
    archive_day = today - dt.timedelta(days=2)
    archive_url = f'{HCKRNEWS_URL}data/{archive_day:%Y%m%d}.js'
    calls = mock_http(monkeypatch, {
        HCKRNEWS_URL: homepage([
            entry(1, 2, 1, today), entry(2, 3, 1, yesterday),
        ]),
        archive_url: [entry(3, 4, 1, archive_day)],
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'by': 'author', 'time': 123,
            'kids': [20],
        },
        HN_API_URL + '2.json': {'id': 2, 'type': 'story'},
        HN_API_URL + '3.json': {'id': 3, 'type': 'story'},
    })
    source = SourceConfig(
        'news.ycombinator.com', (), hacker_news=HackerNewsSourceConfig(
            HackerNewsFilter.ALL,
        ),
    )
    batches = [batch async for batch in scrape_hacker_news(
        HackerNewsSettings(days=3), source, cast(Any, None),
    )]
    assert [post.post_id for batch in batches for post in batch.posts] == [
        '1', '2', '3',
    ]
    assert calls == [
        HCKRNEWS_URL, HN_API_URL + '1.json', HN_API_URL + '2.json',
        archive_url, HN_API_URL + '3.json',
    ]
    payload = json.loads(batches[0].posts[0].payload)
    assert payload['story']['by'] == 'author'
    assert payload['story']['time'] == 123
    assert 'comments' not in payload
    assert batches[0].posts[0].post_created_at == today + dt.timedelta(hours=1)
    assert all(not batch.assets for batch in batches)


async def test_archive_failure_does_not_discard_other_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = dt.datetime.now(dt.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    old_day = today - dt.timedelta(days=2)
    missing_day = today - dt.timedelta(days=1)
    mock_http(monkeypatch, {
        HCKRNEWS_URL: homepage([entry(1, 2, 1, today)]),
        HN_API_URL + '1.json': {'id': 1, 'type': 'story'},
        HN_API_URL + '3.json': {'id': 3, 'type': 'story'},
        f'{HCKRNEWS_URL}data/{missing_day:%Y%m%d}.js':
            aiohttp.ClientConnectionError('offline'),
        f'{HCKRNEWS_URL}data/{old_day:%Y%m%d}.js': [entry(3, 4, 1, old_day)],
    })
    batches = [batch async for batch in scrape_hacker_news(
        HackerNewsSettings(days=3), SourceConfig('news.ycombinator.com', ()),
        cast(Any, None),
    )]
    assert [post.post_id for batch in batches for post in batch.posts] == [
        '1', '3',
    ]


async def test_details_preserve_native_items_and_bound_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    story = {'id': 1, 'type': 'story', 'kids': [2, 3], 'descendants': 3}
    first = {
        'id': 2, 'type': 'comment', 'text': '<p>Hi</p>', 'kids': [4],
        'parent': 1,
    }
    second = {'id': 3, 'type': 'comment', 'text': 'Other', 'parent': 1}
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': story,
        HN_API_URL + '2.json': first,
        HN_API_URL + '3.json': second,
    })
    post = raw_post()
    batches = [batch async for batch in scrape_hacker_news_details(
        HackerNewsSettings(comment_limit=2), [post], cast(Any, None),
    )]
    detailed = batches[0].posts[0]
    body = json.loads(detailed.payload)
    assert body['story'] == story
    assert body['comments'] == [first, second]
    assert body['hckrnews'] == json.loads(post.payload)['hckrnews']
    assert body['hacker_news'] == json.loads(post.payload)['hacker_news']
    assert body['comment_capture'] == {
        'status': 'partial', 'limit': 2, 'captured': 2, 'requests': 3,
        'total': 3, 'reason': 'comment_limit',
    }
    assert len(calls) == 3
    assert detailed.post_created_at == post.post_created_at


async def test_details_finish_small_tree_and_ignore_duplicate_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'kids': [2, 2], 'descendants': 1,
        },
        HN_API_URL + '2.json': {
            'id': 2, 'deleted': True, 'kids': [1],
        },
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(comment_limit=1), [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['comment_capture']['status'] == 'captured'
    assert len(calls) == 2
    assert body['comments'][0]['deleted'] is True


async def test_details_failure_is_explicit_and_keeps_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_http(monkeypatch, {
        HN_API_URL + '1.json': aiohttp.ClientConnectionError('offline'),
    })
    original = raw_post()
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [original], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['comment_capture']['status'] == 'failed'
    assert body['comment_capture']['captured'] == 0
    assert body['comments'] == []
    assert body['hckrnews'] == json.loads(original.payload)['hckrnews']


async def test_details_failure_after_first_comment_preserves_partial_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_http(monkeypatch, {
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'kids': [2, 3], 'descendants': 2,
        },
        HN_API_URL + '2.json': {
            'id': 2, 'type': 'comment', 'text': 'Saved', 'parent': 1,
        },
        HN_API_URL + '3.json': None,
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['comment_capture']['status'] == 'partial'
    assert body['comment_capture']['captured'] == 1
    assert body['comments'][0]['text'] == 'Saved'


async def test_details_timeout_and_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def waiting(*args: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        await asyncio.Event().wait()
        yield  # pragma: no cover

    monkeypatch.setattr(hacker_news, 'http_response', waiting)
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(detail_timeout_seconds=0.01),
        [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['comment_capture']['reason'] == 'timeout'
    assert body['comment_capture']['status'] == 'failed'

    @asynccontextmanager
    async def cancelled(*args: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        raise asyncio.CancelledError
        yield  # pragma: no cover

    monkeypatch.setattr(hacker_news, 'http_response', cancelled)

    with pytest.raises(asyncio.CancelledError):
        await anext(scrape_hacker_news_details(
            HackerNewsSettings(), [raw_post()], cast(Any, None),
        ))


async def test_details_respect_maximum_posts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': {'id': 1, 'type': 'story', 'descendants': 0},
    })
    batches = [batch async for batch in scrape_hacker_news_details(
        HackerNewsSettings(detail_max_posts=1), [raw_post(), raw_post()],
        cast(Any, None),
    )]
    assert len(batches) == 1
    assert len(calls) == 1
    assert json.loads(batches[0].posts[0].payload)[
        'comment_capture'
    ]['status'] == 'captured'


@pytest.mark.parametrize('archive', [[], {}, [None], [entry(7, 20, 1)]])
async def test_unrecognized_archive_day_records_failure(
    monkeypatch: pytest.MonkeyPatch, archive: object,
) -> None:
    today = dt.datetime.now(dt.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    missing = today - dt.timedelta(days=1)
    source = SourceConfig('news.ycombinator.com', ())
    stats = SourceMetrics(1, source)
    mock_http(monkeypatch, {
        HCKRNEWS_URL: homepage([entry(1, 5, 1, today)]),
        HN_API_URL + '1.json': {'id': 1, 'type': 'story'},
        f'{HCKRNEWS_URL}data/{missing:%Y%m%d}.js': archive,
    })
    token = current_source.set(stats)

    try:
        batches = [batch async for batch in scrape_hacker_news(
            HackerNewsSettings(days=2), source, cast(Any, None),
        )]
    finally:
        current_source.reset(token)

    assert len(batches) == 1
    assert stats.counts['parse_failures'] >= 1


async def test_failed_homepage_still_collects_available_archives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = dt.datetime.now(dt.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    mock_http(monkeypatch, {
        HCKRNEWS_URL: aiohttp.ClientConnectionError('offline'),
        f'{HCKRNEWS_URL}data/{today:%Y%m%d}.js': [entry(3, 4, 1, today)],
        HN_API_URL + '3.json': {'id': 3, 'type': 'story'},
    })
    batch = await anext(scrape_hacker_news(
        HackerNewsSettings(days=1), SourceConfig('news.ycombinator.com', ()),
        cast(Any, None),
    ))
    assert batch.posts[0].post_id == '3'


async def test_unrelated_comment_parent_is_not_captured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_http(monkeypatch, {
        HN_API_URL + '1.json': {'id': 1, 'type': 'story', 'kids': [2]},
        HN_API_URL + '2.json': {
            'id': 2, 'type': 'comment', 'parent': 99, 'text': 'Unrelated',
        },
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['comments'] == []
    assert body['comment_capture']['status'] == 'failed'
    assert 'parent' in body['comment_capture']['reason']


async def test_nested_replies_follow_their_actual_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_http(monkeypatch, {
        HN_API_URL + '1.json': {'id': 1, 'type': 'story', 'kids': [2]},
        HN_API_URL + '2.json': {
            'id': 2, 'type': 'comment', 'parent': 1, 'kids': [3],
            'text': 'Parent',
        },
        HN_API_URL + '3.json': {
            'id': 3, 'type': 'comment', 'parent': 2, 'text': 'Child',
        },
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert [item['parent'] for item in body['comments']] == [1, 2]
    assert body['comment_capture']['status'] == 'captured'


async def test_deleted_story_tombstone_is_preserved_as_failed_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tombstone = {'id': 1, 'deleted': True}
    mock_http(monkeypatch, {HN_API_URL + '1.json': tombstone})
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    body = json.loads(batch.posts[0].payload)
    assert body['story'] == tombstone
    assert body['comment_capture']['status'] == 'failed'
    assert body['hckrnews'] == json.loads(raw_post().payload)['hckrnews']


async def test_default_details_capture_more_than_500_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    comments = {
        HN_API_URL + f'{item_id}.json': {
            'id': item_id, 'type': 'comment',
            'parent': 1 if item_id <= 301 else item_id - 300,
            'kids': [item_id + 300] if item_id <= 301 else [],
            'text': f'Comment {item_id}',
        } for item_id in range(2, 602)
    }
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'kids': list(range(2, 302)),
            'descendants': 600,
        },
        **comments,
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    payload = json.loads(batch.posts[0].payload)
    assert len(payload['comments']) == 600
    assert payload['comment_capture']['status'] == 'captured'
    assert payload['comment_capture']['limit'] is None
    assert len(calls) == 601
    assert payload['comments'][-1]['parent'] == 301


async def test_resume_refreshes_saved_leaves_and_captures_new_children(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = raw_post()
    payload = json.loads(original.payload)
    payload['comments'] = [
        {'id': 2, 'type': 'comment', 'parent': 1, 'text': 'Old'},
    ]
    payload['comment_capture'] = {
        'status': 'partial', 'reason': 'comment_limit', 'captured': 1,
    }
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'kids': [2], 'descendants': 2,
        },
        HN_API_URL + '2.json': {
            'id': 2, 'type': 'comment', 'parent': 1, 'text': 'Updated',
            'kids': [3],
        },
        HN_API_URL + '3.json': {
            'id': 3, 'type': 'comment', 'parent': 2, 'text': 'New reply',
        },
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [original._replace(payload=json.dumps(payload))],
        cast(Any, None),
    ))
    result = json.loads(batch.posts[0].payload)
    assert result['comment_capture']['status'] == 'captured'
    assert [comment['text'] for comment in result['comments']] == [
        'Updated', 'New reply',
    ]
    assert len(calls) == 3


async def test_failed_comment_refresh_keeps_saved_tree_and_other_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = raw_post()
    payload = json.loads(original.payload)
    payload['comments'] = [
        {
            'id': 2, 'type': 'comment', 'parent': 1,
            'kids': [4], 'text': 'Saved',
        },
        {'id': 4, 'type': 'comment', 'parent': 2, 'text': 'Saved reply'},
    ]
    mock_http(monkeypatch, {
        HN_API_URL + '1.json': {
            'id': 1, 'type': 'story', 'kids': [2, 3], 'descendants': 3,
        },
        HN_API_URL + '2.json': aiohttp.ClientConnectionError('offline'),
        HN_API_URL + '3.json': {
            'id': 3, 'type': 'comment', 'parent': 1, 'text': 'Other branch',
        },
        HN_API_URL + '4.json': {
            'id': 4, 'type': 'comment', 'parent': 2, 'text': 'Updated reply',
        },
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [original._replace(payload=json.dumps(payload))],
        cast(Any, None),
    ))
    result = json.loads(batch.posts[0].payload)
    assert result['comment_capture']['status'] == 'partial'
    assert result['comment_capture']['unresolved_ids'] == [2]
    comments = {item['id']: item['text'] for item in result['comments']}
    assert comments == {
        2: 'Saved', 3: 'Other branch', 4: 'Updated reply',
    }


async def test_failed_story_refresh_does_not_erase_saved_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = raw_post()
    payload = json.loads(original.payload)
    payload['comments'] = [
        {'id': 2, 'type': 'comment', 'parent': 1, 'text': 'Saved'},
    ]
    calls = mock_http(monkeypatch, {
        HN_API_URL + '1.json': aiohttp.ClientConnectionError('offline'),
    })
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [original._replace(payload=json.dumps(payload))],
        cast(Any, None),
    ))
    result = json.loads(batch.posts[0].payload)
    assert result['comments'] == payload['comments']
    assert result['comment_capture']['status'] == 'partial'
    assert len(calls) == 3


async def test_metadata_failure_preserves_archive_submission_time_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = dt.datetime.now(dt.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    archived = entry(1, 10, 1, today) | {'time': 123}
    mock_http(monkeypatch, {
        HCKRNEWS_URL: '<html>Unavailable</html>',
        f'{HCKRNEWS_URL}data/{today:%Y%m%d}.js': [archived],
        HN_API_URL + '1.json': aiohttp.ClientConnectionError('offline'),
    })
    batch = await anext(scrape_hacker_news(
        HackerNewsSettings(days=1), SourceConfig('news.ycombinator.com', ()),
        cast(Any, None),
    ))
    payload = json.loads(batch.posts[0].payload)
    assert payload['hckrnews']['time'] == 123
    assert 'story' not in payload
    assert payload['hacker_news']['day'] == today.date().isoformat()


async def test_comment_fetches_are_concurrent_but_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = 0
    peak = 0

    async def fetch(client: Any, item_id: int) -> dict[str, Any]:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.001)
        active -= 1

        if item_id == 1:
            return {'id': 1, 'type': 'story', 'kids': list(range(2, 30))}

        return {'id': item_id, 'type': 'comment', 'parent': 1, 'text': 'Hi'}

    monkeypatch.setattr(hacker_news, '_fetch_item', fetch)
    batch = await anext(scrape_hacker_news_details(
        HackerNewsSettings(), [raw_post()], cast(Any, None),
    ))
    payload = json.loads(batch.posts[0].payload)
    assert len(payload['comments']) == 28
    assert 1 < peak <= hacker_news.ITEM_BATCH_SIZE
