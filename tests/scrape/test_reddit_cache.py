import datetime as dt
import json
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.config import RedditSettings
from dank.model import RawPost
from dank.scrape.reddit import (
    _capture_batch_details,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.reddit.cache import (
    DetailSnapshot,
    listing_signature,
    reuse_detail,
)
from dank.scrape.reddit.html import extract_html_posts
from dank.scrape.runner import (
    _load_reddit_details,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult

NOW = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
CARD = ('<shreddit-post id="t3_abc" post-title="Post" author="author" '
        'permalink="/r/python/comments/abc/post/" comment-count="4">'
        '<div slot="text-body"><p>Preview</p></div></shreddit-post>')


def fixture() -> tuple[RawPost, DetailSnapshot]:
    listing = extract_html_posts(CARD, 'python')[0]
    raw = RawPost('reddit.com', 't3_abc', str(listing['permalink']), None,
                  NOW, 'reddit', '', json.dumps(listing))
    previous = {
        **listing, 'selftext_html': '<p>Full body</p>',
        'detail_capture': {'status': 'captured'},
        'comment_capture': {'status': 'captured', 'sort': 'best', 'limit': 50,
                            'comments': [{'id': 't1_first', 'body': 'Reply'}]},
        'raw': {'listing': listing['raw'], 'detail': {'html': 'full html'}},
    }

    return raw, DetailSnapshot(previous, NOW - dt.timedelta(hours=1))


def test_legacy_archive_reused_with_fresh_score_and_original_capture_age():
    raw, cached = fixture()
    listing = json.loads(raw.payload)
    listing['score'] = 200
    raw = raw._replace(payload=json.dumps(listing))
    before = json.dumps(cached.payload)
    reused = reuse_detail(raw, cached, RedditSettings())
    assert reused is not None
    payload = json.loads(reused.payload)
    assert payload['score'] == 200
    assert payload['selftext_html'] == '<p>Full body</p>'
    assert payload['detail_capture']['reused'] is True
    assert (
        payload['comment_capture']['captured_at']
        == cached.scraped_at.isoformat()
    )
    assert payload['raw']['listing'] == listing['raw']
    assert payload['raw']['detail'] == {'html': 'full html'}
    assert reused.scraped_at == NOW
    assert json.dumps(cached.payload) == before
    # Saving a cache hit must not extend the original 24-hour expiry.
    second = DetailSnapshot(payload, NOW)
    assert reuse_detail(raw._replace(scraped_at=NOW + dt.timedelta(hours=24)),
                        second, RedditSettings()) is None


@pytest.mark.parametrize(('field', 'value'), [
    ('title', 'Edited'), ('selftext_html', '<p>Changed</p>'),
    ('num_comments', 5), ('url', 'https://example.test/changed'),
    ('author', '[deleted]'), ('media', [{'url': 'https://i.redd.it/new.jpg'}]),
])
def test_listing_changes_require_new_detail(field: str, value: object):
    raw, cached = fixture()
    listing = json.loads(raw.payload)
    listing[field] = value
    assert reuse_detail(raw._replace(payload=json.dumps(listing)), cached,
                        RedditSettings()) is None


@pytest.mark.parametrize('settings', [
    RedditSettings(detail_cache_hours=0),
    RedditSettings(detail_cache_hours=.5),
    RedditSettings(comment_limit=100),
])
def test_expiry_disabled_cache_and_changed_comment_options(
    settings: RedditSettings,
):
    raw, cached = fixture()
    assert reuse_detail(raw, cached, settings) is None


@pytest.mark.parametrize('field', ['detail_capture', 'comment_capture'])
def test_failed_captures_are_not_reused(field: str):
    raw, cached = fixture()
    cast(dict[str, Any], cached.payload[field])['status'] = 'failed'
    assert reuse_detail(raw, cached, RedditSettings()) is None


async def test_reuse_skips_browser_but_emits_post_and_assets(
    monkeypatch: pytest.MonkeyPatch,
):
    raw, cached = fixture()
    cached.payload['media'] = [{'url': 'https://i.redd.it/picture.jpg',
                               'asset_type': 'photo'}]
    visit = AsyncMock(side_effect=AssertionError('No browser access expected'))
    monkeypatch.setattr('dank.scrape.reddit.capture_post_detail', visit)
    results = [batch async for batch in _capture_batch_details(
        ScrapeBatch([raw], []), None, RedditSettings(), {raw.post_id: cached},
    )]
    result = results[0]
    assert len(result.posts) == len(result.assets) == 1
    assert result.assets[0].url == 'https://i.redd.it/picture.jpg'
    visit.assert_not_awaited()


async def test_runner_loads_only_successful_recent_target_community_details():
    raw, cached = fixture()
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {'post_id': raw.post_id, 'payload': json.dumps(cached.payload),
         'scraped_at': cached.scraped_at.isoformat()},
        {'post_id': 'broken', 'payload': '{}', 'scraped_at': 'bad'},
    ])
    loaded = await _load_reddit_details(client, ('Python',), RedditSettings())
    assert list(loaded) == [raw.post_id]
    sql, params = client.fetch_json.await_args.args
    assert "= 'captured'" in sql
    assert 'LIMIT 1 BY post_id' in sql
    assert params['communities'] == ['python']
    assert params['cutoff'].tzinfo == dt.UTC
    client.reset_mock()
    assert await _load_reddit_details(
        client, ('python',), RedditSettings(detail_cache_hours=0),
    ) == {}
    client.fetch_json.assert_not_awaited()


def test_signed_media_queries_do_not_change_listing_identity():
    raw, _ = fixture()
    listing = json.loads(raw.payload)
    listing['media'] = [{'url': 'https://v.redd.it/abc/DASH.mp4?s=old',
                         'asset_type': 'video'}]
    signature = listing_signature(listing)
    listing['media'][0]['url'] = 'https://v.redd.it/abc/DASH.mp4?s=new'
    assert listing_signature(listing) == signature


def test_legacy_merged_listing_captures_are_eligible_for_reuse():
    raw, cached = fixture()
    native = cast(dict[str, Any], cached.payload['raw'])
    original = native['listing']
    native['listing'] = {
        'captures': [{'captures': [original, original]}, original],
    }
    assert reuse_detail(raw, cached, RedditSettings()) is not None


async def test_cache_hit_after_rate_limit_does_not_resume_network_requests(
    monkeypatch: pytest.MonkeyPatch,
):
    from dank.scrape.reddit.details import defer_post_detail

    raw, cached = fixture()
    failed = raw._replace(post_id='t3_fail')
    other = raw._replace(post_id='t3_other')
    visit = AsyncMock(return_value=defer_post_detail(failed, RedditSettings()))
    monkeypatch.setattr('dank.scrape.reddit.capture_post_detail', visit)
    batches = [batch async for batch in _capture_batch_details(
        ScrapeBatch([failed, raw, other], []), None, RedditSettings(),
        {raw.post_id: cached},
    )]
    posts = [post for batch in batches for post in batch.posts]
    assert len(posts) == 3
    assert json.loads(posts[1].payload)['detail_capture']['reused']
    assert (
        json.loads(posts[2].payload)['detail_capture']['reason']
        == 'rate_limited'
    )
    visit.assert_awaited_once()


async def test_completed_details_are_yielded_before_visiting_the_next_post(
    monkeypatch: pytest.MonkeyPatch,
):
    raw, _ = fixture()
    visit = AsyncMock(side_effect=[raw, RuntimeError('second visit failed')])
    monkeypatch.setattr('dank.scrape.reddit.capture_post_detail', visit)
    batches = _capture_batch_details(
        ScrapeBatch([raw, raw._replace(post_id='t3_second')], []),
        None, RedditSettings(),
    )
    first = await anext(batches)
    assert first.posts == [raw]
    visit.assert_awaited_once()

    with pytest.raises(RuntimeError, match='second visit'):
        await anext(batches)
