import asyncio
import datetime as dt
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.config import SourceConfig
from dank.model import AssetDiscovery, RawAsset, RedditCommunityDiscovery
from dank.scrape import runner
from dank.scrape.metrics import SourceMetrics
from dank.scrape.runner import (
    _persist_community_icons,  # pyright: ignore[reportPrivateUsage]
    _process_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch

URL = 'https://styles.redditmedia.com/t5_test/styles/communityIcon_test.png'


def discovery(url: str = URL) -> RedditCommunityDiscovery:
    return RedditCommunityDiscovery(
        'LocalLLaMA', url, dt.datetime.now(dt.UTC),
        'https://www.reddit.com/r/LocalLLaMA/top/?t=day',
        '{"header":"original community identity"}',
    )


async def test_community_metadata_is_cached_without_creating_a_post(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    stats = SourceMetrics(1, SourceConfig('reddit.com', ()))
    stats.pending = 1
    stats.collected()
    assert stats.finished_at is None
    community = discovery()

    async def download(
        items: list[AssetDiscovery], **kwargs: Any,
    ) -> list[RawAsset]:
        assert stats.finished_at is None
        assert kwargs['max_asset_bytes'] == 1024 * 1024
        assert items[0].post_id == 'communities/localllama'
        target = tmp_path / 'community.png'
        target.write_bytes(b'\x89PNG\r\n\x1a\n')
        return [RawAsset(
            items[0].domain, items[0].post_id, items[0].url, 'image',
            community.scraped_at, 'reddit-community', str(target),
        )]

    monkeypatch.setattr(runner, 'download_assets', download)
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    await queue.put(ScrapeBatch([], [], 1, (community,)))
    await queue.put(None)
    history = cast(Any, type('History', (), {
        'sources': {1: stats}, 'queue_size': 1,
    })())
    totals = await _process_batches(
        queue, client, cast(Any, None), assets_dir=tmp_path,
        browser_profile_dir=tmp_path, max_asset_bytes=None,
        batch_size=50, history=history,
    )
    assert totals.posts == 0
    client.insert_rows.assert_awaited_once()
    table, rows = client.insert_rows.call_args.args
    assert table == 'reddit_community_icons'
    assert rows[0]['community'] == 'localllama'
    assert rows[0]['payload'] == community.payload
    assert rows[0]['content_type'] == 'image/png'
    assert stats.pending == 0 and stats.status == 'completed'


@pytest.mark.parametrize('url', [
    '', 'https://evil.test/icon.png', 'https://[malformed/icon.png',
    'https://user:secret@styles.redditmedia.com/icon.png',
])
async def test_missing_or_untrusted_icon_does_not_erase_cached_identity(
    url: str, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    download = AsyncMock()
    client = AsyncMock()
    monkeypatch.setattr(runner, 'download_assets', download)
    await _persist_community_icons(
        client, cast(Any, None), (discovery(url),), assets_dir=tmp_path,
        max_asset_bytes=None, download_types=('image',), owner=None,
    )
    download.assert_not_awaited()
    client.insert_rows.assert_not_awaited()


@pytest.mark.parametrize('payload', [None, b'<html>unavailable</html>'])
async def test_failed_or_non_image_refresh_keeps_previous_cached_icon(
    payload: bytes | None, tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'icon.png'
    if payload is not None:
        path.write_bytes(payload)
    community = discovery()
    download = AsyncMock(return_value=[RawAsset(
        'reddit.com', 'communities/localllama', URL, 'image',
        community.scraped_at, 'reddit-community',
        str(path) if payload is not None else '',
    )])
    monkeypatch.setattr(runner, 'download_assets', download)
    client = AsyncMock()
    await _persist_community_icons(
        client, cast(Any, None), (community,), assets_dir=tmp_path,
        max_asset_bytes=1000, download_types=('image',), owner=None,
    )
    assert download.call_args.kwargs['max_asset_bytes'] == 1000
    client.insert_rows.assert_not_awaited()


async def test_disabled_images_keep_metadata_queue_from_stalling(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    download = AsyncMock()
    monkeypatch.setattr(runner, 'download_assets', download)
    client = AsyncMock()
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    await queue.put(ScrapeBatch([], [], communities=(discovery(),)))
    await queue.put(None)
    await _process_batches(
        queue, client, cast(Any, None), assets_dir=tmp_path,
        browser_profile_dir=tmp_path, max_asset_bytes=None, batch_size=50,
        download_types=(),
    )
    download.assert_not_awaited()
    client.insert_rows.assert_not_awaited()


def test_community_schema_is_bundled_and_matches_fresh_database():
    schema = pathlib.Path('src/dank/storage/reddit.sql').read_text()
    qualified = schema.replace(
        'IF NOT EXISTS reddit_', 'IF NOT EXISTS dank.reddit_',
    )
    assert qualified in pathlib.Path('schema.sql').read_text()
    package = pathlib.Path('pyproject.toml').read_text()
    assert '"dank.storage" = ["reddit.sql", "raw_posts.sql"]' in package


async def test_invalid_icon_is_retried_on_the_next_collection(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'icon.png'
    path.write_bytes(b'<html>transient failure</html>')
    downloads = 0
    community = discovery()

    async def download(*args: Any, **kwargs: Any) -> list[RawAsset]:
        nonlocal downloads
        if not path.exists():
            downloads += 1
            path.write_bytes(b'\x89PNG\r\n\x1a\n')

        return [RawAsset(
            'reddit.com', 'communities/localllama', URL, 'image',
            community.scraped_at, 'reddit-community', str(path),
        )]

    monkeypatch.setattr(runner, 'download_assets', download)
    client = AsyncMock()
    for _ in range(2):
        await _persist_community_icons(
            client, cast(Any, None), (community,), assets_dir=tmp_path,
            max_asset_bytes=None, download_types=('image',), owner=None,
        )
    assert downloads == 1
    client.insert_rows.assert_awaited_once()
    assert path.read_bytes().startswith(b'\x89PNG')
