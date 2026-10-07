from __future__ import annotations

import asyncio
import datetime
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.config import MEDIA_DOWNLOAD_TYPES, ScrapeSettings, SourceConfig
from dank.model import AssetDiscovery, RawAsset
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import http_asset_path
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics
from dank.scrape.x.payloads import extract_posts_from_payload


@pytest.mark.parametrize("types, allowed", [
    (MEDIA_DOWNLOAD_TYPES, {
        "image", "photo", "audio", "video", "youtube", "animated_gif", "other",
    }),
    (("image",), {"image", "photo"}),
    (("audio",), {"audio"}),
    (("video",), {"video", "youtube", "animated_gif"}),
    ((), set[str]()),
])
async def test_only_selected_types_reach_downloaders(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
    types: tuple[str, ...], allowed: set[str],
) -> None:
    assets = [AssetDiscovery(
        "rss", "one.test", "post", f"https://one.test/{kind}", kind, None,
    ) for kind in (
        "image", "photo", "audio", "video", "youtube", "animated_gif",
        "other", "iframe", "link",
    )]
    stats = SourceMetrics(1, SourceConfig("one.test", ()))
    calls: list[tuple[str, str]] = []

    async def download(
        *, discovery: AssetDiscovery, timestamp: datetime.datetime,
        **kwargs: Any,
    ) -> RawAsset:
        calls.append((discovery.asset_type, discovery.url))

        # Simulate a failure to distinguish attempted downloads from skips.
        return RawAsset(discovery.domain, discovery.post_id, discovery.url,
                        discovery.asset_type, timestamp, discovery.source, "")

    http = AsyncMock(side_effect=download)
    youtube = AsyncMock(side_effect=download)
    monkeypatch.setattr("dank.scrape.assets.download_file_http", http)
    monkeypatch.setattr(
        "dank.scrape.assets.download_audio_video_asset", youtube,
    )
    results = await download_assets(
        assets, assets_dir=tmp_path, http_client=cast(Any, None),
        download_types=types, owners=dict.fromkeys(assets, stats),
    )
    assert {kind for kind, _ in calls} == allowed
    assert youtube.await_count == int("youtube" in allowed)
    assert http.await_count == len(allowed - {"youtube"})
    assert {asset.url for asset in results} == {asset.url for asset in assets}
    assert stats.counts["media_skipped"] == len(assets) - len(allowed)
    assert stats.counts["files_failed"] == len(allowed)
    assert stats.active_media == 0


async def test_disabled_types_do_not_wait_for_media_capacity(
    tmp_path: pathlib.Path,
) -> None:
    limiter = RequestLimiter(ScrapeSettings(media_concurrency=1))
    token = current_limiter.set(limiter)
    await limiter.media.acquire()
    directory = tmp_path / "untouched"

    try:
        async with asyncio.timeout(1):
            results = await download_assets(
                [AssetDiscovery("rss", "test", "post", "https://test/video",
                                "youtube", None)],
                assets_dir=directory, http_client=cast(Any, None),
                download_types=(),
            )

        assert len(results) == 1 and results[0].local_path == ""
        assert not await asyncio.to_thread(directory.exists)
    finally:
        limiter.media.release()
        current_limiter.reset(token)


async def test_disabled_downloads_leave_cached_files_intact(
    tmp_path: pathlib.Path,
) -> None:
    asset = AssetDiscovery("rss", "test", "post", "https://test/image.png",
                           "image", None)
    target = http_asset_path(tmp_path / "test/post", asset.url)
    await asyncio.to_thread(target.parent.mkdir, parents=True)
    await asyncio.to_thread(target.write_bytes, b"cached image")
    results = await download_assets(
        [asset], assets_dir=tmp_path, http_client=cast(Any, None),
        download_types=(),
    )
    assert results[0].url == asset.url
    assert results[0].local_path == ""
    assert await asyncio.to_thread(target.read_bytes) == b"cached image"


@pytest.mark.parametrize("kind", ["video", "animated_gif"])
async def test_x_posters_remain_eligible_in_image_only_mode(
    kind: str, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    poster = "https://pbs.twimg.com/poster.jpg"
    video = "https://video.twimg.com/clip.mp4"
    posts = extract_posts_from_payload({
        "__typename": "Tweet", "rest_id": "post",
        "legacy": {"full_text": "Clip", "entities": {"media": [{
            "media_url_https": poster, "type": kind,
            "video_info": {"variants": [{
                "url": video, "content_type": "video/mp4", "bitrate": 100,
            }]},
        }]}},
    })
    assert len(posts) == 1
    discoveries = [AssetDiscovery(
        "x", "x.com", "post", asset.url, asset.asset_type, None,
    ) for asset in posts[0].assets]
    http = AsyncMock(return_value=None)
    monkeypatch.setattr("dank.scrape.assets.download_file_http", http)
    await download_assets(
        discoveries, assets_dir=tmp_path, http_client=cast(Any, None),
        download_types=("image",),
    )
    assert http.await_count == 1
    assert http.call_args.kwargs["discovery"].url == poster
