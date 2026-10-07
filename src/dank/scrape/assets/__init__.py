from __future__ import annotations

import asyncio
import datetime
import pathlib
from collections.abc import Iterable
from contextlib import nullcontext
from urllib.parse import urlparse

import aiohttp

from dank.config import MEDIA_DOWNLOAD_TYPES
from dank.model import AssetDiscovery, RawAsset
from dank.progress import Progress
from dank.scrape.http import current_limiter
from dank.scrape.metrics import (
    MEDIA_CONCURRENCY,
    SourceMetrics,
    count,
    current_source,
)

from .audio_video import download_audio_video_asset
from .http import download_file_http, http_asset_path

SKIP_ASSET_TYPES = {"iframe", "link"}
ASSET_FILENAMES_TO_NEVER_DOWNLOAD = {
    "loader.gif",
}


async def download_assets(
    discoveries: Iterable[AssetDiscovery],
    *,
    assets_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path | None = None,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None = None,
    concurrency: int = MEDIA_CONCURRENCY,
    download_types: tuple[str, ...] = MEDIA_DOWNLOAD_TYPES,
    scraped_at: datetime.datetime | None = None,
    owners: dict[AssetDiscovery, SourceMetrics] | None = None,
) -> list[RawAsset]:
    timestamp = scraped_at or datetime.datetime.now(datetime.UTC)
    unique: dict[str, AssetDiscovery] = {}

    for discovery in discoveries:
        if discovery.url:
            unique.setdefault(discovery.url, discovery)

    limiter = current_limiter.get()
    # Limit each batch's waiters so one large source cannot monopolise the run.
    semaphore = asyncio.Semaphore(
        limiter.settings.media_concurrency if limiter else concurrency,
    )
    shared = limiter.media if limiter else nullcontext()


    if not unique:
        return []

    domains = ", ".join(sorted({item.domain for item in unique.values()}))
    progress = Progress(f"Media for {domains}", len(unique))

    async def _tracked_download(discovery: AssetDiscovery) -> RawAsset | None:
        source = (owners or {}).get(discovery)
        token = current_source.set(source)

        try:
            if _skip_download(discovery, download_types):
                count("media_skipped")

                return RawAsset(
                    discovery.domain, discovery.post_id, discovery.url,
                    discovery.asset_type, timestamp, discovery.source, "",
                )

            async with semaphore, shared:
                if source is not None:
                    source.active_media += 1

                try:
                    before: set[pathlib.Path] = set()

                    if source is not None:
                        before = await asyncio.to_thread(
                            _existing_files, assets_dir, discovery,
                        )

                    result = await _download_one(
                        discovery, assets_dir, browser_profile_dir,
                        http_client, max_asset_bytes, timestamp,
                        download_types=download_types,
                    )

                    if source is not None:
                        outcome, size = await asyncio.to_thread(
                            _download_outcome, result, before,
                        )
                        count(outcome)
                        count("downloaded_file_bytes", size)

                    return result
                finally:
                    if source is not None:
                        source.active_media -= 1
        finally:
            current_source.reset(token)
            progress.advance()


    async with progress:
        tasks = [
            asyncio.create_task(_tracked_download(item))
            for item in unique.values()
        ]

        results = await _gather_downloads(tasks)

    return [result for result in results if result is not None]


async def _gather_downloads(
    tasks: list[asyncio.Task[RawAsset | None]],
) -> list[RawAsset | None]:
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        # gather does not cancel siblings when one task fails.
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _download_one(
    discovery: AssetDiscovery,
    assets_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path | None,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None,
    timestamp: datetime.datetime,
    *, download_types: tuple[str, ...] = MEDIA_DOWNLOAD_TYPES,
) -> RawAsset | None:
    target_dir = assets_dir / discovery.domain / discovery.post_id

    if discovery.asset_type == "youtube":
        # Download YouTube assets with yt-dlp.
        return await download_audio_video_asset(
            discovery=discovery,
            target_dir=target_dir,
            browser_profile_dir=browser_profile_dir,
            max_asset_bytes=max_asset_bytes,
            download_thumbnail="image" in download_types,
            timestamp=timestamp,
        )
    else:
        # Default to downloading assets with the HTTP client.
        return await download_file_http(
            discovery=discovery,
            target_dir=target_dir,
            http_client=http_client,
            max_asset_bytes=max_asset_bytes,
            timestamp=timestamp,
        )



def _skip_download(
    discovery: AssetDiscovery, download_types: tuple[str, ...],
) -> bool:
    if (
        discovery.asset_type in SKIP_ASSET_TYPES
        or pathlib.PurePosixPath(urlparse(discovery.url).path).name.lower()
        in ASSET_FILENAMES_TO_NEVER_DOWNLOAD
    ):
        return True

    kind = {"photo": "image", "youtube": "video", "animated_gif": "video"}.get(
        discovery.asset_type, discovery.asset_type,
    )

    if kind in MEDIA_DOWNLOAD_TYPES:
        return kind not in download_types

    # Preserve the legacy fallback only when all media types are allowed.
    return not set(MEDIA_DOWNLOAD_TYPES).issubset(download_types)


def _existing_files(
    assets_dir: pathlib.Path, discovery: AssetDiscovery,
) -> set[pathlib.Path]:
    target = assets_dir / discovery.domain / discovery.post_id

    if discovery.asset_type == "youtube":
        return {path.resolve() for path in target.glob("*")}

    path = http_asset_path(target, discovery.url)

    return {path.resolve()} if path.exists() else set()


def _download_outcome(
    result: RawAsset | None,
    before: set[pathlib.Path],
) -> tuple[str, int]:
    if result is None or not result.local_path:
        return "files_failed", 0

    path = pathlib.Path(result.local_path)

    if path.resolve() in before:
        return "files_cached", 0

    try:
        return "files_downloaded", path.stat().st_size
    except OSError:
        return "files_downloaded", 0
