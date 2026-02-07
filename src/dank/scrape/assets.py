from __future__ import annotations

import asyncio
import datetime
import pathlib
from collections.abc import Iterable
from urllib.parse import urlparse

import aiohttp

from dank.model import AssetDiscovery, RawAsset

SKIP_ASSET_TYPES = {"iframe", "link", "youtube"}


async def download_assets(
    discoveries: Iterable[AssetDiscovery],
    *,
    assets_dir: pathlib.Path,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None = None,
    concurrency: int = 4,
    scraped_at: datetime.datetime | None = None,
) -> list[RawAsset]:
    timestamp = scraped_at or datetime.datetime.now(datetime.UTC)
    unique: dict[str, AssetDiscovery] = {}

    for discovery in discoveries:
        if discovery.url:
            unique.setdefault(discovery.url, discovery)

    semaphore = asyncio.Semaphore(concurrency)

    async def _download(discovery: AssetDiscovery) -> RawAsset | None:
        if discovery.asset_type in SKIP_ASSET_TYPES:
            return RawAsset(
                domain=discovery.domain,
                post_id=discovery.post_id,
                url=discovery.url,
                asset_type=discovery.asset_type,
                scraped_at=timestamp,
                source=discovery.source,
                local_path="",
            )

        parsed = urlparse(discovery.url)
        filename = pathlib.Path(parsed.path).name or "asset"
        target_dir = assets_dir / discovery.domain / discovery.post_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / filename

        if target_path.exists():
            return RawAsset(
                domain=discovery.domain,
                post_id=discovery.post_id,
                url=discovery.url,
                asset_type=discovery.asset_type,
                scraped_at=timestamp,
                source=discovery.source,
                local_path=str(target_path),
            )

        temp_path = target_path.with_suffix(f"{target_path.suffix}.part")
        async with semaphore:
            try:
                async with http_client.get(discovery.url) as response:
                    response.raise_for_status()

                    if max_asset_bytes is not None:
                        content_length = response.content_length

                        if (
                            content_length is not None
                            and content_length > max_asset_bytes
                        ):
                            temp_path.unlink(missing_ok=True)

                            return RawAsset(
                                domain=discovery.domain,
                                post_id=discovery.post_id,
                                url=discovery.url,
                                asset_type=discovery.asset_type,
                                scraped_at=timestamp,
                                source=discovery.source,
                                local_path="",
                            )

                    bytes_read = 0
                    exceeded_limit = False

                    with temp_path.open("wb") as file:
                        async for chunk in response.content.iter_chunked(
                            65536,
                        ):
                            if not chunk:
                                continue

                            bytes_read += len(chunk)

                            if (
                                max_asset_bytes is not None
                                and bytes_read > max_asset_bytes
                            ):
                                exceeded_limit = True
                                break

                            file.write(chunk)

                    if exceeded_limit:
                        temp_path.unlink(missing_ok=True)

                        return RawAsset(
                            domain=discovery.domain,
                            post_id=discovery.post_id,
                            url=discovery.url,
                            asset_type=discovery.asset_type,
                            scraped_at=timestamp,
                            source=discovery.source,
                            local_path="",
                        )
            except Exception:
                temp_path.unlink(missing_ok=True)

                return None

        temp_path.replace(target_path)

        return RawAsset(
            domain=discovery.domain,
            post_id=discovery.post_id,
            url=discovery.url,
            asset_type=discovery.asset_type,
            scraped_at=timestamp,
            source=discovery.source,
            local_path=str(target_path),
        )

    results = await asyncio.gather(
        *(_download(discovery) for discovery in unique.values()),
    )

    return [result for result in results if result is not None]
