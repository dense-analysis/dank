from __future__ import annotations

import datetime
import logging
import pathlib
from urllib.parse import urlparse

import aiohttp

from dank.model import AssetDiscovery, RawAsset

logger = logging.getLogger(__name__)


async def download_file_http(
    *,
    discovery: AssetDiscovery,
    target_dir: pathlib.Path,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None,
    timestamp: datetime.datetime,
) -> RawAsset | None:
    target_dir.mkdir(parents=True, exist_ok=True)

    parsed = urlparse(discovery.url)
    filename = pathlib.Path(parsed.path).name or "asset"
    target_path = target_dir / filename

    # Only download assets if we don't already have them.
    if not target_path.exists():
        # Create a temporary path for the download.
        # We'll move completed downloads to the real path when they are done.
        temp_path = target_path.with_suffix(f"{target_path.suffix}.part")

        try:
            async with http_client.get(
                discovery.url,
                allow_redirects=True,
                max_redirects=5,
            ) as response:
                response.raise_for_status()
                content_length = response.content_length
                bytes_read = 0
                exceeded_limit = (
                    max_asset_bytes is not None
                    and content_length is not None
                    and content_length > max_asset_bytes
                )

                # Stream file bytes to the system if we think we won't be over
                # the download limit.
                if not exceeded_limit:
                    with temp_path.open("wb") as file:
                        async for chunk in response.content.iter_chunked(
                            65536,
                        ):
                            if not chunk:
                                continue

                            bytes_read += len(chunk)

                            # Stop if the bytes we've downloaded have exceed
                            # the download limit, even though content length
                            # reported a smaller size.
                            if (
                                max_asset_bytes is not None
                                and bytes_read > max_asset_bytes
                            ):
                                exceeded_limit = True
                                break

                            file.write(chunk)

                if exceeded_limit:
                    raise OSError(
                        (
                            f"Exceeded maximum download size "
                            f"of {max_asset_bytes} "
                            f"with Content-Length: {content_length} "
                            f"and {bytes_read} bytes downloaded"
                        ),
                    )
        except Exception:
            logger.debug("Failed to download %s", discovery.url, exc_info=True)

            # Delete partial downloads when downloading fails.
            temp_path.unlink(missing_ok=True)
            target_path = None
        else:
            temp_path.replace(target_path)

    return RawAsset(
        domain=discovery.domain,
        post_id=discovery.post_id,
        url=discovery.url,
        asset_type=discovery.asset_type,
        scraped_at=timestamp,
        source=discovery.source,
        local_path=str(target_path) if target_path else "",
    )
