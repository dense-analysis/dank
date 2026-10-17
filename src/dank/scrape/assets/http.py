from __future__ import annotations

import datetime
import hashlib
import logging
import pathlib
import uuid
from urllib.parse import urlparse, urlsplit

import aiohttp

from dank.media_urls import canonical_media_url
from dank.model import AssetDiscovery, RawAsset
from dank.scrape.http import http_response
from dank.scrape.metrics import count

from .limits import AssetSizeExceeded

logger = logging.getLogger(__name__)


def http_asset_path(target_dir: pathlib.Path, url: str) -> pathlib.Path:
    # The query string can identify an entirely different image on a CDN.
    digest = hashlib.sha256(url.encode()).hexdigest()
    suffix = pathlib.PurePosixPath(urlparse(url).path).suffix

    if len(suffix) > 16:
        suffix = ""

    return target_dir / f"{digest}{suffix}"


async def download_file_http(
    *,
    discovery: AssetDiscovery,
    target_dir: pathlib.Path,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None,
    timestamp: datetime.datetime,
) -> RawAsset | None:
    target_dir.mkdir(parents=True, exist_ok=True)  # noqa

    target_path = http_asset_path(target_dir, discovery.url)

    # Only download assets if we don't already have them.
    if not target_path.exists():
        # Create a temporary path for the download.
        # We'll move completed downloads to the real path when they are done.
        temp_path = target_path.with_name(
            f"{target_path.name}.{uuid.uuid4().hex}.part",
        )

        try:
            async with http_response(
                http_client, canonical_media_url(discovery.url),
                headers=_referrer_headers(discovery),
                preserve_url=urlparse(discovery.url).hostname in {
                    "preview.redd.it", "external-preview.redd.it",
                },
            ) as response:
                response.raise_for_status()
                content_length = response.content_length
                bytes_read = 0
                exceeded_limit = (
                    max_asset_bytes is not None and max_asset_bytes > 0
                    and content_length is not None
                    and content_length > max_asset_bytes
                )

                # Stream bytes while staying within the download limit.
                if not exceeded_limit:
                    with temp_path.open("wb") as file:
                        async for chunk in response.content.iter_chunked(
                            65536,
                        ):
                            if not chunk:
                                continue

                            bytes_read += len(chunk)

                            # Enforce the limit even for a wrong header.
                            if (
                                max_asset_bytes is not None
                                and max_asset_bytes > 0
                                and bytes_read > max_asset_bytes
                            ):
                                exceeded_limit = True
                                break

                            file.write(chunk)

                if exceeded_limit:
                    raise AssetSizeExceeded(
                        (
                            f"Exceeded maximum download size "
                            f"of {max_asset_bytes} "
                            f"with Content-Length: {content_length} "
                            f"and {bytes_read} bytes downloaded"
                        ),
                    )
            temp_path.replace(target_path)
        except AssetSizeExceeded:
            raise
        except Exception as error:
            if (
                isinstance(error, aiohttp.ClientResponseError)
                and error.status == 429
            ):
                count("http_429")

            logger.warning(
                "Asset not downloaded: %s (%s: %s)",
                discovery.url, type(error).__name__, error,
            )

            target_path = None
        finally:
            # Each request owns its temporary file, including on cancellation.
            temp_path.unlink(missing_ok=True)

    return RawAsset(
        domain=discovery.domain,
        post_id=discovery.post_id,
        url=discovery.url,
        asset_type=discovery.asset_type,
        scraped_at=timestamp,
        source=discovery.source,
        local_path=str(target_path) if target_path else "",
    )


def _referrer_headers(discovery: AssetDiscovery) -> dict[str, str]:
    """Send an origin without article paths, queries or credentials."""
    try:
        source = urlsplit(discovery.referrer_url or "")
        target = urlsplit(discovery.url)

        if (
            source.scheme not in {"http", "https"} or not source.hostname
            or source.username or source.password
            or (source.scheme == "https" and target.scheme == "http")
        ):
            return {}

        _ = source.port
    except ValueError:
        return {}

    return {"Referer": f"{source.scheme}://{source.netloc}/"}
