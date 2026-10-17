from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import pathlib
import re
import uuid
from urllib.parse import urlparse, urlsplit

import aiohttp

from dank.media_urls import canonical_media_url
from dank.model import AssetDiscovery, RawAsset, YouTubeThumbnailObservation
from dank.scrape.http import http_response
from dank.scrape.metrics import count
from dank.thumbnails import thumbnail_signature, thumbnail_suffix

from .limits import AssetSizeExceeded

logger = logging.getLogger(__name__)


def http_asset_path(target_dir: pathlib.Path, url: str) -> pathlib.Path:
    # The query string can identify an entirely different image on a CDN.
    digest = hashlib.sha256(url.encode()).hexdigest()
    suffix = pathlib.PurePosixPath(urlparse(url).path).suffix

    if len(suffix) > 16:
        suffix = ""

    return target_dir / f"{digest}{suffix}"


def youtube_thumbnail_urls(discovery: AssetDiscovery) -> tuple[str, ...]:
    """Recover only a validated thumbnail belonging to this exact video."""
    match = re.fullmatch(r"video:([A-Za-z0-9_-]{11})", discovery.post_id)

    if (discovery.source != "youtube" or discovery.domain != "youtube.com"
            or discovery.asset_type != "image" or match is None):
        return ()

    try:
        parsed = urlsplit(discovery.url)

        if (
            parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.port not in {None, 443}
            or not re.fullmatch(r"(?:i\d*\.ytimg\.com|img\.youtube\.com)",
                                parsed.hostname or "")
            or not re.fullmatch(
                rf"/vi(?:_webp)?/{re.escape(match[1])}/[^/]+", parsed.path,
            )
        ):
            return ()
    except ValueError:
        return ()

    # These are alternate encodings of the chosen thumbnail, not extra frames.
    return tuple(dict.fromkeys((discovery.url, *(
        f"https://i.ytimg.com/vi/{match[1]}/{quality}.jpg"
        for quality in ("maxresdefault", "sddefault", "hqdefault", "mqdefault")
    ))))


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
    candidates = youtube_thumbnail_urls(discovery)
    observation = None

    if candidates or not target_path.exists():
        try:
            target_path, observation = await _download_candidates(
                discovery, candidates or (discovery.url,), target_path,
                http_client, max_asset_bytes, timestamp,
                snapshot=bool(candidates),
            )
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

    return RawAsset(
        domain=discovery.domain,
        post_id=discovery.post_id,
        url=discovery.url,
        asset_type=discovery.asset_type,
        scraped_at=timestamp,
        source=discovery.source,
        local_path=str(target_path) if target_path else "",
        thumbnail=observation,
    )


async def _download_candidates(
    discovery: AssetDiscovery,
    urls: tuple[str, ...],
    target_path: pathlib.Path,
    client: aiohttp.ClientSession,
    max_asset_bytes: int | None,
    timestamp: datetime.datetime,
    *, snapshot: bool,
) -> tuple[pathlib.Path, YouTubeThumbnailObservation | None]:
    for index, url in enumerate(urls):
        temp_path = target_path.with_name(
            f"{target_path.name}.{uuid.uuid4().hex}.part",
        )

        try:
            async with http_response(
                client, canonical_media_url(url),
                headers=_referrer_headers(discovery),
                preserve_url=urlparse(url).hostname in {
                    "preview.redd.it", "external-preview.redd.it",
                },
            ) as response:
                response.raise_for_status()
                await _stream_response(response, temp_path, max_asset_bytes)

            observation = None

            if snapshot:
                observation = await asyncio.to_thread(
                    thumbnail_observation, temp_path, discovery.post_id,
                    timestamp, discovery.url, url,
                )
                if capture := discovery.youtube_capture:
                    observation = observation._replace(
                        title=capture.title,
                        title_observed_at=capture.observed_at,
                    )

                suffix = await asyncio.to_thread(thumbnail_suffix, temp_path)
                target_path = target_path.parent / "thumbnails" / (
                    f"{observation.content_hash}{suffix}"
                )
                target_path.parent.mkdir(parents=True, exist_ok=True)
                observation = observation._replace(local_path=str(target_path))

            # Content addressed snapshots keep all previous bytes intact.
            if not snapshot or not target_path.exists():
                temp_path.replace(target_path)

            if url != discovery.url:
                logger.info("Recovered YouTube thumbnail: %s -> %s",
                            discovery.url, url)

            return target_path, observation
        except aiohttp.ClientResponseError as error:
            if error.status != 404 or index == len(urls) - 1:
                raise
        finally:
            temp_path.unlink(missing_ok=True)

    raise RuntimeError("No thumbnail download candidates")


async def _stream_response(
    response: aiohttp.ClientResponse,
    temp_path: pathlib.Path,
    max_asset_bytes: int | None,
) -> None:
    content_length = response.content_length
    limit = (
        max_asset_bytes if max_asset_bytes and max_asset_bytes > 0 else None
    )

    if (limit is not None and content_length is not None
            and content_length > limit):
        raise AssetSizeExceeded(f"Content-Length exceeds {limit}-byte limit")

    bytes_read = 0

    with temp_path.open("wb") as file:
        async for chunk in response.content.iter_chunked(65536):
            if not chunk:
                continue

            bytes_read += len(chunk)

            if limit is not None and bytes_read > limit:
                raise AssetSizeExceeded(f"Download exceeds {limit}-byte limit")

            file.write(chunk)


def thumbnail_observation(
    path: pathlib.Path, post_id: str, timestamp: datetime.datetime,
    captured_url: str, fetched_url: str,
) -> YouTubeThumbnailObservation:
    signature = thumbnail_signature(path)

    with path.open("rb") as file:
        digest = hashlib.file_digest(file, "sha256").hexdigest()

    return YouTubeThumbnailObservation(
        post_id, timestamp, captured_url, fetched_url, str(path), digest,
        *signature,
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
