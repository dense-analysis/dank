from __future__ import annotations

import asyncio
import datetime
import functools
import hashlib
import logging
import optparse
import pathlib
import shutil
import tempfile
from typing import TYPE_CHECKING, NamedTuple, Self, cast
from urllib.parse import urlsplit

from yt_dlp import ParsedOptions, YoutubeDL, parse_options  # type: ignore

from dank.model import AssetDiscovery, RawAsset

from .limits import AssetSizeExceeded, VideoSizeLimit
from .video_formats import youtube_format

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from _typeshed import Unused
    def parse_options(argv: list[str]) -> ParsedOptions: ...

    class ParsedOptions(NamedTuple):
        parser: object
        options: optparse.Values
        urls: list[str]
        ydl_opts: dict[str, object]

    class YoutubeDL:
        def __init__(
            self,
            params: dict[str, object] | None=None,
            *,
            auto_init: bool=True,
        ): ...
        def __enter__(self) -> Self: ...
        def __exit__(self, *args: Unused) -> None: ...
        def extract_info(
            self,
            url: str,
            *,
            download: bool=True,
            ie_key: str | None=None,
            extra_info: dict[str, object] | None=None,
            process: bool=True,
            force_generic_extractor: bool=False,
        ) -> dict[str, object] | None: ...


logger = logging.getLogger(__name__)

AUDIO_VIDEO_FORMAT_SELECTOR = "bestvideo+bestaudio/best"
COOKIES_BROWSER_NAME = "chromium"
NON_MEDIA_SUFFIXES = {
    ".avif",
    ".bmp",
    ".description",
    ".gif",
    ".jpeg",
    ".jpg",
    ".json",
    ".png",
    ".txt",
    ".webp",
    ".ytdl",
}


class _YtDlpLogger:
    """A yt-dlp logger for redirecting logs to our logger."""
    def __init__(self) -> None:
        self.size_excluded = False

    def debug(self, message: str) -> None:
        # The pinned yt-dlp HTTP downloader reports header size skips here.
        if message.lstrip().startswith(
            "[download] File is larger than max-filesize (",
        ):
            self.size_excluded = True

        logger.debug("[yt-dlp] %s", message)

    def warning(self, message: str) -> None:
        logger.warning("[yt-dlp] %s", message)

    def error(self, message: str) -> None:
        logger.error("[yt-dlp] %s", message)


async def download_audio_video_asset(
    *,
    discovery: AssetDiscovery,
    target_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path | None,
    max_asset_bytes: int | None,
    timestamp: datetime.datetime,
    download_thumbnail: bool = True,
) -> RawAsset:
    local_path = await asyncio.to_thread(
        _download_audio_video_asset_sync,
        discovery.url,
        target_dir=target_dir,
        browser_profile_dir=browser_profile_dir,
        max_asset_bytes=max_asset_bytes,
        download_thumbnail=download_thumbnail,
    )

    return RawAsset(
        domain=discovery.domain,
        post_id=discovery.post_id,
        url=discovery.url,
        asset_type=discovery.asset_type,
        scraped_at=timestamp,
        source=discovery.source,
        local_path=local_path or "",
    )


def _download_audio_video_asset_sync(
    url: str,
    *,
    target_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path | None,
    max_asset_bytes: int | None,
    download_thumbnail: bool = True,
) -> str | None:
    directory = target_dir / hashlib.sha256(url.encode()).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "media.mp4"

    if destination.is_file():
        _check_file_size(destination, max_asset_bytes)

        return str(destination.resolve())

    # A job owns its staging directory, so failures remove only its partials.
    with tempfile.TemporaryDirectory(
        prefix=".download-", dir=directory,
    ) as work:
        staging = pathlib.Path(work)
        arguments = _build_yt_dlp_arguments(
            url, target_dir=staging, max_asset_bytes=max_asset_bytes,
            cookies_from_browser=_cookies_from_browser_argument(
                browser_profile_dir,
            ),
            download_thumbnail=download_thumbnail,
        )
        info = _download_with_yt_dlp(arguments)

        if info is None:
            return None

        local_path = _extract_download_path(info, staging)

        if local_path is None or local_path.suffix.lower() != ".mp4":
            logger.warning("No completed MP4 media file found for %s", url)

            return None

        _check_file_size(local_path, max_asset_bytes)
        local_path.replace(destination)

        for sidecar in staging.iterdir():
            if sidecar.is_file() and _is_sidecar_file(sidecar):
                if not sidecar.name.endswith((".part", ".ytdl")):
                    sidecar.replace(directory / sidecar.name)

    return str(destination.resolve())


def _check_file_size(path: pathlib.Path, maximum: int | None) -> None:
    if maximum and maximum > 0 and path.stat().st_size > maximum:
        raise AssetSizeExceeded(
            f"Downloaded media exceeds {maximum}-byte limit",
        )


def _build_yt_dlp_arguments(
    url: str,
    *,
    target_dir: pathlib.Path,
    max_asset_bytes: int | None,
    cookies_from_browser: str | None,
    download_thumbnail: bool = True,
) -> list[str]:
    output_template = target_dir / "media.%(ext)s"
    arguments = [
        "--no-playlist",
        "--no-progress",
        "--no-overwrites",
        "--restrict-filenames",
        "--add-metadata",
        *(["--write-thumbnail"] if download_thumbnail else []),
        "--write-info-json",
        "--format",
        AUDIO_VIDEO_FORMAT_SELECTOR,
        "--merge-output-format", "mp4",
        "--remux-video", "mp4",
        "--socket-timeout", "15",
        "--retries", "1",
        "--fragment-retries", "1",
        "--extractor-retries", "1",
        "--output",
        str(output_template),
    ]

    if max_asset_bytes is not None and max_asset_bytes > 0:
        arguments.extend(["--max-filesize", str(max_asset_bytes)])

    js_runtimes = _build_js_runtime_argument()

    if js_runtimes is not None:
        arguments.extend(["--js-runtimes", js_runtimes])

    if cookies_from_browser is not None:
        arguments.extend(["--cookies-from-browser", cookies_from_browser])

    arguments.append(url)

    return arguments


def _build_js_runtime_argument() -> str | None:
    runtimes: list[str] = []

    for runtime_name in ("deno", "node"):
        runtime_path = shutil.which(runtime_name)

        if runtime_path:
            runtimes.append(f"{runtime_name}:{runtime_path}")

    if not runtimes:
        return None

    return ",".join(runtimes)


def _cookies_from_browser_argument(
    browser_profile_dir: pathlib.Path | None,
) -> str | None:
    if browser_profile_dir is None:
        return None

    profile_dir = browser_profile_dir.expanduser()

    if not profile_dir.is_dir():
        return None

    resolved_profile = profile_dir.resolve()

    return f"{COOKIES_BROWSER_NAME}:{resolved_profile}"


def _download_with_yt_dlp(arguments: list[str]) -> dict[str, object] | None:
    parsed_options = parse_options(arguments)

    if not parsed_options.urls:
        # This should never happen.
        raise RuntimeError("No URLs for yt-dlp options!")

    download_logger = _YtDlpLogger()
    parsed_options.ydl_opts["logger"] = download_logger
    # CLI defaults swallow custom exceptions; the API must retain size skips.
    parsed_options.ydl_opts["ignoreerrors"] = False
    maximum = parsed_options.ydl_opts.get("max_filesize")
    maximum = maximum if isinstance(maximum, int) and maximum > 0 else None
    parsed_options.ydl_opts["progress_hooks"] = [VideoSizeLimit(maximum)]

    if urlsplit(parsed_options.urls[0]).hostname != "v.redd.it":
        parsed_options.ydl_opts["format"] = functools.partial(
            youtube_format, maximum=maximum,
        )

    try:
        with YoutubeDL(parsed_options.ydl_opts) as downloader:
            info = downloader.extract_info(
                parsed_options.urls[0], download=True,
            )

        if download_logger.size_excluded:
            raise AssetSizeExceeded(
                f"yt-dlp excluded media above the {maximum}-byte limit",
            )

        return info
    except AssetSizeExceeded:
        raise
    except Exception as error:
        logger.warning(
            "Media not downloaded: %s (%s: %s)",
            parsed_options.urls[0], type(error).__name__, error,
        )

        return None


def _extract_download_path(
    info: dict[str, object],
    target_dir: pathlib.Path,
) -> pathlib.Path | None:
    target_root = target_dir.expanduser().resolve()

    for candidate in _download_path_candidates(info):
        resolved = candidate.expanduser().resolve()

        if not resolved.is_file() or resolved.name != "media.mp4":
            continue

        if _is_sidecar_file(resolved):
            continue

        try:
            resolved.relative_to(target_root)
        except ValueError:
            continue

        return resolved

    return None


def _download_path_candidates(info: dict[str, object]) -> list[pathlib.Path]:
    candidates: list[pathlib.Path] = []

    for key in ("filepath", "_filename", "filename"):
        match info.get(key):
            case str() as value if value:
                candidates.append(pathlib.Path(value))
            case _:
                pass

    requested_downloads = info.get("requested_downloads")

    if isinstance(requested_downloads, list):
        for entry in cast(list[object], requested_downloads):
            match entry:
                case {"filepath": str() as filepath} if filepath:
                    candidates.append(pathlib.Path(filepath))
                case _:
                    pass

    return candidates


def _is_sidecar_file(path: pathlib.Path) -> bool:
    name = path.name.lower()

    if name.endswith(".part"):
        return True

    if name.endswith(".info.json"):
        return True

    return path.suffix.lower() in NON_MEDIA_SUFFIXES
