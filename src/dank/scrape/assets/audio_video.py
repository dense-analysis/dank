from __future__ import annotations

import asyncio
import datetime
import logging
import optparse
import pathlib
import shutil
from typing import TYPE_CHECKING, NamedTuple, Self, cast

from yt_dlp import ParsedOptions, YoutubeDL, parse_options  # type: ignore

from dank.model import AssetDiscovery, RawAsset

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

AUDIO_VIDEO_FORMAT_SELECTOR = "best[ext=mp4]/best"
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
    def debug(self, message: str) -> None:
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
) -> RawAsset:
    local_path = await asyncio.to_thread(
        _download_audio_video_asset_sync,
        discovery.url,
        target_dir=target_dir,
        browser_profile_dir=browser_profile_dir,
        max_asset_bytes=max_asset_bytes,
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
) -> str | None:
    # Create the download directory if it doesn't already exist.
    target_dir.mkdir(parents=True, exist_ok=True)

    cookies_from_browser = _cookies_from_browser_argument(
        browser_profile_dir,
    )
    arguments = _build_yt_dlp_arguments(
        url,
        target_dir=target_dir,
        max_asset_bytes=max_asset_bytes,
        cookies_from_browser=cookies_from_browser,
    )
    info = _download_with_yt_dlp(arguments)

    if info is None and cookies_from_browser is not None:
        fallback_arguments = _build_yt_dlp_arguments(
            url,
            target_dir=target_dir,
            max_asset_bytes=max_asset_bytes,
            cookies_from_browser=None,
        )
        info = _download_with_yt_dlp(fallback_arguments)

    if info is None:
        return None

    local_path = _extract_download_path(info, target_dir)

    if local_path is None:
        return None

    if max_asset_bytes is not None and max_asset_bytes > 0:
        if local_path.stat().st_size > max_asset_bytes:
            local_path.unlink(missing_ok=True)

            return None

    return str(local_path)


def _build_yt_dlp_arguments(
    url: str,
    *,
    target_dir: pathlib.Path,
    max_asset_bytes: int | None,
    cookies_from_browser: str | None,
) -> list[str]:
    output_template = target_dir / "%(id)s.%(ext)s"
    arguments = [
        "--no-playlist",
        "--no-progress",
        "--no-overwrites",
        "--restrict-filenames",
        "--add-metadata",
        "--write-thumbnail",
        "--write-info-json",
        "--format",
        AUDIO_VIDEO_FORMAT_SELECTOR,
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

    parsed_options.ydl_opts["logger"] = _YtDlpLogger()

    try:
        with YoutubeDL(parsed_options.ydl_opts) as downloader:
            return downloader.extract_info(
                parsed_options.urls[0],
                download=True,
            )
    except Exception:
        logger.debug(
            "Failed to download %s",
            parsed_options.urls[0],
            exc_info=True,
        )

        return None


def _extract_download_path(
    info: dict[str, object],
    target_dir: pathlib.Path,
) -> pathlib.Path | None:
    target_root = target_dir.expanduser().resolve()

    for candidate in _download_path_candidates(info):
        resolved = candidate.expanduser().resolve()

        if not resolved.is_file():
            continue

        if _is_sidecar_file(resolved):
            continue

        try:
            resolved.relative_to(target_root)
        except ValueError:
            continue

        return resolved

    return _latest_downloaded_media_file(target_root)


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


def _latest_downloaded_media_file(
    target_dir: pathlib.Path,
) -> pathlib.Path | None:
    try:
        files = [
            path.resolve()
            for path in target_dir.iterdir()
            if path.is_file() and not _is_sidecar_file(path)
        ]
    except FileNotFoundError:
        return None

    if not files:
        return None

    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)

    return files[0]


def _is_sidecar_file(path: pathlib.Path) -> bool:
    name = path.name.lower()

    if name.endswith(".part"):
        return True

    if name.endswith(".info.json"):
        return True

    return path.suffix.lower() in NON_MEDIA_SUFFIXES
