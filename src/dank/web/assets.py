"""Downloaded media: where it lives and how the reader links to it."""

from __future__ import annotations

import pathlib
from urllib.parse import quote

from dank.api.posts.model import AssetRow
from dank.config import Settings


def assets_path(settings: Settings) -> pathlib.Path:
    return (settings.data_dir / "assets").expanduser().resolve()


def asset_view(
    asset: AssetRow,
    *,
    assets_dir: pathlib.Path,
) -> dict[str, str] | None:
    local_url = _asset_local_url(asset, assets_dir)

    if not local_url:
        return None

    kind = "file"

    if _is_image_asset(asset, local_url):
        kind = "image"
    elif _is_audio_asset(asset, local_url):
        kind = "audio"
    elif _is_video_asset(asset, local_url):
        kind = "video"

    return {
        "kind": kind,
        "url": local_url,
        "content_type": asset.content_type,
    }


def _asset_local_url(asset: AssetRow, assets_dir: pathlib.Path) -> str | None:
    if not asset.local_path:
        return None

    path = pathlib.Path(asset.local_path).expanduser()

    try:
        resolved = path.resolve()
    except FileNotFoundError:
        return None

    if not resolved.is_file():
        return None

    try:
        relative = resolved.relative_to(assets_dir)
    except ValueError:
        return None

    return "/assets/" + quote(relative.as_posix(), safe="/")


def _is_image_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("image/"):
        return True

    return _has_extension(url, (".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _is_audio_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("audio/"):
        return True

    return _has_extension(url, (".mp3", ".wav", ".ogg", ".m4a"))


def _is_video_asset(asset: AssetRow, url: str) -> bool:
    content_type = asset.content_type.lower()

    if content_type.startswith("video/"):
        return True

    return _has_extension(url, (".mp4", ".webm"))


def _has_extension(value: str, extensions: tuple[str, ...]) -> bool:
    trimmed = value.split("?", 1)[0].lower()

    return trimmed.endswith(extensions)
