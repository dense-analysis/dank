"""Select complete browser-compatible video within the per-file budget."""
from collections.abc import Iterator
from typing import Any

from .limits import AssetSizeExceeded

# Leave room for the MP4 container; the completed-file check is authoritative.
MUX_SIZE_RESERVE = 0.02


def _groups(formats: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
    audio = [
        item for item in reversed(formats)
        if item.get("vcodec") == "none" and item.get("ext") == "m4a"
        and str(item.get("acodec", "")).startswith("mp4a")
    ]

    for video in reversed(formats):
        if video.get("ext") != "mp4" or not str(
            video.get("vcodec", ""),
        ).startswith(("avc1", "h264")):
            continue

        if str(video.get("acodec", "")).startswith("mp4a"):
            yield [video]
        elif video.get("acodec") == "none":
            for sound in audio:
                yield [video, sound]


def _size(item: dict[str, Any]) -> float | None:
    value = item.get("filesize") or item.get("filesize_approx")

    if isinstance(value, int | float) and value > 0:
        return float(value)

    return None


def youtube_format(
    context: dict[str, Any], *, maximum: int | None,
) -> Iterator[dict[str, Any]]:
    """Select H.264/AAC with room for the final MP4 container."""
    unknown: list[dict[str, Any]] | None = None
    compatible = False
    sized = False
    budget = maximum * (1 - MUX_SIZE_RESERVE) if maximum else None

    for group in _groups(context.get("formats", [])):
        compatible = True
        sizes = [_size(item) for item in group]

        if budget is None or all(size is not None for size in sizes):
            sized = True

            if budget is None or sum(size or 0 for size in sizes) <= budget:
                yield _merge(group)

                return
        elif unknown is None:
            unknown = group

    if unknown is not None and not sized:
        yield _merge(unknown)
    elif compatible:
        raise AssetSizeExceeded(
            f"No complete H.264/AAC format fits the {maximum}-byte limit",
        )
    else:
        raise ValueError("No complete H.264/AAC video format is available")


def _merge(group: list[dict[str, Any]]) -> dict[str, Any]:
    if len(group) == 1:
        return group[0]

    # yt-dlp's documented custom selector contract for merging streams:
    # https://github.com/yt-dlp/yt-dlp#use-a-custom-format-selector
    return {
        "format_id": "+".join(str(item["format_id"]) for item in group),
        "ext": "mp4",
        "requested_formats": group,
        "protocol": "+".join(str(item["protocol"]) for item in group),
    }
