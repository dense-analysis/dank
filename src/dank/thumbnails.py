from __future__ import annotations

import pathlib
from typing import NamedTuple

from PIL import Image, ImageStat

# A conservative display heuristic, never a reason to discard source bytes.
VISUAL_HASH_DISTANCE = 2
VISUAL_COLOR_DISTANCE = 3
MAX_THUMBNAIL_PIXELS = 16_000_000
# Ignore near-black letterboxing without trimming most of a dark composition.
LETTERBOX_BLACK_LEVEL = 12
MIN_CROPPED_FRACTION = 0.5
ASPECT_RATIO_DISTANCE = 0.04
DHASH_SIZE = 16


class ThumbnailSignature(NamedTuple):
    visual_hash: str
    visual_color: str
    width: int
    height: int


def thumbnail_signature(path: pathlib.Path) -> ThumbnailSignature:
    with Image.open(path) as opened:
        if opened.width * opened.height > MAX_THUMBNAIL_PIXELS:
            raise ValueError("Thumbnail exceeds image dimension limit")

        image = opened.convert("RGB")

    # YouTube's hqdefault rendition can add black bars around 16:9 artwork.
    bounds = image.convert("L").point([
        0 if value <= LETTERBOX_BLACK_LEVEL else 255 for value in range(256)
    ])
    box = bounds.getbbox()

    if box and (
        box[2] - box[0] >= image.width * MIN_CROPPED_FRACTION
        and box[3] - box[1] >= image.height * MIN_CROPPED_FRACTION
    ):
        image = image.crop(box)

    # Adjacent luminance comparisons follow the standard difference hash:
    # https://github.com/JohannesBuchner/imagehash/blob/master/imagehash/__init__.py
    small = image.convert("L").resize(
        (DHASH_SIZE + 1, DHASH_SIZE), Image.Resampling.LANCZOS,
    )
    pixels = small.tobytes()
    fingerprint = 0

    for row in range(DHASH_SIZE):
        for column in range(DHASH_SIZE):
            offset = row * (DHASH_SIZE + 1) + column
            fingerprint = (fingerprint << 1) | (
                pixels[offset] > pixels[offset + 1]
            )

    color = ",".join(str(round(value)) for value in ImageStat.Stat(image).mean)

    return ThumbnailSignature(
        f"{fingerprint:064x}", color, image.width, image.height,
    )


def similar_thumbnail_signatures(
    left: ThumbnailSignature, right: ThumbnailSignature,
) -> bool:
    if not all((left.visual_hash, right.visual_hash, left.height,
                right.height, left.width, right.width)):
        return False

    # Do not collapse distinct crops or image formats with similar flat areas.
    if abs(left.width / left.height - right.width / right.height) > (
        ASPECT_RATIO_DISTANCE
    ):
        return False

    try:
        distance = (int(left.visual_hash, 16)
                    ^ int(right.visual_hash, 16)).bit_count()
        colors = list(zip(
            map(int, left.visual_color.split(",")),
            map(int, right.visual_color.split(",")), strict=True,
        ))
    except ValueError:
        return False

    return (
        distance <= VISUAL_HASH_DISTANCE and len(colors) == 3
        and all(abs(a - b) <= VISUAL_COLOR_DISTANCE for a, b in colors)
    )


def thumbnail_suffix(path: pathlib.Path) -> str:
    # YouTube sometimes returns AVIF bytes even for a URL ending in .jpg.
    with Image.open(path) as image:
        suffix = {"JPEG": ".jpg", "PNG": ".png", "WEBP": ".webp",
                  "AVIF": ".avif", "GIF": ".gif"}.get(image.format or "")

    if suffix is None:
        raise ValueError("Unsupported thumbnail image format")

    return suffix
