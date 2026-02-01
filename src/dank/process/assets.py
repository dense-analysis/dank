from __future__ import annotations

import mimetypes
import pathlib

from dank.model import Asset, RawAsset


def convert_raw_asset(raw: RawAsset) -> Asset | None:
    if not raw.local_path:
        return None

    path = pathlib.Path(raw.local_path)
    if not path.is_file():
        return None

    size_bytes = path.stat().st_size
    content_type = mimetypes.guess_type(path.name)[0] or ""

    return Asset(
        domain=raw.domain,
        post_id=raw.post_id,
        url=raw.url,
        local_path=raw.local_path,
        content_type=content_type,
        size_bytes=size_bytes,
        created_at=raw.scraped_at,
        updated_at=raw.scraped_at,
        source=raw.source,
    )
