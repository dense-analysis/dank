"""Plans for retiring downloaded files without changing saved posts."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections import Counter
from typing import NamedTuple


class AssetRetention(NamedTuple):
    domain: str
    post_id: str
    url: str
    local_path: pathlib.Path
    first_scraped_at: dt.datetime
    scrubbed_at: dt.datetime | None = None


class StoredFile(NamedTuple):
    path: pathlib.Path
    modified_at: dt.datetime
    size_bytes: int
    allocated_bytes: int
    device: int
    inode: int
    links: int
    modified_ns: int


class ScrubPlan(NamedTuple):
    files: tuple[StoredFile, ...]
    records: tuple[AssetRetention, ...]
    reclaimable_bytes: int


class ScrubResult(NamedTuple):
    raw_posts: int
    raw_assets: int
    files: int
    reclaimable_bytes: int
    dry_run: bool


def plan_scrub(
    files: tuple[StoredFile, ...], records: tuple[AssetRetention, ...],
    protected: frozenset[pathlib.Path], cutoff: dt.datetime,
    now: dt.datetime,
) -> ScrubPlan:
    first_seen: dict[pathlib.Path, dt.datetime] = {}
    bundles: dict[pathlib.Path, dt.datetime] = {}

    for record in records:
        path = record.local_path
        first_seen[path] = min(
            record.first_scraped_at,
            first_seen.get(path, record.first_scraped_at),
        )

        # yt-dlp publishes the MP4 and its sidecars in one URL-specific folder.
        if path.name == "media.mp4":
            bundles[path.parent] = first_seen[path]

    expired = tuple(
        file for file in files if file.path not in protected
        and first_seen.get(
            file.path, bundles.get(file.path.parent, file.modified_at),
        ) < cutoff
    )
    expired_paths = {file.path for file in expired}
    present = {file.path for file in files}
    retained = tuple(
        record._replace(scrubbed_at=record.scrubbed_at or now)
        if record.local_path not in protected and (
            record.local_path in expired_paths
            or (record.local_path not in present
                and record.first_scraped_at < cutoff)
        ) else record
        for record in records
    )
    links = Counter((file.device, file.inode) for file in expired)
    reclaimed = {
        (file.device, file.inode): file.allocated_bytes
        for file in expired if links[file.device, file.inode] >= file.links
    }

    return ScrubPlan(expired, retained, sum(reclaimed.values()))
