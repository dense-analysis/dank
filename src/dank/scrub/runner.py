"""Preview and apply retention to raw captures and downloaded media."""

from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
from itertools import batched

from dank.collection_lock import collection_lock
from dank.config import Settings
from dank.model import parse_datetime
from dank.scrub.plan import AssetRetention, ScrubResult, StoredFile, plan_scrub
from dank.storage.clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)


async def run_scrub(
    settings: Settings, *, age: datetime.timedelta,
    dry_run: bool = False, now: datetime.datetime | None = None,
) -> ScrubResult:
    if age <= datetime.timedelta():
        raise ValueError("Scrub age must be greater than zero")

    now = now or datetime.datetime.now(datetime.UTC)

    if now.tzinfo is None:
        raise ValueError("Scrub time must include a timezone")

    now = now.astimezone(datetime.UTC)
    cutoff = now - age
    cutoff = cutoff.replace(microsecond=cutoff.microsecond // 1000 * 1000)
    directory = settings.data_dir.expanduser().resolve()
    assets_dir = directory / "assets"

    with collection_lock(
        directory,
        busy_message="Another collection or scrub is already running",
    ):
        if assets_dir.is_symlink():
            raise OSError(f"Asset directory is a symlink: {assets_dir}")

        # Large raw-table mutations may take longer than ordinary queries.
        async with ClickHouseClient(
            settings.clickhouse, timeout_seconds=300,
        ) as client:
            return await _scrub_content(
                client, assets_dir,
                cutoff=cutoff, now=now, dry_run=dry_run,
            )


async def _scrub_content(
    client: ClickHouseClient, assets_dir: pathlib.Path, *,
    cutoff: datetime.datetime, now: datetime.datetime, dry_run: bool,
) -> ScrubResult:
    tables = await client.fetch_json("SHOW TABLES")
    available = {str(row["name"]) for row in tables.rows}
    counts: dict[str, int] = {}
    # The driver drops subsecond precision when binding a plain datetime.
    cutoff_params = {
        "cutoff": cutoff.replace(tzinfo=None).isoformat(
            sep=" ", timespec="milliseconds",
        ),
    }
    expired = "scraped_at < toDateTime64(%(cutoff)s, 3, 'UTC')"

    for table in ("raw_posts", "raw_assets"):
        counts[table] = 0

        if table in available:
            found = await client.fetch_json(
                f"SELECT count() AS count FROM {table} WHERE {expired}",
                cutoff_params,
            )
            counts[table] = int(found.rows[0]["count"])

    records, stored = await _asset_retention_records(
        client, available, assets_dir,
    )
    files = await asyncio.to_thread(_stored_asset_files, assets_dir)
    protected = await _protected_asset_paths(client, available, assets_dir)
    plan = plan_scrub(files, records, protected, cutoff, now)
    result = ScrubResult(
        counts["raw_posts"], counts["raw_assets"], len(plan.files),
        plan.reclaimable_bytes, dry_run,
    )
    logger.info(
        "Scrub %s before %s: raw posts=%d; raw assets=%d; files=%d; "
        "up to %.3f GB reclaimable from files",
        "dry run" if dry_run else "plan", cutoff.isoformat(),
        result.raw_posts, result.raw_assets, result.files,
        result.reclaimable_bytes / 1_000_000_000,
    )

    if dry_run:
        return result

    # Persist first-seen times and retirement decisions before removing files
    # or raw records. Retrying an interrupted scrub keeps those decisions.
    schema = pathlib.Path(__file__).parents[1] / "storage/retention.sql"
    await client.execute(schema.read_text())

    for batch in batched(
        (record for record in plan.records if record not in stored), 1000,
    ):
        await client.insert_rows("asset_retention", [
            {**record._asdict(), "local_path": str(record.local_path),
             "updated_at": now} for record in batch
        ])

    _remove_scrubbed_files(plan.files, assets_dir)

    # Keep this whitelist literal: no processed table may be a scrub target.
    for table in ("raw_posts", "raw_assets"):
        if counts[table]:
            await client.execute(
                f"ALTER TABLE {table} DELETE WHERE {expired} "
                "SETTINGS mutations_sync = 1", cutoff_params,
            )

    logger.info(
        "Scrub complete: %d raw posts, %d raw assets and %d files removed; "
        "processed posts and other saved tables retained",
        result.raw_posts, result.raw_assets, result.files,
    )

    return result


async def _asset_retention_records(
    client: ClickHouseClient, tables: set[str], assets_dir: pathlib.Path,
) -> tuple[tuple[AssetRetention, ...], set[AssetRetention]]:
    records: dict[tuple[str, str, str, pathlib.Path], AssetRetention] = {}
    stored: set[AssetRetention] = set()

    for table, timestamp in (
        ("asset_retention", "first_scraped_at"),
        ("raw_assets", "scraped_at"),
        ("assets", "created_at"),
    ):
        if table not in tables:
            continue

        if table == "asset_retention":
            result = await client.fetch_json(
                "SELECT domain, post_id, url, local_path, first_scraped_at, "
                "scrubbed_at FROM asset_retention FINAL",
            )
        else:
            result = await client.fetch_json(
                "SELECT domain, post_id, url, local_path, "
                f"min({timestamp}) AS first_scraped_at FROM {table} "
                "WHERE local_path != '' GROUP BY domain, post_id, url, "
                "local_path",
            )

        for row in result.rows:
            path = _retained_asset_path(assets_dir, str(row["local_path"]))
            first_seen = parse_datetime(row["first_scraped_at"])

            if path is None or first_seen is None:
                continue

            key = (str(row["domain"]), str(row["post_id"]),
                   str(row["url"]), path)
            previous = records.get(key)
            scrubbed_at = parse_datetime(row.get("scrubbed_at"))

            if previous:
                first_seen = min(first_seen, previous.first_scraped_at)
                scrubbed_at = previous.scrubbed_at or scrubbed_at

            record = AssetRetention(*key, first_seen, scrubbed_at)
            records[key] = record

            if table == "asset_retention":
                stored.add(record)

    return tuple(records.values()), stored


def _retained_asset_path(
    assets_dir: pathlib.Path, value: str,
) -> pathlib.Path | None:
    path = pathlib.Path(value).expanduser().absolute()

    if assets_dir.is_symlink() or path.is_symlink() or any(
        parent.is_symlink() for parent in path.parents
        if parent.is_relative_to(assets_dir) and parent != assets_dir
    ):
        return None

    resolved = path.resolve()

    if resolved != assets_dir and resolved.is_relative_to(assets_dir):
        return resolved

    return None


def _stored_asset_files(assets_dir: pathlib.Path) -> tuple[StoredFile, ...]:
    files: list[StoredFile] = []

    for path in assets_dir.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue

        try:
            stat = path.stat()
        except FileNotFoundError:
            continue

        files.append(StoredFile(
            path, datetime.datetime.fromtimestamp(stat.st_mtime, datetime.UTC),
            stat.st_size, stat.st_blocks * 512, stat.st_dev, stat.st_ino,
            stat.st_nlink, stat.st_mtime_ns,
        ))

    return tuple(files)


async def _protected_asset_paths(
    client: ClickHouseClient, tables: set[str], assets_dir: pathlib.Path,
) -> frozenset[pathlib.Path]:
    protected = set((assets_dir / "source-icons").rglob("*"))

    for table in (
        "reddit_community_icons", "youtube_channels",
        "youtube_thumbnail_observations",
    ):
        if table not in tables:
            continue

        result = await client.fetch_json(
            f"SELECT DISTINCT local_path FROM {table} WHERE local_path != ''",
        )

        for row in result.rows:
            path = _retained_asset_path(assets_dir, str(row["local_path"]))

            if path is not None:
                protected.add(path)

    return frozenset(protected)


def _remove_scrubbed_files(
    files: tuple[StoredFile, ...], assets_dir: pathlib.Path,
) -> None:
    # Unlinks stay on this task so cancellation cannot release the collection
    # lock while an uncancellable background deletion is still running.
    for file in files:
        if _retained_asset_path(assets_dir, str(file.path)) != file.path:
            raise OSError(f"Asset path changed during scrub: {file.path}")

        try:
            stat = file.path.stat()
        except FileNotFoundError:
            continue

        if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns) != (
            file.device, file.inode, file.size_bytes,
            file.modified_ns,
        ):
            raise OSError(f"Asset changed during scrub: {file.path}")

        file.path.unlink()

        for parent in file.path.parents:
            if parent == assets_dir:
                break

            try:
                parent.rmdir()
            except OSError:
                break
