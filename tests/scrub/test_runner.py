from __future__ import annotations

import datetime as dt
import os
import pathlib
from copy import deepcopy
from typing import Any, Self, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.collection_lock import CollectionBusyError, collection_lock
from dank.config import Settings, load_settings
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets, scrubbed_assets
from dank.scrape.runner import (
    _load_scrubbed_assets,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrub import (
    __main__ as cli,
    runner,
)
from dank.scrub.runner import (
    _scrub_content,  # pyright: ignore[reportPrivateUsage]
    run_scrub,
)
from dank.storage.clickhouse import ClickHouseClient, QueryResult

NOW = dt.datetime(2026, 11, 21, 12, tzinfo=dt.UTC)
OLD = NOW - dt.timedelta(days=60)
CUTOFF = NOW - dt.timedelta(days=30)


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Settings:
    config = tmp_path / "settings.toml"
    config.touch()

    return load_settings(config)._replace(data_dir=tmp_path)


def asset(
    path: pathlib.Path, when: dt.datetime, **fields: Any,
) -> dict[str, Any]:
    return {
        "domain": "example.test", "post_id": "post", "url": "https://media/a",
        "local_path": str(path), "scraped_at": when, "created_at": when,
        **fields,
    }


def file(path: pathlib.Path, when: dt.datetime = OLD) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"media content")
    os.utime(path, (when.timestamp(), when.timestamp()))

    return path


class Store:
    def __init__(self, **tables: list[dict[str, Any]]) -> None:
        self.tables = tables
        self.writes: list[str] = []
        self.fail_mutation = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        if query == "SHOW TABLES":
            return QueryResult([{"name": table} for table in self.tables])

        table = next(
            name for name in self.tables if f"FROM {name} " in query + " "
        )
        rows = self.tables[table]

        if query.startswith("SELECT count()"):
            assert params is not None
            cutoff = dt.datetime.fromisoformat(params["cutoff"]).replace(
                tzinfo=dt.UTC,
            )

            return QueryResult([{"count": sum(
                row["scraped_at"] < cutoff for row in rows
            )}])

        if "WHERE scrubbed_at IS NOT NULL" in query:
            return QueryResult([row for row in rows if row.get("scrubbed_at")])

        if "min(" in query:
            field = "scraped_at" if table == "raw_assets" else "created_at"
            groups: dict[tuple[str, ...], dict[str, Any]] = {}

            for row in rows:
                if not row["local_path"]:
                    continue

                key = tuple(row[name] for name in (
                    "domain", "post_id", "url", "local_path",
                ))
                previous = groups.get(key)
                groups[key] = {
                    **row, "first_scraped_at": min(
                        row[field], previous["first_scraped_at"]
                        if previous else row[field],
                    ),
                }

            return QueryResult(list(groups.values()))

        return QueryResult(deepcopy(rows))

    async def execute(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> None:
        self.writes.append(query)

        if query.startswith("CREATE TABLE"):
            assert "CREATE TABLE IF NOT EXISTS asset_retention" in query
            self.tables.setdefault("asset_retention", [])

            return

        assert params is not None
        table = query.split()[2]
        assert table in {"raw_posts", "raw_assets"}
        assert "mutations_sync = 1" in query

        if self.fail_mutation:
            raise RuntimeError("mutation failed")

        cutoff = dt.datetime.fromisoformat(params["cutoff"]).replace(
            tzinfo=dt.UTC,
        )
        self.tables[table] = [
            row for row in self.tables[table]
            if row["scraped_at"] >= cutoff
        ]

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        assert table == "asset_retention"
        self.writes.append(table)
        latest = {
            tuple(row[key] for key in (
                "domain", "post_id", "url", "local_path",
            )): row
            for row in [*self.tables.get(table, []), *rows]
        }
        self.tables[table] = list(latest.values())


async def run(store: Store, root: pathlib.Path, *, dry_run: bool = False):
    return await _scrub_content(
        cast(ClickHouseClient, store), root, cutoff=CUTOFF, now=NOW,
        dry_run=dry_run,
    )


async def test_scrub_removes_old_captures_and_preserves_processed_content(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    old = file(root / "example.test/post/old.mp4")
    fresh = file(root / "example.test/post/new.mp4", NOW)
    boundary = file(root / "example.test/post/boundary.jpg", CUTOFF)
    profile = file(tmp_path / "browser-profile/session")
    store = Store(
        raw_posts=[{"scraped_at": when} for when in (OLD, CUTOFF, NOW)],
        raw_assets=[
            asset(path, when, url=path.name) for path, when in (
                (old, OLD), (fresh, NOW), (boundary, CUTOFF),
            )
        ],
        posts=[{"html": "Keep this post", "embedding": [1.0]}],
        assets=[asset(old, OLD)],
        category_training=[{"category": "Keep this training"}],
        youtube_discoveries=[{"title": "Keep this feed history"}],
    )
    preserved = deepcopy({key: rows for key, rows in store.tables.items()
                          if key not in {"raw_posts", "raw_assets"}})
    result = await run(store, root)

    assert (result.raw_posts, result.raw_assets, result.files) == (1, 1, 1)
    assert not old.exists()
    assert fresh.exists() and boundary.exists() and profile.exists()
    assert [row["scraped_at"] for row in store.tables["raw_posts"]] == [
        CUTOFF, NOW,
    ]

    for key, rows in preserved.items():
        assert store.tables[key] == rows


async def test_dry_run_does_not_create_tables_update_rows_or_delete_files(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    old = file(root / "site/post/video.mp4")
    store = Store(
        raw_posts=[{"scraped_at": OLD}], raw_assets=[asset(old, OLD)],
    )
    before = deepcopy(store.tables)
    result = await run(store, root, dry_run=True)

    assert result.dry_run and result.files == 1
    assert old.exists() and store.tables == before and not store.writes


async def test_first_capture_survives_repeated_pinned_observations_and_scrubs(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    pinned = file(root / "site/pinned/video.mp4", NOW)
    store = Store(
        raw_posts=[], raw_assets=[asset(pinned, OLD), asset(pinned, NOW)],
    )
    first = await run(store, root)
    again = await run(store, root)
    record = store.tables["asset_retention"][0]

    assert first.files == 1 and again.files == 0
    assert store.writes.count("asset_retention") == 1
    assert record["first_scraped_at"] == OLD and record["scrubbed_at"] == NOW
    assert len(store.tables["raw_assets"]) == 1
    blocked = await _load_scrubbed_assets(cast(ClickHouseClient, store))
    token = scrubbed_assets.set(blocked)

    try:
        downloads = await download_assets(
            [AssetDiscovery("rss", "example.test", "post", "https://media/a",
                            "video", None)],
            assets_dir=root, http_client=cast(Any, None),
        )
    finally:
        scrubbed_assets.reset(token)

    assert len(downloads) == 1 and downloads[0].local_path == ""
    assert not pinned.exists()
    assert ("example.test", "post", "https://media/new") not in blocked


async def test_recent_download_keeps_old_source_mtime_and_sidecars(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    video = file(root / "site/post/url-hash/media.mp4")
    sidecar = file(video.with_suffix(".info.json"))
    store = Store(raw_posts=[], raw_assets=[asset(video, NOW)])

    assert (await run(store, root)).files == 0
    assert video.exists() and sidecar.exists()


async def test_old_video_bundle_and_orphans_are_removed(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    video = file(root / "site/post/url-hash/media.mp4")
    sidecar = file(video.with_suffix(".info.json"), NOW)
    orphan = file(root / "site/forgotten/asset.jpg")
    store = Store(raw_posts=[], raw_assets=[asset(video, OLD)])

    assert (await run(store, root)).files == 3
    assert not any(path.exists() for path in (video, sidecar, orphan))


async def test_icons_and_saved_thumbnail_history_are_preserved(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    icons = [file(root / name / "icon.png") for name in (
        "source-icons", "reddit.com/community-icons", "youtube.com/channel",
        "youtube.com/video/thumbnails",
    )]
    store = Store(
        raw_posts=[], raw_assets=[asset(path, OLD) for path in icons],
        reddit_community_icons=[{"local_path": str(icons[1])}],
        youtube_channels=[{"local_path": str(icons[2])}],
        youtube_thumbnail_observations=[{"local_path": str(icons[3])}],
    )

    assert (await run(store, root)).files == 0
    assert all(path.exists() for path in icons)
    assert all(
        row["scrubbed_at"] is None for row in store.tables["asset_retention"]
    )


async def test_paths_outside_assets_and_symlinks_are_never_followed(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    root.mkdir()
    outside = file(tmp_path / "private/keep")
    (root / "link").symlink_to(outside.parent, target_is_directory=True)
    (root / "file-link").symlink_to(outside)
    store = Store(raw_posts=[], raw_assets=[
        asset(outside, OLD), asset(root / "link/keep", OLD),
        asset(root / "file-link", OLD),
    ])

    assert (await run(store, root)).files == 0
    assert outside.exists() and (root / "link").is_symlink()
    assert not store.tables["asset_retention"]


async def test_scrub_refuses_a_symlinked_asset_directory(
    settings: Settings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    outside = file(settings.data_dir / "private/keep")
    (settings.data_dir / "assets").symlink_to(
        outside.parent, target_is_directory=True,
    )

    def connect(*args: object, **kwargs: object) -> None:
        pytest.fail("A scrub with an invalid path must not connect")

    monkeypatch.setattr(runner, "ClickHouseClient", connect)

    with pytest.raises(OSError, match="Asset directory is a symlink"):
        await run_scrub(settings, age=dt.timedelta(days=30), now=NOW)

    assert outside.exists()


async def test_hardlinks_only_reclaim_space_when_the_last_link_is_removed(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    old = file(root / "site/post/old")
    kept = old.with_name("kept")
    kept.hardlink_to(old)
    store = Store(raw_posts=[], raw_assets=[asset(kept, NOW)])
    result = await run(store, root)

    assert result.files == 1 and result.reclaimable_bytes == 0
    assert not old.exists() and kept.read_bytes() == b"media content"


async def test_failed_mutation_can_be_retried_without_redownloading_media(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "assets"
    old = file(root / "site/post/video.mp4")
    store = Store(
        raw_posts=[{"scraped_at": OLD}], raw_assets=[asset(old, OLD)],
    )
    store.fail_mutation = True

    with pytest.raises(RuntimeError, match="mutation failed"):
        await run(store, root)

    assert not old.exists() and store.tables["raw_posts"]
    assert store.tables["asset_retention"][0]["scrubbed_at"] == NOW
    store.fail_mutation = False
    assert (await run(store, root)).files == 0
    assert not store.tables["raw_posts"] and not store.tables["raw_assets"]


@pytest.mark.parametrize("shared", [False, True])
async def test_scrub_refuses_an_active_collection(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, *, shared: bool,
) -> None:
    def connect(*args: object, **kwargs: object) -> None:
        pytest.fail("A busy scrub must not connect to the database")

    monkeypatch.setattr(runner, "ClickHouseClient", connect)

    with collection_lock(
        settings.data_dir, shared=shared, busy_message="Collection is busy",
    ):
        with pytest.raises(CollectionBusyError):
            await run_scrub(settings, age=dt.timedelta(days=30), now=NOW)


async def test_unlink_failure_keeps_raw_captures_and_retirement_decision(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "assets"
    old = file(root / "site/post/video.mp4")
    store = Store(
        raw_posts=[{"scraped_at": OLD}], raw_assets=[asset(old, OLD)],
    )
    original = pathlib.Path.unlink

    def remove(path: pathlib.Path, *, missing_ok: bool = False) -> None:
        if path == old:
            raise PermissionError("Cannot remove file")

        original(path, missing_ok=missing_ok)

    monkeypatch.setattr(pathlib.Path, "unlink", remove)

    with pytest.raises(PermissionError, match="Cannot remove file"):
        await run(store, root)

    assert old.exists() and store.tables["raw_posts"]
    assert store.tables["raw_assets"]
    assert store.tables["asset_retention"][0]["scrubbed_at"] == NOW


async def test_a_changed_media_url_can_be_downloaded_after_retirement(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.model import RawAsset
    from dank.scrape import assets

    calls: list[str] = []

    async def download(
        discovery: AssetDiscovery, *args: Any, **kwargs: Any,
    ) -> RawAsset:
        calls.append(discovery.url)

        return RawAsset(
            discovery.domain, discovery.post_id, discovery.url,
            discovery.asset_type, NOW, discovery.source, "saved.mp4",
        )

    monkeypatch.setattr(assets, "_download_one", download)
    token = scrubbed_assets.set(frozenset({
        ("example.test", "post", "https://media/old"),
    }))

    try:
        results = await download_assets([
            AssetDiscovery("rss", "example.test", post_id, url, "video", None)
            for post_id, url in (
                ("post", "https://media/old"), ("post", "https://media/new"),
                ("other-post", "https://media/old"),
            )
        ], assets_dir=tmp_path, http_client=cast(Any, None))
    finally:
        scrubbed_assets.reset(token)

    assert calls == ["https://media/new", "https://media/old"]
    assert [result.local_path for result in results] == [
        "", "saved.mp4", "saved.mp4",
    ]


@pytest.mark.parametrize("arguments", [[], ["--age", "all"], ["--age", "0d"]])
def test_cli_requires_an_explicit_positive_age(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str],
) -> None:
    monkeypatch.setattr("sys.argv", ["scrub", *arguments])

    with pytest.raises(SystemExit) as error:
        cli.main()

    assert error.value.code == 2


def test_cli_passes_settings_and_options_to_the_runner(
    settings: Settings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    load = Mock(return_value=settings)
    logging = Mock()
    execute = AsyncMock()
    monkeypatch.setattr("sys.argv", [
        "scrub", "--config", "private.toml", "--age", "30d", "--dry-run",
    ])
    monkeypatch.setattr(cli, "load_settings", load)
    monkeypatch.setattr(cli, "configure_logging", logging)
    monkeypatch.setattr(cli, "run_scrub", execute)
    cli.main()

    load.assert_called_once_with("private.toml")
    logging.assert_called_once_with(settings.logging, component="scrub")
    execute.assert_awaited_once_with(
        settings, age=dt.timedelta(days=30), dry_run=True,
    )
