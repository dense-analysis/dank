from __future__ import annotations

import copy
import datetime as dt
import fcntl
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import Settings, load_settings
from dank.runtime import RuntimeInfo
from dank.scrape import runner
from dank.storage.clickhouse import QueryResult

HOME = "https://www.youtube.com/"
START = "2026-10-23T10:00:00+00:00"
OBSERVED = "2026-10-23T10:00:01+00:00"
FINISH = "2026-10-23T10:00:28+00:00"


def report() -> dict[str, Any]:
    entries: list[dict[str, Any]] = []

    for index, (kind, identifier, shelf) in enumerate((
        ("video", "abcdefghijk", ""),
        ("short", "lmnopqrstuv", "Shorts"),
        ("video", "abcdefghijk", "Watch again"),
        ("post", "UgCommunityPost", "Posts"),
    ), 1):
        prefix = "post" if kind == "post" else "video"
        entries.append({
            "identity": prefix + ":" + identifier,
            "kind": kind, "position": index,
            "raw": {
                "schema_version": 1, "kind": kind,
                "video_id": identifier if kind != "post" else "",
                "post_id": identifier if kind == "post" else "",
                "title": "Saved recommendation", "text": "",
                "is_live": False, "detail_capture": {"status": "disabled"},
                "renderer": {"videoRenderer": {"videoId": identifier}},
                "discovery": {
                    "feed_url": HOME, "feed_kind": "home",
                    "feed_name": "Recommendations", "shelf": shelf,
                    "position": index, "rank": 1, "region": "GB",
                    "observed_at": OBSERVED,
                },
            },
        })

    return {
        "schema_version": 1, "feed_url": HOME, "started_at": START,
        "browser": {
            "outcome": "completed", "partial": False,
            "stop_reason": "end of feed", "started_at": START,
            "finished_at": FINISH, "elapsed_seconds": 27.7,
            "entries": entries,
        },
        "yt_dlp": {"outcome": "failed", "entries": [
            {"identity": "video:zzzzzzzzzzz", "kind": "video", "raw": {}},
        ]},
    }


class Client:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.fail_table = ""

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, *args: object) -> None:
        pass

    async def fetch_json(
        self, query: str, params: dict[str, Any],
    ) -> QueryResult:
        assert "scrape_runs FINAL" in query
        existing = [row for row in self.rows.get("scrape_runs", [])
                    if str(row["run_id"]) == params["run_id"]]

        return QueryResult(existing[-1:])

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        if table == self.fail_table:
            raise OSError("fixture write failure")

        self.rows.setdefault(table, []).extend(copy.deepcopy(rows))


def setup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> tuple[Settings, Client, Mock]:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path / "data",
    )
    client = Client()
    database = Mock(return_value=client)
    monkeypatch.setattr(runner, "ClickHouseClient", database)
    monkeypatch.setattr(runner, "runtime_info", Mock(return_value=RuntimeInfo(
        "test", "test", "3.13", 1, None, "fixture-import-code",
    )))
    monkeypatch.setattr(runner, "BrowserSession", Mock(
        side_effect=AssertionError("Import must never open a browser"),
    ))
    monkeypatch.setattr(runner, "download_assets", AsyncMock(
        side_effect=AssertionError("Import must never download assets"),
    ))

    return settings, client, database


async def test_import_preserves_browser_payload_order_dates_and_history(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, client, _ = setup(monkeypatch, tmp_path)
    captured = report()
    result = await runner.store_youtube_comparison(settings, captured)
    assert result["status"] == "completed"
    assert result["posts_saved"] == 4
    assert result["unique_items"] == 3
    assert result["capture_elapsed_ms"] == 27700
    assert result["import_elapsed_ms"] >= 0
    assert not result["reused"]
    raw = client.rows["raw_posts"]
    discoveries = client.rows["youtube_discoveries"]
    assert len(raw) == len(discoveries) == 4
    assert [row["post_id"] for row in raw] == [
        entry["identity"] for entry in captured["browser"]["entries"]
    ]
    assert all(row["domain"] == "youtube.com" for row in raw)
    assert all(row["source"] == "youtube" for row in raw)
    assert all(row["post_created_at"] is None for row in raw)
    assert all(row["request_url"] == HOME for row in raw)
    assert all(row["scraped_at"] == dt.datetime.fromisoformat(OBSERVED)
               for row in raw)
    assert raw[-1]["url"] == HOME + "post/UgCommunityPost"
    assert raw[1]["url"] == HOME + "watch?v=lmnopqrstuv"
    assert [json.loads(row["payload"]) for row in raw] == [
        entry["raw"] for entry in captured["browser"]["entries"]
    ]
    assert [json.loads(row["payload"])["discovery"]["position"]
            for row in discoveries] == [1, 2, 3, 4]
    assert all(row["run_id"] == result["run_id"] for row in discoveries)
    final = client.rows["scrape_runs"][-1]
    source = client.rows["scrape_source_runs"][-1]
    assert final["started_at"] == dt.datetime.fromisoformat(START)
    assert final["finished_at"] == dt.datetime.fromisoformat(FINISH)
    assert final["elapsed_ms"] == source["collection_ms"] == 27700
    assert source["feed_urls"] == [HOME]
    assert source["source"] == "youtube"
    assert source["posts_saved"] == 4
    assert final["media_download_types"] == []
    assert final["files_downloaded"] == final["media_references"] == 0
    assert "raw_assets" not in client.rows
    assert "youtube_transcripts" not in client.rows
    assert not any(row["post_id"] == "video:zzzzzzzzzzz" for row in raw)


async def test_repeat_import_ignores_ytdlp_cleanup_changes_and_writes_no_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, client, _ = setup(monkeypatch, tmp_path)
    captured = report()
    first = await runner.store_youtube_comparison(settings, captured)
    before = copy.deepcopy(client.rows)
    captured["yt_dlp"] = {"outcome": "completed", "entries": []}
    captured["cleanup_seconds"] = 12
    captured["started_at"] = "2026-10-23T09:59:00+00:00"
    second = await runner.store_youtube_comparison(settings, captured)
    assert first["run_id"] == second["run_id"]
    assert second["reused"]
    assert second["posts_saved"] == 4
    assert client.rows == before


@pytest.mark.parametrize("outcome", ["failed", "incomplete", "timeout"])
async def test_partial_browser_rows_stay_partial_even_if_ytdlp_completes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, outcome: str,
) -> None:
    settings, client, _ = setup(monkeypatch, tmp_path)
    captured = report()
    captured["browser"].update(outcome=outcome, partial=True,
                               stop_reason="listing error")
    captured["yt_dlp"]["outcome"] = "completed"
    result = await runner.store_youtube_comparison(settings, captured)
    assert result["status"] == "partial"
    assert client.rows["scrape_runs"][-1]["fetch_failures"] == 1
    assert client.rows["scrape_source_runs"][-1]["status"] == "partial"


async def test_failed_empty_capture_never_opens_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, client, database = setup(monkeypatch, tmp_path)
    captured = report()
    captured["browser"].update(outcome="failed", partial=True, entries=[])
    result = await runner.store_youtube_comparison(settings, captured)
    assert result["status"] == "failed"
    assert result["posts_saved"] == 0
    database.assert_not_called()
    assert not client.rows
    assert not settings.data_dir.exists()


@pytest.mark.parametrize("field,value", [
    ("identity", "video:bad"), ("kind", "live"), ("position", True),
    ("position", 7), ("raw", "{}"),
])
async def test_invalid_browser_rows_are_rejected_before_any_storage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, field: str, value: object,
) -> None:
    settings, _, database = setup(monkeypatch, tmp_path)
    captured = report()
    captured["browser"]["entries"][-1][field] = value

    with pytest.raises(ValueError):
        await runner.store_youtube_comparison(settings, captured)

    database.assert_not_called()
    assert not settings.data_dir.exists()


@pytest.mark.parametrize("field,value", [
    ("feed_url", HOME + "feed/hype"), ("feed_kind", "news"),
    ("observed_at", "2026-10-23T10:00:01"),
    ("observed_at", "2026-10-24T10:00:01+00:00"), ("rank", False),
    ("position", 5),
])
async def test_invalid_provenance_is_rejected_before_storage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, field: str, value: object,
) -> None:
    settings, _, database = setup(monkeypatch, tmp_path)
    captured = report()
    captured["browser"]["entries"][0]["raw"]["discovery"][field] = value

    with pytest.raises(ValueError):
        await runner.store_youtube_comparison(settings, captured)

    database.assert_not_called()


async def test_interrupted_import_records_failure_and_refuses_unsafe_replay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, client, _ = setup(monkeypatch, tmp_path)
    client.fail_table = "youtube_discoveries"

    with pytest.raises(OSError):
        await runner.store_youtube_comparison(settings, report())

    assert client.rows["scrape_runs"][-1]["status"] == "failed"
    assert len(client.rows["raw_posts"]) == 4
    client.fail_table = ""

    with pytest.raises(ValueError, match="interrupted"):
        await runner.store_youtube_comparison(settings, report())

    assert len(client.rows["raw_posts"]) == 4


async def test_existing_collection_lock_blocks_import_before_db_access(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, _, database = setup(monkeypatch, tmp_path)
    settings.data_dir.mkdir()

    with (settings.data_dir / "collection.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

        with pytest.raises(runner.CollectionBusyError):
            await runner.store_youtube_comparison(settings, report())

    database.assert_not_called()


async def test_repeated_shelf_appearance_is_rejected_before_storage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    settings, _, database = setup(monkeypatch, tmp_path)
    captured = report()
    captured["browser"]["entries"][2]["raw"]["discovery"]["shelf"] = ""

    with pytest.raises(ValueError, match="shelf appearance"):
        await runner.store_youtube_comparison(settings, captured)

    database.assert_not_called()
