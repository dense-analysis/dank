"""Snapshot-bound browsing of saved YouTube playlists and their occurrences."""

from __future__ import annotations

import base64
import datetime as dt
import json
import re
from typing import Any, NamedTuple, cast

from aiohttp import web
from multidict import MultiMapping

from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.reader_youtube import (  # pyright: ignore[reportPrivateUsage]
    _http_url,  # pyright: ignore[reportPrivateUsage]
    youtube_items,
)
from dank.web.reader_youtube_query import YouTubeFilters
from dank.youtube import VIDEO_ID, payload_object

PLAYLIST_ID = re.compile(r"[A-Za-z0-9_-]{1,200}")
PAGE_LIMIT = 100


class PlaylistPage(NamedTuple):
    limit: int = 30
    q: str = ""
    snapshot: str = ""
    after: tuple[str, str] | int | None = None
    sort: str = "recent"


def playlist_cursor(
    identity: str,
    query: str,
    snapshot: str,
    after: tuple[str, str] | int,
    sort: str = "recent",
) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(
            [
                identity,
                query,
                snapshot,
                after,
                sort,
            ],
        ).encode(),
    ).decode()


def parse_page(
    query: MultiMapping[str],
    playlist_id: str = "",
) -> PlaylistPage:
    allowed = {"limit", "cursor"}

    if not playlist_id:
        allowed.update({"q", "sort"})

    if set(query) - allowed or any(len(query.getall(k)) != 1 for k in query):
        raise ValueError("Unknown or repeated playlist filter")

    try:
        limit = int(query.get("limit", "30"))
    except ValueError:
        raise ValueError("Playlist limit must be an integer") from None

    search = query.get("q", "").strip()
    sort = query.get("sort", "recent")

    if sort not in {"recent", "az"}:
        raise ValueError("Playlist sort must be recent or az")

    if not 1 <= limit <= PAGE_LIMIT or len(search) > 2000:
        raise ValueError("Invalid playlist limit or search")

    if playlist_id and not PLAYLIST_ID.fullmatch(playlist_id):
        raise ValueError("Invalid YouTube playlist identifier")

    snapshot = ""
    after = None

    if cursor := query.get("cursor", ""):
        snapshot, after = parse_cursor(cursor, playlist_id, search, sort)

    return PlaylistPage(limit, search, snapshot, after, sort)


def parse_cursor(
    cursor: str,
    identity: str,
    query: str,
    sort: str = "recent",
) -> tuple[str, tuple[str, str] | int]:
    try:
        if len(cursor) > 16384:
            raise ValueError

        decoded: object = json.loads(
            base64.b64decode(
                cursor,
                altchars=b"-_",
                validate=True,
            ),
        )

        if not isinstance(decoded, list):
            raise ValueError

        values = cast(list[Any], decoded)

        if len(values) != 5 or values[4] != sort:
            raise ValueError

        if values[:2] != [identity, query] or not isinstance(values[2], str):
            raise ValueError

        snapshot = dt.datetime.fromisoformat(values[2])

        if snapshot.tzinfo is None:
            raise ValueError

        after = _cursor_boundary(values[3], identity)

        return snapshot.astimezone(dt.UTC).isoformat(), after
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid cursor for this playlist view") from None


def _cursor_boundary(value: Any, identity: str) -> tuple[str, str] | int:
    if identity:
        if type(value) is not int or not 1 <= value <= 2**32 - 1:
            raise ValueError

        return value

    if not isinstance(value, list):
        raise ValueError

    parts = cast(list[Any], value)

    if (len(parts) != 2 or any(
        not isinstance(part, str) or len(part) > 4000 for part in parts
    )):
        raise ValueError

    return str(parts[0]), str(parts[1])


def latest_playlists_query(*, individual: bool = False) -> str:
    selected = " AND playlist_id = %(playlist_id)s" if individual else ""

    return (
        "SELECT * FROM youtube_playlists WHERE checked_at <= "
        "parseDateTime64BestEffort(%(snapshot)s, 6)" + selected
        + " ORDER BY checked_at DESC LIMIT 1 BY playlist_id"
    )


def library_query(
    page: PlaylistPage,
    *,
    count: bool = False,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"snapshot": page.snapshot}
    conditions: list[str] = []

    if page.q:
        conditions.append(
            "positionCaseInsensitiveUTF8(concat(title, ' ', "
            "description, ' ', channel_name), %(q)s) > 0",
        )
        params["q"] = page.q

    if isinstance(page.after, tuple) and not count:
        conditions.append(
            "(sort_key, playlist_id) > (%(after_key)s, %(after_id)s)",
        )
        params.update(after_key=page.after[0], after_id=page.after[1])

    select = "count() AS total" if count else "*"
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    order = (
        ""
        if count
        else (" ORDER BY sort_key ASC, playlist_id ASC LIMIT %(limit)s")
    )
    params["limit"] = page.limit + 1

    key = "lowerUTF8(title)"

    if page.sort == "recent":
        library = (
            "SELECT any(playlist_ids) FROM (SELECT playlist_ids "
            "FROM youtube_playlist_library WHERE observed_at <= "
            "parseDateTime64BestEffort(%(snapshot)s, 6) "
            "ORDER BY observed_at DESC LIMIT 1)"
        )
        rank = f"indexOf(({library}), playlist_id)"
        # Source positions are authoritative. Archived/configured playlists
        # absent from that library follow it alphabetically.
        key = (
            f"concat(leftPad(toString(if({rank} = 0, 4294967295, "
            f"{rank})), 10, '0'), lowerUTF8(title))"
        )

    ordered = f"SELECT *, {key} AS sort_key FROM ({latest_playlists_query()})"

    return f"SELECT {select} FROM ({ordered}){where}{order}", params


def playlist_metadata(row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or "not_collected")
    count = row.get("reported_count")

    return {
        "id": str(row["playlist_id"]),
        "title": str(row.get("title") or "Untitled playlist"),
        "url": ("https://www.youtube.com/playlist?list="
                + str(row["playlist_id"])),
        "description": str(row.get("description") or ""),
        "thumbnail_url": _http_url(row.get("thumbnail_url")),
        "channel": {
            "id": str(row.get("channel_id") or ""),
            "name": str(row.get("channel_name") or ""),
            "url": _http_url(row.get("channel_url")) or "",
        },
        "video_count": int(count) if count is not None else None,
        "collected_count": int(row.get("entry_count") or 0),
        "status": status, "complete": status == "complete",
        "observed_at": _timestamp(row.get("observed_at")),
        "checked_at": _timestamp(row.get("checked_at")),
        "order": str(row.get("sort_order") or "YouTube order"),
        "snapshot_id": str(row.get("snapshot_hash") or ""),
    }


def _timestamp(value: object) -> str:
    timestamp = parse_datetime(value)

    return timestamp.isoformat() if timestamp else ""


async def playlist_snapshot_time(
    client: ClickHouseClient,
    playlist_id: str = "",
) -> str:
    # A capture can finish before its rows are published. Freeze on a persisted
    # check, so publishing that in-flight capture cannot change a later page.
    where = " WHERE playlist_id = %(playlist_id)s" if playlist_id else ""
    query = (
        "SELECT max(checked_at) AS snapshot_time FROM youtube_playlists"
        + where
    )

    if not playlist_id:
        query = (
            "SELECT max(snapshot_time) AS snapshot_time FROM ("
            + query
            + " UNION ALL SELECT max(observed_at) AS snapshot_time "
            "FROM youtube_playlist_library)"
        )

    result = await client.fetch_json(query, {"playlist_id": playlist_id})
    value = result.rows[0].get("snapshot_time") if result.rows else None

    return _timestamp(value) or (
        dt.datetime.fromtimestamp(0, dt.UTC).isoformat()
    )


async def handle_playlists(request: web.Request) -> web.Response:
    try:
        page = parse_page(request.query)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]

    if not page.snapshot:
        page = page._replace(snapshot=await playlist_snapshot_time(client))

    result = await client.fetch_json(*library_query(page))
    totals = await client.fetch_json(*library_query(page, count=True))
    rows = result.rows[: page.limit]
    cursor = None

    if len(result.rows) > page.limit and rows:
        last = rows[-1]
        cursor = playlist_cursor(
            "",
            page.q,
            page.snapshot,
            (
                str(last["sort_key"]),
                str(last["playlist_id"]),
            ),
            page.sort,
        )

    return web.json_response(
        {
            "playlists": [playlist_metadata(row) for row in rows],
            "total": int(totals.rows[0]["total"]) if totals.rows else 0,
            "next_cursor": cursor,
        },
    )


async def handle_playlist(request: web.Request) -> web.Response:
    playlist_id = request.match_info["playlist_id"]

    try:
        page = parse_page(request.query, playlist_id)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]

    if not page.snapshot:
        page = page._replace(
            snapshot=await playlist_snapshot_time(client, playlist_id),
        )

    result = await client.fetch_json(
        latest_playlists_query(individual=True),
        {"playlist_id": playlist_id, "snapshot": page.snapshot},
    )

    if not result.rows:
        return web.json_response({"error": "Playlist not found"}, status=404)

    row = result.rows[0]
    metadata = playlist_metadata(row)
    entries = await client.fetch_json(
        "SELECT * FROM youtube_playlist_entries FINAL "
        "WHERE playlist_id = %(playlist_id)s "
        "AND snapshot_hash = %(snapshot_hash)s AND position > %(after)s "
        "ORDER BY position ASC LIMIT %(limit)s",
        {"playlist_id": playlist_id, "snapshot_hash": metadata["snapshot_id"],
         "after": page.after or 0, "limit": page.limit + 1},
    )
    rows = entries.rows[:page.limit]
    items = await playlist_items(request, rows, row)
    cursor = None

    if len(entries.rows) > page.limit and rows:
        cursor = playlist_cursor(
            playlist_id, "", page.snapshot, int(rows[-1]["position"]),
        )

    return web.json_response({
        "playlist": metadata, "items": items, "next_cursor": cursor,
        "total": metadata["collected_count"],
    })


async def playlist_items(
    request: web.Request, rows: list[dict[str, Any]], playlist: dict[str, Any],
) -> list[dict[str, Any]]:
    adapted = [_entry_row(row, playlist["observed_at"]) for row in rows]
    items = await youtube_items(
        request.app["clickhouse"], adapted,
        YouTubeFilters(feed=str(playlist["url"])),
        request.app["state"].assets_dir,
    )

    for item, row in zip(items, rows, strict=True):
        available = bool(row["available"]) and bool(
            VIDEO_ID.fullmatch(str(row["video_id"])),
        )
        item.update(
            playlist_position=int(row["position"]),
            playlist_entry_id=str(row["entry_id"]), available=available,
        )

        if not available:
            item.update(video_url=None, url="")

    return items


def _entry_row(row: dict[str, Any], observed_at: object) -> dict[str, Any]:
    video_id = str(row["video_id"])
    valid = bool(VIDEO_ID.fullmatch(video_id))
    payload = payload_object(row.get("payload"))
    payload.update({
        key: row.get(key) or payload.get(key) or "" for key in (
            "title", "channel_id", "channel_name", "channel_url",
            "thumbnail_url",
        )
    })
    if not payload["title"] and not row["available"]:
        payload["title"] = "Unavailable video"

    seconds = row.get("duration_seconds")

    if seconds is not None:
        minutes, remainder = divmod(max(0, int(seconds)), 60)
        hours, minutes = divmod(minutes, 60)
        payload["duration_text"] = (
            f"{hours}:{minutes:02}:{remainder:02}" if hours
            else f"{minutes}:{remainder:02}"
        )

    is_short = isinstance(payload.get("shortsLockupViewModel"), dict)

    return {
        "item_id": ("video:" + video_id if valid
                    else "unavailable:" + str(row["position"])),
        "kind": "short" if is_short else "video",
        "observed_at": observed_at,
        "payload": json.dumps(payload),
    }
