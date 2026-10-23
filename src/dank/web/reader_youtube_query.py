"""Bounded queries over YouTube observations and link indexes."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
from typing import Any, NamedTuple, cast

from multidict import MultiMapping

RECOMMENDATIONS_FEED = "https://www.youtube.com/"


class YouTubeFilters(NamedTuple):
    feed: str = ""
    channel: str = ""
    kind: str = ""
    q: str = ""
    item_id: str = ""
    limit: int = 30
    cursor: str = ""
    sort: str = "newest"
    channel_scope: str = ""

    def signature(self) -> str:
        values = (*self[:5], self.sort)

        if self.feed == RECOMMENDATIONS_FEED:
            values = (*values, "recommendation-history-v1")

        if self.channel_scope:
            values = (*values, self.channel_scope)

        encoded = json.dumps(values).encode()

        return hashlib.sha256(encoded).hexdigest()[:24]


def parse_youtube_filters(query: MultiMapping[str]) -> YouTubeFilters:
    allowed = {"feed", "channel", "kind", "q", "id", "limit", "cursor",
               "sort", "channel_scope"}

    if set(query) - allowed or any(len(query.getall(k)) != 1 for k in query):
        raise ValueError("Unknown or repeated YouTube filter")

    try:
        limit = int(query.get("limit", "30"))
    except ValueError:
        raise ValueError("limit must be an integer from 1 to 500") from None

    filters = YouTubeFilters(
        feed=query.get("feed", "").strip(),
        channel=query.get("channel", "").strip(),
        kind=query.get("kind", ""), q=query.get("q", "").strip(),
        item_id=query.get("id", ""), limit=limit,
        cursor=query.get("cursor", ""), sort=query.get("sort", "newest"),
        channel_scope=query.get("channel_scope", ""),
    )

    if not 1 <= limit <= 500 or filters.kind not in {
        "", "video", "short", "post",
    }:
        raise ValueError("Invalid limit or content kind")

    if any(len(value) > 2000 for value in filters[:5]):
        raise ValueError("YouTube filters are limited to 2000 characters")

    if filters.sort not in {"newest", "oldest", "most-linked"} or (
        filters.sort != "newest" and filters.feed != "linked"
    ):
        raise ValueError("Invalid sort for this YouTube feed")

    if filters.channel_scope not in {"", "configured"}:
        raise ValueError("Invalid channel scope")

    if filters.cursor:
        decode_youtube_cursor(filters)

    return filters


def encode_youtube_cursor(
    filters: YouTubeFilters, observed_at: str, item_id: str,
    position: int = 0, reference_count: int = 0, *, snapshot_time: str = "",
) -> str:
    data: list[Any] = [
        filters.signature(), observed_at, item_id, position, reference_count,
    ]

    if filters.feed == RECOMMENDATIONS_FEED:
        data.append(snapshot_time or observed_at)

    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def decode_youtube_cursor(
    filters: YouTubeFilters,
) -> tuple[str, str, int, int]:
    data = _cursor_data(filters)

    return data[1], data[2], data[3], data[4]


def youtube_cursor_snapshot(filters: YouTubeFilters) -> str:
    data = _cursor_data(filters)

    return str(data[5] if len(data) == 6 else data[1])


def _cursor_data(filters: YouTubeFilters) -> list[Any]:
    try:
        if len(filters.cursor) > 8192:
            raise ValueError

        raw = json.loads(base64.b64decode(
            filters.cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(raw, list):
            raise ValueError

        data = cast(list[Any], raw)

        if (
            len(data) != (6 if filters.feed == RECOMMENDATIONS_FEED else 5)
            or not all(isinstance(value, str) for value in data[:3])
            or type(data[3]) is not int or not 0 <= data[3] <= 2**32 - 1
            or type(data[4]) is not int or not 0 <= data[4] <= 2**64 - 1
            or data[0] != filters.signature()
        ):
            raise ValueError

        date = dt.datetime.fromisoformat(data[1])

        if date.tzinfo is None:
            raise ValueError

        data[1] = date.astimezone(dt.UTC).isoformat()

        if len(data) == 6:
            if not isinstance(data[5], str):
                raise ValueError

            snapshot = dt.datetime.fromisoformat(data[5])

            if snapshot.tzinfo is None or snapshot < date:
                raise ValueError

            data[5] = snapshot.astimezone(dt.UTC).isoformat()

        return data
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid cursor for these filters") from None


def build_youtube_query(
    filters: YouTubeFilters, *, count: bool = False,
    configured_channels: tuple[str, ...] = (),
    paginate: bool = True, snapshot_time: str = "",
) -> tuple[str, dict[str, Any]]:
    snapshot_time = (
        youtube_cursor_snapshot(filters)
        if filters.cursor and filters.feed == RECOMMENDATIONS_FEED
        else snapshot_time
    )
    params: dict[str, Any] = {"limit": filters.limit + 1}
    conditions: list[str] = []
    table = _metadata_items(
        _items_table(filters, snapshot_time), prefer_metadata=(
            filters.feed == "linked"
            or bool(filters.item_id and not filters.feed)
        ),
    )

    if snapshot_time:
        params["snapshot_time"] = snapshot_time

    if filters.channel_scope == "configured":
        conditions.append(
            "(JSONExtractString(payload, 'channel_id') IN %(configured)s "
            "OR JSONExtractString(payload, 'channel_url') IN %(configured)s)"
            if configured_channels else "0",
        )
        params["configured"] = configured_channels

    if filters.feed and filters.feed != "linked":
        params["feed"] = filters.feed

        if filters.cursor:
            params["snapshot_time"] = youtube_cursor_snapshot(filters)

    for key, value, condition in (
        ("kind", filters.kind, "kind = %(kind)s"),
        ("id", filters.item_id, "item_id = %(id)s"),
        ("channel", filters.channel,
         "(JSONExtractString(payload, 'channel_id') = %(channel)s OR "
         "JSONExtractString(payload, 'channel_url') = %(channel)s)"),
        ("q", filters.q,
         "positionCaseInsensitiveUTF8(concat("
         "JSONExtractString(payload, 'title'), ' ', "
         "JSONExtractString(payload, 'text'), ' ', "
         "JSONExtractString(payload, 'channel_name'), ' ', "
         "search_text), %(q)s) > 0"),
    ):
        if value:
            conditions.append(condition)
            params[key] = value

    order_by = "observed_at DESC, position ASC, item_id DESC"
    cursor_columns = "(observed_at, -toInt64(position), item_id)"
    cursor_values = (
        "(parseDateTime64BestEffort(%(cursor_time)s, 3), "
        "-toInt64(%(cursor_position)s), %(cursor_id)s)"
    )
    comparison = "<"

    if filters.sort == "oldest":
        order_by = "observed_at ASC, position DESC, item_id ASC"
        comparison = ">"
    elif filters.sort == "most-linked":
        order_by = "reference_count DESC, observed_at DESC, item_id DESC"
        cursor_columns = "(reference_count, observed_at, item_id)"
        cursor_values = (
            "(%(cursor_count)s, "
            "parseDateTime64BestEffort(%(cursor_time)s, 3), %(cursor_id)s)"
        )

    if filters.cursor and not count and paginate:
        timestamp, item_id, position, reference_count = decode_youtube_cursor(
            filters,
        )
        conditions.append(f"{cursor_columns} {comparison} {cursor_values}")
        params.update(cursor_time=timestamp, cursor_id=item_id,
                      cursor_position=position, cursor_count=reference_count)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    select = "count() AS total" if count else "*"
    order = ("" if count or not paginate
             else f" ORDER BY {order_by} LIMIT %(limit)s")

    return f"SELECT {select} FROM ({table}){where}{order}", params


def _feed_items(filters: YouTubeFilters, snapshot_time: str = "") -> str:
    # Ranked feeds retain one captured run; home recommendations span history.
    # Apply the fixed cutoff before selecting each item's latest observation.
    snapshot_bound = (
        " AND observed_at <= "
        "parseDateTime64BestEffort(%(snapshot_time)s, 3)"
        if filters.cursor or snapshot_time else ""
    )
    where = (
        " WHERE feed_url = %(feed)s AND (feed_kind = 'channel' OR "
        "run_id = (SELECT argMax(run_id, observed_at) "
        "FROM youtube_discoveries WHERE feed_url = %(feed)s"
        f"{snapshot_bound}))"
        if filters.feed else (
            " WHERE d.observed_at <= "
            "parseDateTime64BestEffort(%(snapshot_time)s, 3)"
            if snapshot_time else ""
        )
    )

    if filters.feed == RECOMMENDATIONS_FEED:
        where = " WHERE d.feed_url = %(feed)s"

    if snapshot_time and filters.feed:
        where += (
            " AND d.observed_at <= "
            "parseDateTime64BestEffort(%(snapshot_time)s, 3)"
        )

    position = (
        "if(d.feed_kind != 'hype' AND "
        "JSONExtractUInt(d.payload, 'discovery', 'position') > 0, "
        "JSONExtractUInt(d.payload, 'discovery', 'position'), "
        "if(d.rank = 0, 4294967295, d.rank))"
    )

    return (
        "SELECT item_id, argMax(d.kind, d.observed_at) AS kind, "
        "max(d.observed_at) AS observed_at, "
        "argMax(d.payload, d.observed_at) AS payload, '' AS search_text, "
        f"argMax({position}, (d.observed_at, -toInt64({position}))) "
        "AS position, toUInt64(0) AS reference_count "
        f"FROM youtube_discoveries AS d{where} GROUP BY item_id"
    )


def _linked_items(snapshot_time: str = "") -> str:
    # The index keeps paging independent of source payload size.
    bound = (
        " WHERE youtube_reference_evidence.observed_at <= "
        "parseDateTime64BestEffort(%(snapshot_time)s, 3)"
        if snapshot_time else ""
    )

    discovered_bound = bound.replace("youtube_reference_evidence.", "d.")

    return (
        "SELECT concat('video:', links.video_id) AS item_id, "
        "if(empty(discovered.kind), links.kind, discovered.kind) AS kind, "
        "links.observed_at AS observed_at, discovered.payload AS payload, "
        "links.search_text AS search_text, toUInt32(0) AS position, "
        "links.reference_count AS reference_count FROM ("
        "SELECT video_id, max(observed_at) AS observed_at, "
        "uniqExact(tuple(domain, post_id, "
        "youtube_reference_evidence.kind, reference_id)) "
        "AS reference_count, "
        "if(countIf(format_hint = 'short') > 0, 'short', 'video') AS kind, "
        "arrayStringConcat(groupUniqArray(100)(concat(title, ' ', excerpt)), "
        "' ') AS search_text "
        f"FROM youtube_reference_evidence FINAL{bound} "
        "GROUP BY video_id) AS links "
        "LEFT JOIN ("
        "SELECT item_id, argMax(d.kind, d.observed_at) AS kind, "
        "argMax(d.payload, d.observed_at) AS payload "
        f"FROM youtube_discoveries AS d{discovered_bound} GROUP BY item_id"
        ") AS discovered ON discovered.item_id = "
        "concat('video:', links.video_id)"
    )


def _items_table(filters: YouTubeFilters, snapshot_time: str = "") -> str:
    if filters.feed == "linked":
        return _linked_items(snapshot_time)

    if filters.item_id and not filters.feed:
        # A shared detail link also resolves a video only found in other posts.
        return (
            "SELECT item_id, argMax(i.kind, i.observed_at) AS kind, "
            "max(i.observed_at) AS observed_at, "
            "argMax(i.payload, i.observed_at) AS payload, "
            "argMax(i.search_text, i.observed_at) AS search_text, "
            "toUInt32(0) AS position, "
            "max(i.reference_count) AS reference_count "
            "FROM ("
            + _feed_items(filters) + " UNION ALL " + _linked_items()
            + ") AS i GROUP BY item_id"
        )

    return _feed_items(filters, snapshot_time)


def _metadata_items(items: str, *, prefer_metadata: bool = False) -> str:
    # Feed captures own their title. Metadata fills gaps and carries its own
    # capture state; a failed refresh never erases the last successful preview.
    # clickhouse-connect 0.8 interprets brace/colon spans as server parameters;
    # construct empty JSON without braces so ordinary filters still bind.
    captured = "if(empty(i.payload), toJSONString(map()), i.payload)"
    metadata = "if(empty(m.payload), toJSONString(map()), m.payload)"
    combined = (
        f"JSONMergePatch({captured}, {metadata})" if prefer_metadata else
        "if(notEmpty(JSONExtractString(i.payload, 'title')), "
        f"JSONMergePatch({metadata}, {captured}), "
        f"JSONMergePatch({captured}, {metadata}))"
    )

    return (
        "SELECT i.item_id AS item_id, i.kind AS kind, "
        "i.observed_at AS observed_at, i.search_text AS search_text, "
        "i.reference_count AS reference_count, "
        f"i.position AS position, {combined} AS payload "
        "FROM (" + items + ") AS i LEFT JOIN ("
        "SELECT v.video_id AS video_id, JSONMergePatch("
        "if(empty(argMaxIf(v.payload, v.observed_at, v.status = 'captured')), "
        "toJSONString(map()), "
        "argMaxIf(v.payload, v.observed_at, v.status = 'captured')), "
        "argMax(v.payload, v.observed_at)) AS payload "
        "FROM youtube_video_metadata AS v GROUP BY v.video_id"
        ") AS m ON i.item_id = concat('video:', m.video_id)"
    )
