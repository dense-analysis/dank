"""Pair titles and images only when they share capture evidence."""
from __future__ import annotations

from dank.storage.clickhouse import ClickHouseClient, parse_datetime

MAX_TITLES_PER_THUMBNAIL = 20


async def thumbnail_title_pairs(
    client: ClickHouseClient, ids: list[str], hashes: list[str],
) -> dict[tuple[str, str], list[dict[str, str]]]:
    if not hashes:
        return {}

    # Legacy images have no direct listing reference. Only accept an exact URL
    # in one completed scrape with one title before the image fetch.
    # A recovery rendition cannot establish the original image/title pairing.
    query = """
SELECT post_id, content_hash, paired_title AS title,
       min(paired_at) AS first_seen, max(paired_at) AS last_seen,
       max(reconstructed) AS reconstructed
FROM (
    SELECT post_id, content_hash, trimBoth(title) AS paired_title,
           title_observed_at AS paired_at, toUInt8(0) AS reconstructed
    FROM youtube_thumbnail_observations
    WHERE post_id IN %(ids)s AND content_hash IN %(hashes)s
      AND notEmpty(trimBoth(title)) AND title_observed_at IS NOT NULL
      AND title_observed_at <= observed_at AND captured_url = fetched_url
    UNION ALL
    SELECT t.post_id, t.content_hash,
           any(trimBoth(JSONExtractString(d.payload, 'title')))
               AS paired_title,
           max(d.observed_at) AS paired_at, toUInt8(1) AS reconstructed
    FROM youtube_thumbnail_observations AS t
    INNER JOIN youtube_discoveries AS d
      ON t.post_id = d.item_id
     AND t.captured_url = JSONExtractString(d.payload, 'thumbnail_url')
    INNER JOIN (
        SELECT toString(run_id) AS capture_run, started_at, finished_at
        FROM scrape_runs FINAL
    ) AS r ON d.run_id = r.capture_run
    WHERE t.post_id IN %(ids)s AND t.content_hash IN %(hashes)s
      AND empty(t.title) AND t.captured_url = t.fetched_url
      AND t.observed_at >= r.started_at AND t.observed_at <= r.finished_at
      AND d.observed_at >= r.started_at AND d.observed_at <= t.observed_at
      AND notEmpty(trimBoth(JSONExtractString(d.payload, 'title')))
    GROUP BY t.post_id, t.content_hash, t.observed_at, t.captured_url
    HAVING uniqExact(d.run_id) = 1
       AND uniqExact(trimBoth(JSONExtractString(d.payload, 'title'))) = 1
)
GROUP BY post_id, content_hash, paired_title
ORDER BY last_seen DESC, title ASC
LIMIT %(limit)s BY post_id, content_hash
"""
    result = await client.fetch_json(query, {
        "ids": ids, "hashes": hashes, "limit": MAX_TITLES_PER_THUMBNAIL,
    })
    pairs: dict[tuple[str, str], list[dict[str, str]]] = {}

    for row in result.rows:
        first = parse_datetime(row.get("first_seen"))
        last = parse_datetime(row.get("last_seen"))

        if not first or not last or not row.get("title"):
            continue

        key = (str(row["post_id"]), str(row["content_hash"]))
        pairs.setdefault(key, []).append({
            "title": str(row["title"]), "first_seen": first.isoformat(),
            "last_seen": last.isoformat(),
            "association": (
                "matched" if int(row.get("reconstructed") or 0)
                else "captured"
            ),
        })

    return pairs


def merge_title_pairs(
    existing: list[dict[str, str]], incoming: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Visually grouped encodings share pair dates only for the same title."""
    combined: dict[str, dict[str, str]] = {}

    for pair in (*existing, *incoming):
        current = combined.get(pair["title"])

        if current is None:
            combined[pair["title"]] = dict(pair)
            continue

        current["first_seen"] = min(current["first_seen"], pair["first_seen"])
        current["last_seen"] = max(current["last_seen"], pair["last_seen"])

        if pair["association"] == "matched":
            current["association"] = "matched"

    return sorted(combined.values(), key=lambda pair: (
        pair["last_seen"], pair["title"],
    ), reverse=True)[:MAX_TITLES_PER_THUMBNAIL]
