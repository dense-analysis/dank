"""Current Reddit post flairs shared by reader filters and source facets."""

from __future__ import annotations

from typing import TypedDict

from dank.storage.clickhouse import ClickHouseClient

MAX_FLAIR_LENGTH = 300
# Match Python str.strip() without trimming individual UTF-8 bytes.
FLAIR_WHITESPACE_PATTERN = (
    r"^[\s\p{Z}\x{000B}\x{001C}-\x{001F}\x{0085}]+"
    r"|[\s\p{Z}\x{000B}\x{001C}-\x{001F}\x{0085}]+$"
)
REDDIT_COMMUNITY_SQL = "lowerUTF8(extract(url, '/r/([^/]+)/comments/'))"
# Aggregate the whole snapshot first: missing, cleared or malformed newer
# payloads must never restore a flair from an older capture.
LATEST_REDDIT_FLAIRS_SQL = (
    "SELECT post_id, replaceRegexpAll(if("
    "JSONType(latest_payload, 'link_flair_text') = 'String', "
    "JSONExtractString(latest_payload, 'link_flair_text'), ''), "
    "%(flair_whitespace)s, '') AS flair FROM ("
    "SELECT post_id, argMax(payload, scraped_at) AS latest_payload "
    "FROM raw_posts WHERE domain = 'reddit.com' GROUP BY post_id)"
)


class FlairCount(TypedDict):
    name: str
    count: int


async def reddit_flairs(
    client: ClickHouseClient,
) -> dict[str, list[FlairCount]]:
    """Count current flair labels on processed posts in each community."""
    result = await client.fetch_json(
        f"SELECT {REDDIT_COMMUNITY_SQL} AS community, flair AS name, "
        "count() AS count FROM posts FINAL INNER JOIN "
        f"({LATEST_REDDIT_FLAIRS_SQL}) AS reddit_flairs USING (post_id) "
        "WHERE domain = 'reddit.com' AND flair != '' "
        "AND lengthUTF8(flair) <= %(max_flair_length)s "
        "GROUP BY community, name ORDER BY community, name",
        {"max_flair_length": MAX_FLAIR_LENGTH,
         "flair_whitespace": FLAIR_WHITESPACE_PATTERN},
    )
    communities: dict[str, list[FlairCount]] = {}

    for row in result.rows:
        communities.setdefault(str(row["community"]), []).append({
            "name": str(row["name"]), "count": int(row["count"]),
        })

    return communities
