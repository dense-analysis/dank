"""Versioned, bounded archive migration for attributable source connections."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from collections import OrderedDict
from typing import Any

from dank.references import reference_rows
from dank.storage.clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)
INDEX_VERSION = 1
BACKFILL_BATCH = 64  # Raw article pages can be large; never load the archive.


async def ensure_reference_storage(client: ClickHouseClient) -> None:
    schema = pathlib.Path(__file__).parents[1] / "storage/references.sql"

    for statement in schema.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)

    complete = await client.fetch_json(
        "SELECT version FROM reference_index_versions FINAL "
        "WHERE version = %(version)s", {"version": INDEX_VERSION},
    )

    if complete.rows:
        return

    await backfill_references(client)
    await client.insert_rows("reference_index_versions", [{
        "version": INDEX_VERSION, "completed_at": dt.datetime.now(dt.UTC),
    }])


async def backfill_references(client: ClickHouseClient) -> None:
    count = 0
    cache: OrderedDict[tuple[Any, ...], list[dict[str, Any]]] = OrderedDict()

    for table, raw in (("raw_posts", True), ("posts FINAL", False)):
        content = "payload" if raw else "html"
        timestamp = "scraped_at" if raw else "updated_at"
        fields = "payload" if raw else "html, title, author"
        keys = (
            ("domain", "stamp", "post_id", "fingerprint")
            if raw else ("domain", "post_id")
        )
        ordering = ", ".join(keys).replace("stamp", "scraped_at")
        boundary_keys = ", ".join(keys).replace(
            "stamp", "toUnixTimestamp64Milli(scraped_at)",
        )
        after: tuple[Any, ...] | None = None

        while True:
            boundary = (
                f" AND ({boundary_keys}) > %(after)s"
                if after else ""
            )
            sources = (
                "('rss', 'linked_article', 'hacker_news', "
                "'reddit', 'x', 'youtube')"
                if raw else "('rss', 'linked_article')"
            )
            result = await client.fetch_json(
                f"SELECT domain, post_id, url, source, {fields}, {timestamp}, "
                f"toUnixTimestamp64Milli({timestamp}) AS stamp, "
                f"cityHash64({content}, url, source) AS fingerprint "
                f"FROM {table} "
                f"WHERE source IN {sources}" + boundary
                + f" ORDER BY {ordering} "
                "LIMIT %(limit)s",
                {"limit": BACKFILL_BATCH,
                 **({"after": after} if after else {})},
            )

            if not result.rows:
                break

            evidence: list[dict[str, Any]] = []

            for row in result.rows:
                key = (raw, row["domain"], row["post_id"], row["fingerprint"])

                if key not in cache:
                    cache[key] = reference_rows(row, raw=raw)

                    if len(cache) > 128:
                        cache.popitem(last=False)

                cache.move_to_end(key)
                evidence.extend({**link, "observed_at": row[timestamp]}
                                for link in cache[key])

            if evidence:
                await client.insert_rows("post_reference_evidence", evidence)
                count += len(evidence)

            last = result.rows[-1]
            after = tuple(last[key] for key in keys)

    logger.info("Indexed %d archived source link observations", count)
