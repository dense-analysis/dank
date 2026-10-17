"""Rebuild and audit attributable YouTube evidence from the saved archive."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pathlib
from collections import Counter
from collections.abc import AsyncIterator
from typing import Any

from dank.config import load_settings
from dank.storage.clickhouse import ClickHouseClient
from dank.youtube_references import reference_rows

logger = logging.getLogger(__name__)
BACKFILL_BATCH = 64  # Bound memory for full-page source captures.


async def evidence_batches(
    client: ClickHouseClient, *, sources: tuple[str, ...] = (),
) -> AsyncIterator[tuple[list[dict[str, Any]], list[dict[str, Any]]]]:
    for table, raw in (("raw_posts", True), ("posts FINAL", False)):
        content = "payload" if raw else "html"
        timestamp = "scraped_at" if raw else "updated_at"
        fields = "payload" if raw else "html, title, author"
        after: tuple[Any, ...] | None = None

        while True:
            boundary = (
                " AND (domain, post_id, stamp, fingerprint) > %(after)s"
                if after
                else ""
            )
            scope = " AND source IN %(sources)s" if sources else ""
            result = await client.fetch_json(
                f"SELECT domain, post_id, url, source, {fields}, {timestamp}, "
                f"toUnixTimestamp64Milli({timestamp}) AS stamp, "
                f"cityHash64({content}) AS fingerprint FROM {table} "
                "WHERE source != 'youtube' AND domain != 'youtube.com' AND "
                f"(positionCaseInsensitive({content}, 'youtu') > 0 "
                "OR positionCaseInsensitive(url, 'youtu') > 0)"
                + scope + boundary
                + " ORDER BY domain, post_id, stamp, fingerprint "
                "LIMIT %(limit)s",
                {
                    "limit": BACKFILL_BATCH,
                    **({"sources": sources} if sources else {}),
                    **({"after": after} if after else {}),
                },
            )

            if not result.rows:
                break

            evidence = [
                link
                for row in result.rows
                for link in reference_rows(row, raw=raw)
            ]
            yield result.rows, evidence
            last = result.rows[-1]
            after = tuple(
                last[key]
                for key in ("domain", "post_id", "stamp", "fingerprint")
            )


async def backfill_evidence(
    client: ClickHouseClient, *, sources: tuple[str, ...] = (),
) -> None:
    observations = 0

    async for _, evidence in evidence_batches(client, sources=sources):
        if evidence:
            await client.insert_rows("youtube_reference_evidence", evidence)
            observations += len(evidence)

    logger.info(
        "Indexed %d archived YouTube reference observations", observations,
    )


async def audit_links(client: ClickHouseClient) -> dict[str, Any]:
    legacy = await client.fetch_json(
        "SELECT video_id, domain, post_id, any(source) AS source, "
        "any(url) AS url FROM youtube_links "
        "GROUP BY video_id, domain, post_id",
    )
    old = {(r["video_id"], r["domain"], r["post_id"]): r for r in legacy.rows}
    accepted: dict[tuple[str, ...], dict[str, Any]] = {}
    samples: dict[tuple[str, ...], dict[str, Any]] = {}
    candidates: dict[tuple[str, str], list[str]] = {}

    for video_id, domain, post_id in old:
        candidates.setdefault((domain, post_id), []).append(video_id)

    async for rows, evidence in evidence_batches(client):
        for link in evidence:
            key = tuple(
                str(link[k])
                for k in (
                    "video_id",
                    "domain",
                    "post_id",
                    "kind",
                    "reference_id",
                )
            )

            if (
                key not in accepted
                or link["observed_at"] > accepted[key]["observed_at"]
            ):
                accepted[key] = link

        for row in rows:
            content = str(row.get("payload", row.get("html", "")))

            for video_id in candidates.get(
                (row["domain"], row["post_id"]), [],
            ):
                position = content.find(video_id)
                key = (video_id, row["domain"], row["post_id"])

                if position >= 0 and key not in samples:
                    samples[key] = {
                        "field": "payload" if "payload" in row else "html",
                        "context": content[
                            max(0, position - 240) : position + 240
                        ],
                    }

    verified = {key[:3] for key in accepted}
    rejected = [
        {
            **row,
            "reason": "No attributable post link or comment in saved content",
            "sample": samples.get(key),
        }
        for key, row in sorted(old.items())
        if key not in verified
    ]

    return {
        "legacy_associations": len(old),
        "verified_legacy_associations": len(set(old) & verified),
        "excluded_legacy_associations": len(rejected),
        "additional_verified_associations": len(verified - set(old)),
        "verified_associations": len(verified),
        "verified_videos": len({k[0] for k in verified}),
        "evidence_by_kind": dict(
            Counter(r["kind"] for r in accepted.values()),
        ),
        "excluded_by_source": dict(Counter(r["source"] for r in rejected)),
        "excluded": rejected,
        "evidence": list(accepted.values()),
    }


async def _audit(config: str, output: pathlib.Path) -> None:
    settings = load_settings(config)

    async with ClickHouseClient(settings.clickhouse) as client:
        result = await audit_links(client)

    await asyncio.to_thread(
        output.write_text, json.dumps(result, indent=2, default=str) + "\n",
    )
    print(
        json.dumps(
            {
                k: v
                for k, v in result.items()
                if k not in {"excluded", "evidence"}
            },
            indent=2,
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read-only YouTube link audit",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    asyncio.run(_audit(args.config, args.output))
