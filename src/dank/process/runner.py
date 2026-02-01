from __future__ import annotations

import asyncio
from collections.abc import Iterable

from dank.config import Settings, SourceConfig, load_settings
from dank.process.processor import process_source_posts
from dank.process.x import convert_raw_post
from dank.storage.clickhouse import ClickHouseClient


def _resolve_sources(sources: Iterable[SourceConfig]) -> list[str]:
    resolved: list[str] = []
    unknown: list[str] = []

    for source in sources:
        domain = source.domain.strip().lower()

        if not domain:
            continue

        if domain != "x.com":
            unknown.append(source.domain)
            continue

        resolved.append(domain)

    if unknown:
        unknown_list = ", ".join(sorted(set(unknown)))

        raise ValueError(f"Unknown sources: {unknown_list}")

    return resolved


async def run_process(
    settings: Settings,
    *,
    limit: int = 500,
) -> int:
    total = 0

    async with ClickHouseClient(settings.clickhouse) as client:
        for domain in _resolve_sources(settings.sources):
            if domain == "x.com":
                total += await process_source_posts(
                    client,
                    "x.com",
                    convert_raw_post,
                    limit=limit,
                )

    return total


def run_process_from_config(
    path: str = "config.toml",
    *,
    limit: int = 500,
) -> int:
    settings = load_settings(path)

    return asyncio.run(run_process(settings, limit=limit))
