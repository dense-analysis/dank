from __future__ import annotations

import asyncio
import datetime
import re

from dank.config import Settings, load_settings
from dank.process.assets import convert_raw_asset
from dank.process.processor import process_source_assets, process_source_posts
from dank.process.rss import convert_raw_post as convert_raw_rss_post
from dank.process.x import convert_raw_x_post
from dank.storage.clickhouse import ClickHouseClient


async def run_process(
    settings: Settings,
    *,
    age: str = "24h",
) -> int:
    window = parse_age_window(age)
    since = datetime.datetime.now(datetime.UTC) - window
    total_posts = 0
    total_assets = 0

    async with ClickHouseClient(settings.clickhouse) as client:
        for source in settings.sources:
            match source.domain:
                case "x.com":
                    converter = convert_raw_x_post
                case _:
                    converter = convert_raw_rss_post

            total_posts += await process_source_posts(
                client,
                source.domain,
                converter,
                since=since,
            )

            total_assets += await process_source_assets(
                client,
                source.domain,
                convert_raw_asset,
                since=since,
            )

    return total_posts + total_assets


def run_process_from_config(
    path: str = "config.toml",
    *,
    age: str = "24h",
) -> int:
    settings = load_settings(path)

    return asyncio.run(run_process(settings, age=age))


def parse_age_window(value: str) -> datetime.timedelta:
    match = re.fullmatch(r"(\d+)\s*([a-z]*)", value.strip().lower())

    if not match:
        raise ValueError("Age must look like 30s, 10m, or 2h")

    amount = int(match.group(1))

    if amount <= 0:
        raise ValueError("Age must be greater than zero")

    # Match the unit.
    match match.group(2):
        case "" | "s" | "sec" | "secs" | "second" | "seconds":
            return datetime.timedelta(seconds=amount)
        case "m" | "min" | "mins" | "minute" | "minutes":
            return datetime.timedelta(minutes=amount)
        case "h" | "hr" | "hrs" | "hour" | "hours":
            return datetime.timedelta(hours=amount)
        case _:
            raise ValueError("Age must be in seconds, minutes, or hours")
