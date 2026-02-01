from __future__ import annotations

import asyncio
import pathlib
from collections.abc import Iterable

from dank.config import Settings, SourceConfig, load_settings
from dank.model import RawAsset, RawPost
from dank.scrape.x import scrape_accounts
from dank.scrape.zendriver import BrowserSession
from dank.storage.clickhouse import ClickHouseClient


def _resolve_sources(sources: Iterable[SourceConfig]) -> list[SourceConfig]:
    resolved: list[SourceConfig] = []
    unknown: list[str] = []

    for source in sources:
        domain = source.domain.strip().lower()

        if not domain:
            continue

        if domain != "x.com":
            unknown.append(source.domain)
            continue

        if domain != source.domain:
            resolved.append(
                SourceConfig(domain=domain, accounts=source.accounts),
            )
        else:
            resolved.append(source)

    if unknown:
        unknown_list = ", ".join(sorted(set(unknown)))
        raise ValueError(f"Unknown sources: {unknown_list}")

    return resolved


async def run_scrape(
    settings: Settings,
    *,
    headless: bool = False,
    batch_size: int = 50,
) -> None:
    assets_dir = pathlib.Path(settings.assets_dir)
    assets_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = assets_dir.parent / "browser-profile"
    async with ClickHouseClient(settings.clickhouse) as client:
        await client.execute("SELECT 1")
        session = BrowserSession(
            headless=headless,
            browser_executable_path=(
                str(settings.browser.executable_path)
                if settings.browser.executable_path
                else None
            ),
            connection_timeout=settings.browser.connection_timeout,
            connection_max_tries=settings.browser.connection_max_tries,
            keep_open=not headless,
            profile_dir=profile_dir,
        )
        pending_posts: list[RawPost] = []
        pending_assets: list[RawAsset] = []

        async def flush_posts() -> None:
            nonlocal pending_posts
            if not pending_posts:
                return
            await client.insert_json_rows(
                "raw_posts",
                [post._asdict() for post in pending_posts],
            )
            pending_posts = []

        async def flush_assets() -> None:
            nonlocal pending_assets
            if not pending_assets:
                return
            await client.insert_json_rows(
                "raw_assets",
                [asset._asdict() for asset in pending_assets],
            )
            pending_assets = []

        failed = False

        try:
            for source in _resolve_sources(settings.sources):
                x_settings = settings.x

                async for batch in scrape_accounts(
                    x_settings,
                    source.accounts,
                    settings.email,
                    assets_dir,
                    session,
                ):
                    if batch.posts:
                        pending_posts.extend(batch.posts)

                    if batch.assets:
                        pending_assets.extend(batch.assets)

                    if len(pending_posts) >= batch_size:
                        await flush_posts()

                    if len(pending_assets) >= batch_size:
                        await flush_assets()
        except Exception:
            failed = True
            raise
        finally:
            await flush_posts()
            await flush_assets()

            if failed and not headless:
                await session.hold_open()

            await session.close()


def run_scrape_from_config(
    path: str = "config.toml",
    *,
    headless: bool = False,
) -> None:
    settings = load_settings(path)
    asyncio.run(run_scrape(settings, headless=headless))
