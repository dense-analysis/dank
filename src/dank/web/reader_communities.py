"""Locally cached community identity for the Reddit reader."""

from __future__ import annotations

import pathlib

from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    AssetRow,
    _asset_view,  # pyright: ignore[reportPrivateUsage]
)


async def community_icons(
    client: ClickHouseClient,
    communities: list[str],
    assets_dir: pathlib.Path,
) -> dict[str, str]:
    if not communities:
        return {}

    result = await client.fetch_json(
        "SELECT community, icon_url, local_path, content_type "
        "FROM reddit_community_icons FINAL "
        "WHERE community IN %(communities)s",
        {"communities": sorted({name.lower() for name in communities})},
    )
    icons: dict[str, str] = {}

    for row in result.rows:
        asset = AssetRow(
            post_id="", url=str(row["icon_url"]),
            local_path=str(row["local_path"]),
            content_type=str(row["content_type"]), size_bytes=0,
        )
        view = _asset_view(asset, assets_dir=assets_dir)

        if view and view["kind"] == "image":
            icons[str(row["community"]).lower()] = view["url"]

    return icons
