from __future__ import annotations

from typing import NamedTuple

from dank.model import AssetDiscovery, RawPost


class ScrapeBatch(NamedTuple):
    """
    A tuple of raw posts and AssetDiscovery values.

    This type is intended to be used to pull out data while scraping pages
    so raw post data can be saved to the database, and asset data can be
    downloaded by asset downloaders after they've been discovered and queued
    for downloading.
    """
    posts: list[RawPost]
    assets: list[AssetDiscovery]
