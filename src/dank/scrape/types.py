from __future__ import annotations

from typing import NamedTuple

from dank.model import AssetDiscovery, RawPost, RedditCommunityDiscovery


class ScrapeBatch(NamedTuple):
    """
    Raw post/reference captures and AssetDiscovery values.

    This type is intended to be used to pull out data while scraping pages
    so raw post data can be saved to the database, and asset data can be
    downloaded by asset downloaders after they've been discovered and queued
    for downloading.
    """
    posts: list[RawPost]
    assets: list[AssetDiscovery]
    source_index: int = 0
    communities: tuple[RedditCommunityDiscovery, ...] = ()


    @property
    def post_count(self) -> int:
        return sum(not post.is_reference for post in self.posts)

    @property
    def reference_count(self) -> int:
        return sum(post.is_reference for post in self.posts)


class ScrapeTotals(NamedTuple):
    posts: int = 0
    assets_available: int = 0
    assets_unavailable: int = 0
