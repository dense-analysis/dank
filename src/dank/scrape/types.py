from __future__ import annotations

from typing import NamedTuple

from dank.model import RawAsset, RawPost


class ScrapeBatch(NamedTuple):
    posts: list[RawPost]
    assets: list[RawAsset]
