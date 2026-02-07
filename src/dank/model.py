import datetime
from typing import NamedTuple


class RawPost(NamedTuple):
    """
    A raw post to be stored in the database during scraping for later
    processing.
    """
    domain: str
    post_id: str
    url: str
    post_created_at: datetime.datetime | None
    scraped_at: datetime.datetime
    source: str
    request_url: str
    # A payload such as raw XML data from a scraped post.
    payload: str


class AssetDiscovery(NamedTuple):
    """
    An asset discovery used in scraping for queuing assets to process
    into RawAsset data and downloaded asset files in the filesystem.
    """
    source: str
    domain: str
    post_id: str
    url: str
    asset_type: str


class RawAsset(NamedTuple):
    """
    A raw asset to be stored in the database during scraping for later
    processing.

    Transformed from an ``AssetDiscovery``.
    """
    domain: str
    post_id: str
    url: str
    asset_type: str
    scraped_at: datetime.datetime
    source: str
    local_path: str


class Asset(NamedTuple):
    """
    A processed asset stored in the database after processing previously
    saved raw asset data and files.

    Transformed from a ``RawAsset``.
    """
    domain: str
    post_id: str
    url: str
    local_path: str
    content_type: str
    size_bytes: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    source: str


class Post(NamedTuple):
    """
    A processed post stored in the database after processing previously
    saved raw post data.

    Transformed from a ``RawPost``.
    """
    domain: str
    post_id: str
    url: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    author: str
    title: str
    title_embedding: list[float]
    html: str
    html_embedding: list[float]
    source: str
