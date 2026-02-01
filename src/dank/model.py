import datetime
from typing import NamedTuple


class RawPost(NamedTuple):
    domain: str
    post_id: str
    url: str
    post_created_at: datetime.datetime | None
    scraped_at: datetime.datetime
    source: str
    request_url: str
    # A payload such as raw XML data from a scraped post.
    payload: str


class RawAsset(NamedTuple):
    domain: str
    post_id: str
    url: str
    asset_type: str
    scraped_at: datetime.datetime
    source: str
    local_path: str


class Asset(NamedTuple):
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
