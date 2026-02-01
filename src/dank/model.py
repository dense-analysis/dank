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
    # A payload such as a JSON string of data from a scraped post.
    payload: str


class RawAsset(NamedTuple):
    domain: str
    post_id: str
    url: str
    asset_type: str
    scraped_at: datetime.datetime
    source: str
    local_path: str


class Post(NamedTuple):
    domain: str
    post_id: str
    url: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    author: str
    title: str
    html: str
    source: str
