import datetime
from typing import NamedTuple


class Post(NamedTuple):
    url: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    author: str
    title: str
    html: str
