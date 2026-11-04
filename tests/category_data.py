from __future__ import annotations

import datetime
from typing import cast

from dank.category_config import Category
from dank.model import Post, PostTrainingData

DEFINITIONS = (
    Category("science", "Scientific research"),
    Category("software", "Programming"),
)


def training_data(*, html: str = "<p>Post body</p>") -> PostTrainingData:
    return {
        "url": "https://example.com/post",
        "author": "Author",
        "title": "Title",
        "html": html,
    }


def embedded_post(
    *, html: str = "", vector: object = (1, 0),
) -> Post:
    return Post(
        domain="example.com", post_id="1", url="https://example.com/post",
        created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        updated_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        author="Author", title="Title", html=html, source="rss",
        title_embedding=cast(tuple[float, ...], vector),
        html_embedding=(0, 1), embedding_algorithm="model",
    )
