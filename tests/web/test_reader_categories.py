"""Saved topic labels and category filtering in the reader contract."""

import datetime as dt
import pathlib

import pytest
from starlette.datastructures import QueryParams

from dank.api.posts.schemas import PostOut, PostsQuery
from dank.web.post_rows import parse_post_row
from dank.web.reader_posts import post_payload
from dank.web.reader_query import build_query, encode_cursor
from tests.web.query import parse_filters


def test_categories_are_exact_and_bound_to_cursors() -> None:
    query = QueryParams(
        [
            ("category", "Software"),
            ("category", "science' OR 1=1"),
            ("category", "Software"),
            ("domain", "example.com"),
        ],
    )
    filters = parse_filters(query)
    assert filters.categories == ("Software", "science' OR 1=1")
    sql, params = build_query(filters, ())
    assert "hasAny(categories.name, %(categories)s)" in sql
    assert "science' OR 1=1" not in sql
    assert params["categories"] == ["Software", "science' OR 1=1"]
    assert params["domains"] == ["example.com"]
    assert (
        PostsQuery(
            category=("Software", "science' OR 1=1"), domain=("example.com",),
        ).filters()
        == filters
    )
    cursor = encode_cursor(
        filters, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "example.com", "1",
    )

    with pytest.raises(ValueError, match="cursor"):
        parse_filters(QueryParams([("category", "other"), ("cursor", cursor)]))


@pytest.mark.parametrize("name", ["", " ", "x" * 1001])
def test_invalid_categories(name: str) -> None:
    with pytest.raises(ValueError, match="Category"):
        parse_filters(QueryParams([("category", name)]))


@pytest.mark.parametrize(
    "categories",
    [
        [["Software", 0.9, 0.95]],
        [{"name": "Software", "confidence": 0.9, "similarity": 0.95}],
    ],
)
def test_assignment_payload(
    categories: list[object], tmp_path: pathlib.Path,
) -> None:
    post = parse_post_row({"categories": categories})
    payload = post_payload(post, [], tmp_path)
    assert payload["categories"] == [
        {"name": "Software", "confidence": 0.9, "similarity": 0.95},
    ]
    assert PostOut.model_validate(payload).categories[0].name == "Software"
    assert post_payload(parse_post_row({}), [], tmp_path)["categories"] == []
