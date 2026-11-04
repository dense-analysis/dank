"""Saved topic labels and category filtering in both reader contracts."""

import datetime as dt
import json
import pathlib
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.api.posts.schemas import PostOut, PostsQuery
from dank.category_config import Category, CategorySettings
from dank.web.app import _parse_post_row  # pyright: ignore[reportPrivateUsage]
from dank.web.reader_api import handle_categories
from dank.web.reader_posts import post_payload
from dank.web.reader_query import build_query, encode_cursor, parse_filters


def test_categories_are_exact_and_bound_to_cursors() -> None:
    query = MultiDict(
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
        parse_filters(MultiDict([("category", "other"), ("cursor", cursor)]))


@pytest.mark.parametrize("name", ["", " ", "x" * 1001])
def test_invalid_categories(name: str) -> None:
    with pytest.raises(ValueError, match="Category"):
        parse_filters(MultiDict([("category", name)]))


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
    post = _parse_post_row({"categories": categories})
    payload = post_payload(post, [], tmp_path)
    assert payload["categories"] == [
        {"name": "Software", "confidence": 0.9, "similarity": 0.95},
    ]
    assert PostOut.model_validate(payload).categories[0].name == "Software"
    assert post_payload(_parse_post_row({}), [], tmp_path)["categories"] == []



@pytest.mark.parametrize("names", [(), ("Software", "Science")])
async def test_category_names_without_database(names: tuple[str, ...]) -> None:
    app = web.Application()
    app['state'] = SimpleNamespace(settings=SimpleNamespace(
        categories=CategorySettings(definitions=tuple(
            Category(name, 'Topic description') for name in names
        )),
    ))
    request = make_mocked_request('GET', '/api/reader/categories', app=app)
    response = await handle_categories(request)
    assert response.text is not None
    assert json.loads(response.text) == {'categories': list(names)}
