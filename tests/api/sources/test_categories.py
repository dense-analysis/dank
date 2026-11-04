"""Configured category names require no database access."""

import httpx
import pytest
from fastapi import FastAPI

from dank.api.deps import settings as settings_dependency
from dank.category_config import Category, CategorySettings
from dank.config import Settings


@pytest.mark.parametrize("names", [(), ("Software", "Science")])
async def test_category_catalog(
    app: FastAPI, client: httpx.AsyncClient, settings: Settings,
    names: tuple[str, ...],
) -> None:
    configured = settings._replace(categories=CategorySettings(
        definitions=tuple(
            Category(name, "Topic description") for name in names
        ),
    ))
    app.dependency_overrides[settings_dependency] = lambda: configured
    response = await client.get('/api/reader/categories')
    assert response.status_code == 200
    assert response.json() == {'categories': list(names)}
