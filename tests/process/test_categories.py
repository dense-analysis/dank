from __future__ import annotations

from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.category_config import CategorySettings
from dank.process.runner import (
    _assign_categories,  # pyright: ignore[reportPrivateUsage]
)
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from tests.category_data import DEFINITIONS, embedded_post


@pytest.mark.parametrize("algorithm", ["", "model", "different-model"])
async def test_assignment_ignores_algorithm_and_passes_threshold_filters(
    algorithm: str,
) -> None:
    client = MagicMock()
    client.fetch_json = AsyncMock(return_value=QueryResult([
        {"category": "science", "example_confidence": 0.8,
         "max_similarity": 1.0},
        {"category": "software", "example_confidence": 0.95,
         "max_similarity": 0.6},
    ]))
    config = CategorySettings(
        enabled=True, cosine_threshold=0.6, confidence_threshold=0.8,
        definitions=DEFINITIONS,
    )
    rows = [embedded_post()._replace(embedding_algorithm=algorithm)]
    assigned = await _assign_categories(
        cast(ClickHouseClient, client), rows, config,
    )
    assert assigned[0].categories == (
        ("science", 0.8, 1.0), ("software", 0.95, 0.6),
    )
    client.fetch_json.assert_awaited_once()
    query, parameters = client.fetch_json.call_args.args
    assert parameters == {
        "embedding": [1.0, 0.0], "dimensions": 2,
        "categories": ["science", "software"], "confidence": 0.8,
        "cosine": 0.6,
    }
    # Check the query contract without duplicating the database's evaluator.
    assert "embedding_algorithm" not in query
    assert "category IN %(categories)s" in query
    assert "confidence >= %(confidence)s" in query
    assert "length(embedding) = %(dimensions)s" in query
    assert "similarity >= %(cosine)s" in query
    assert "cosineDistance(embedding, %(embedding)s)" in query
    assert "GROUP BY category" in query
    assert "LIMIT" not in query


async def test_no_matches_clear_previous_assignments() -> None:
    client = MagicMock()
    client.fetch_json = AsyncMock(return_value=QueryResult([]))
    post = embedded_post()
    post = post._replace(categories=(("science", 0.9, 0.95),))
    config = CategorySettings(
        enabled=True, cosine_threshold=1.0, confidence_threshold=1.0,
        definitions=DEFINITIONS,
    )
    assigned = await _assign_categories(
        cast(ClickHouseClient, client), [post], config,
    )
    assert assigned[0].categories == ()
    assert post.categories == (("science", 0.9, 0.95),)
    parameters = client.fetch_json.call_args.args[1]
    assert parameters["confidence"] == 1.0
    assert parameters["cosine"] == 1.0


async def test_invalid_vectors_do_not_query_the_database() -> None:
    client = MagicMock()
    client.fetch_json = AsyncMock()
    config = CategorySettings(enabled=True, definitions=DEFINITIONS)
    await _assign_categories(
        cast(ClickHouseClient, client), [embedded_post(vector=(0, 0))], config,
    )
    client.fetch_json.assert_not_awaited()
