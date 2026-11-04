"""Queries for loading local categories during processing."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def category_query() -> str:
    # Exact radius search preserves every qualifying category.
    # https://clickhouse.com/docs/en/sql-reference/functions/distance-functions
    return (
        "SELECT category, argMax(confidence, tuple(similarity, confidence)) "
        "AS example_confidence, max(similarity) AS max_similarity FROM ("
        "SELECT category, confidence, "
        "1 - cosineDistance(embedding, %(embedding)s) AS similarity "
        "FROM category_training "
        "PREWHERE category IN %(categories)s "
        "WHERE confidence >= %(confidence)s "
        "AND length(embedding) = %(dimensions)s) "
        "WHERE isFinite(similarity) AND similarity >= %(cosine)s "
        "GROUP BY category ORDER BY category"
    )


def assignment_values(
    rows: Sequence[dict[str, Any]],
) -> list[tuple[str, float, float]]:
    return [
        (
            str(row["category"]),
            float(row["example_confidence"]),
            min(1.0, max(-1.0, float(row["max_similarity"]))),
        )
        for row in rows
    ]
