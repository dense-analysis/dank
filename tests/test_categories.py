from __future__ import annotations

import pytest

from dank.categories import category_embedding
from tests.category_data import embedded_post


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        ("<p>body</p>", (0.0, 1.0)),
        ("", (1.0, 0.0)),
        ("<script>code</script>", (1.0, 0.0)),
    ],
)
def test_vector_choice(html: str, expected: tuple[float, ...]) -> None:
    assert category_embedding(embedded_post(html=html)) == expected


@pytest.mark.parametrize(
    "vector",
    [[], [0, 0], [float("nan"), 1], [True], ["1"]],
)
def test_invalid_vectors_are_skipped(vector: object) -> None:
    assert category_embedding(embedded_post(vector=vector)) == ()
