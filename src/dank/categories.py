"""Shared pure calculations for choosing category vectors."""

from __future__ import annotations

import math
from typing import cast

from dank.html_utils import html_text
from dank.model import Post


def category_embedding(row: Post) -> tuple[float, ...]:
    raw: object = (
        row.html_embedding
        if html_text(row.html).strip()
        else row.title_embedding
    )

    values = cast(list[object] | tuple[object, ...], raw)

    if not values:
        return ()

    if any(
        isinstance(x, bool)
        or not isinstance(x, int | float)
        or not math.isfinite(x)
        for x in values
    ):
        return ()

    vector = tuple(float(cast(float, x)) for x in values)

    return vector if math.hypot(*vector) > 0 else ()
