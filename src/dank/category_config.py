"""Validated category definitions and training settings; no service access."""

from __future__ import annotations

from typing import Annotated, Any, NamedTuple, cast

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

# Decisions request limit, not a DANK tuning parameter.
# https://developers.openai.com/api/docs/guides/decisions
MAX_CATEGORY_QUESTIONS = 400


class Category(NamedTuple):
    name: str
    description: str


class CategorySettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    api_key: SecretStr = SecretStr("")
    model: Annotated[str, Field(min_length=1)] = "gpt-6-luna"
    cosine_threshold: Annotated[float, Field(ge=-1, le=1)] = 0.85
    confidence_threshold: Annotated[float, Field(ge=0, le=1)] = 0.8
    concurrency: Annotated[int, Field(ge=1, le=16)] = 4
    max_content_chars: Annotated[int, Field(ge=1, le=32768)] = 8192
    definitions: tuple[Category, ...] = ()


def parse_categories(value: object) -> CategorySettings:
    if not isinstance(value, dict):
        raise ValueError("categories must be a table")

    data = dict(cast(dict[str, Any], value))
    definitions = data.pop("definitions", [])

    if not isinstance(definitions, list):
        raise ValueError("categories.definitions must be an array of tables")

    categories: list[Category] = []

    for raw in cast(list[object], definitions):
        item = cast(dict[str, Any], raw)

        if (
            not isinstance(raw, dict)
            or set(item) != {"name", "description"}
            or any(
                not isinstance(item[k], str) or not item[k].strip()
                for k in ("name", "description")
            )
        ):
            raise ValueError("Each category needs a name and description")

        categories.append(
            Category(item["name"].strip(), item["description"].strip()),
        )

    if len(categories) > MAX_CATEGORY_QUESTIONS:
        raise ValueError(
            "Decisions supports at most 400 categories per request",
        )

    if len({item.name for item in categories}) != len(categories):
        raise ValueError("Category names must be unique")

    try:
        settings = CategorySettings(**data, definitions=tuple(categories))
    except ValidationError:
        # Pydantic errors include input values, including credentials.
        raise ValueError("Invalid categories settings") from None

    if settings.enabled and not categories:
        raise ValueError("Enabled categories need definitions")

    return settings
