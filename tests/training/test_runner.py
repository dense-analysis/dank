from __future__ import annotations

import asyncio
import datetime
from collections.abc import Iterable
from typing import Any, TypedDict, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.category_config import Category, CategorySettings
from dank.model import Post, PostTrainingData
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.training.decisions import (
    AnswerError,
    TrainingError,
)
from dank.training.runner import (
    _train_posts,  # pyright: ignore[reportPrivateUsage]
)
from tests.category_data import DEFINITIONS, embedded_post


class TrainingExample(TypedDict):
    embedding: list[float]
    embedding_algorithm: str
    category: str
    confidence: float


class FakeTrainingClient:
    """Stateful storage for resume and partial-write orchestration tests."""

    def __init__(self, posts: list[Post]) -> None:
        self.posts = posts
        self.examples: list[TrainingExample] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any],
    ) -> QueryResult:
        if query.startswith("SELECT DISTINCT category"):
            return QueryResult([{"category": row["category"]}
                                for row in self.examples
                                if row["embedding"] == params["embedding"]
                                and row["embedding_algorithm"]
                                == params["algorithm"]])

        assert "FROM posts FINAL" in query
        cursor = cast(tuple[str, str], params["cursor"])
        posts = [row for row in self.posts
                 if (row.domain, row.post_id) > cursor]

        return QueryResult([
            row._asdict() for row in posts[:params["page_size"]]
        ])

    async def insert_rows(
        self, table: str, rows: Iterable[dict[str, Any]],
    ) -> None:
        assert table == "category_training"
        self.examples.extend(cast(TrainingExample, row) for row in rows)


@pytest.mark.parametrize("algorithm", ["model", ""])
async def test_training_resumes_missing_categories_and_uses_limit(
    algorithm: str,
) -> None:
    storage = FakeTrainingClient([
        embedded_post(html="<p>Post body</p>", vector=(1, i))._replace(
            post_id=str(i), url=f"https://example.com/{i}",
            html_embedding=(1, i), embedding_algorithm=algorithm,
        ) for i in range(3)
    ])
    client = cast(ClickHouseClient, storage)
    scorer = MagicMock()

    def scores(
        _post: PostTrainingData,
        cats: tuple[Category, ...],
    ) -> dict[str, float]:
        return {c.name: 0.9 for c in cats}

    scorer.score = AsyncMock(side_effect=scores)
    config = CategorySettings(definitions=DEFINITIONS, concurrency=2)
    kwargs: dict[str, Any] = {
        "since": datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC),
        "limit": 1,
    }
    assert (
        await _train_posts(
            client,
            scorer,
            config,
            ["example.com"],
            **kwargs,
        )
        == 1
    )
    assert (
        await _train_posts(
            client,
            scorer,
            config,
            ["example.com"],
            **kwargs,
        )
        == 1
    )
    assert scorer.score.call_count == 2
    assert scorer.score.call_args.args[0] == {
        "url": "https://example.com/1",
        "author": "Author",
        "title": "Title",
        "html": "<p>Post body</p>",
    }
    more = config.model_copy(
        update={
            "definitions": (*DEFINITIONS, Category("new", "New category")),
        },
    )
    assert (
        await _train_posts(
            client,
            scorer,
            more,
            ["example.com"],
            **kwargs,
        )
        == 1
    )
    assert scorer.score.call_args.args[1] == (Category("new", "New category"),)
    assert len(storage.examples) == 5
    assert all(
        row["embedding_algorithm"] == algorithm for row in storage.examples
    )


async def test_successful_sibling_is_saved_before_training_failure() -> None:
    storage = FakeTrainingClient([
        embedded_post(vector=(1, i))._replace(
            post_id=str(i), url=f"https://example.com/{i}",
        ) for i in range(2)
    ])
    client = cast(ClickHouseClient, storage)
    scorer = MagicMock()

    async def scores(
        post: PostTrainingData,
        _cats: tuple[Category, ...],
    ) -> dict[str, float]:
        if post["url"] == "https://example.com/0":
            raise TrainingError("Scoring failed")

        await asyncio.sleep(0)

        return {"science": 0.9, "software": 0.1}

    scorer.score = AsyncMock(side_effect=scores)
    config = CategorySettings(definitions=DEFINITIONS, concurrency=2)

    with pytest.raises(TrainingError, match="Scoring failed"):
        await _train_posts(
            client,
            scorer,
            config,
            ["example.com"],
            since=datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC),
            limit=2,
        )

    assert sorted(row["category"] for row in storage.examples) == [
        "science", "software",
    ]
    assert all(row["embedding"] == [1.0, 1.0] for row in storage.examples)


@pytest.mark.parametrize("confidence", [0.0, 0.000001, 1.0])
async def test_zero_confidence_is_not_inserted(confidence: float) -> None:
    storage = FakeTrainingClient([embedded_post()])
    client = MagicMock()
    client.fetch_json = AsyncMock(side_effect=storage.fetch_json)
    client.insert_rows = AsyncMock(side_effect=storage.insert_rows)
    scorer = MagicMock()
    scorer.score = AsyncMock(return_value={
        "science": confidence, "software": 0.0,
    })
    config = CategorySettings(definitions=DEFINITIONS)
    trained = await _train_posts(
        cast(ClickHouseClient, client), scorer, config, ["example.com"],
        since=datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC), limit=1,
    )
    # An evaluated post counts toward the request limit even with no matches.
    assert trained == 1
    scorer.score.assert_awaited_once()

    if confidence > 0:
        assert storage.examples == [{
            "embedding": [1.0, 0.0], "embedding_algorithm": "model",
            "category": "science", "confidence": confidence,
        }]
        client.insert_rows.assert_awaited_once()
    else:
        assert storage.examples == []
        client.insert_rows.assert_not_awaited()


async def test_training_continues_after_full_and_partial_refusals() -> None:
    storage = FakeTrainingClient([
        embedded_post(vector=(1, i))._replace(
            post_id=str(i), html="<p>Body</p>", html_embedding=(1, i),
        ) for i in range(2)
    ])
    scorer = MagicMock()
    scorer.score = AsyncMock(side_effect=[{}, {"science": 0.9}])
    trained = await _train_posts(
        cast(ClickHouseClient, storage), scorer,
        CategorySettings(definitions=DEFINITIONS, concurrency=1),
        ["example.com"],
        since=datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC), limit=2,
    )
    assert trained == 2
    assert scorer.score.call_count == 2
    assert len(storage.examples) == 1
    assert storage.examples[0]["category"] == "science"
    assert storage.examples[0]["confidence"] == 0.9


async def test_invalid_answer_does_not_stop_later_posts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    storage = FakeTrainingClient([
        embedded_post(vector=(1, i))._replace(post_id=str(i))
        for i in range(2)
    ])
    scorer = MagicMock()
    scorer.score = AsyncMock(side_effect=[
        AnswerError("OpenAI returned an unexpected answer type"),
        {"science": 0.9},
    ])
    trained = await _train_posts(
        cast(ClickHouseClient, storage), scorer,
        CategorySettings(definitions=DEFINITIONS, concurrency=1),
        ["example.com"],
        since=datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC), limit=2,
    )
    assert trained == 2
    assert scorer.score.call_count == 2
    assert len(storage.examples) == 1
    assert storage.examples[0]["embedding"] == [1.0, 1.0]
    assert "Skipping category training post" in caplog.text
