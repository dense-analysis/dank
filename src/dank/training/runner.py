from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import re
from collections.abc import Coroutine

from dank.age import parse_age_window
from dank.categories import category_embedding
from dank.category_config import CategorySettings
from dank.config import Settings
from dank.model import Post, PostTrainingData, parse_datetime
from dank.progress import Progress
from dank.storage.clickhouse import ClickHouseClient
from dank.training.decisions import AnswerError, CategoryScorer

logger = logging.getLogger(__name__)
# Bound title evidence consistently with the local embedding input.
MAX_TITLE_CHARS = 512


async def run_train(
    settings: Settings,
    *,
    age: str = "24h",
    limit: int = 10_000,
    domain_regex: re.Pattern[str] | None = None,
) -> int:
    config = settings.categories

    if not config.definitions or not config.api_key.get_secret_value().strip():
        raise ValueError(
            "Training requires categories.definitions and api_key",
        )

    if limit < 1:
        raise ValueError("Training limit must be positive")

    since = (
        datetime.datetime(1900, 1, 1, tzinfo=datetime.UTC)
        if age.strip().lower() == "all"
        else datetime.datetime.now(datetime.UTC) - parse_age_window(age)
    )
    domains = sorted(
        {
            s.domain
            for s in settings.sources
            if domain_regex is None or domain_regex.search(s.domain)
        },
    )

    if not domains:
        return 0

    async with ClickHouseClient(settings.clickhouse) as client:
        schema = pathlib.Path(__file__).parents[1] / "storage/categories.sql"

        for statement in schema.read_text().split(";"):
            if statement.strip():
                await client.execute(statement)

        async with (
            CategoryScorer(config) as scorer,
            Progress("Training category embeddings"),
        ):
            return await _train_posts(
                client,
                scorer,
                config,
                domains,
                since=since,
                limit=limit,
            )


async def _train_posts(
    client: ClickHouseClient,
    scorer: CategoryScorer,
    config: CategorySettings,
    domains: list[str],
    *,
    since: datetime.datetime,
    limit: int,
) -> int:
    trained = 0
    cursor = ("", "")

    while trained < limit:
        result = await client.fetch_json(
            "SELECT domain, post_id, url, created_at, updated_at, source, "
            "author, title, html, title_embedding, categories, "
            "html_embedding, embedding_algorithm FROM posts FINAL "
            "WHERE domain IN %(domains)s "
            "AND updated_at >= %(since)s "
            "AND (domain, post_id) > %(cursor)s "
            "ORDER BY domain, post_id LIMIT %(page_size)s",
            {
                "domains": domains,
                "since": since,
                "cursor": cursor,
                "page_size": 250,
            },
        )

        if not result.rows:
            break

        posts: list[Post] = []

        for row in result.rows:
            created_at = parse_datetime(row["created_at"])
            updated_at = parse_datetime(row["updated_at"])

            if created_at is None or updated_at is None:
                raise ValueError("Training post has invalid timestamps")

            posts.append(Post(
                domain=row["domain"], post_id=row["post_id"], url=row["url"],
                created_at=created_at, updated_at=updated_at,
                source=row["source"], author=row["author"], title=row["title"],
                html=row["html"],
                title_embedding=tuple(row["title_embedding"]),
                html_embedding=tuple(row["html_embedding"]),
                embedding_algorithm=row["embedding_algorithm"],
                categories=tuple(
                    (name, confidence, similarity)
                    for name, confidence, similarity in row["categories"]
                ),
            ))

        trained += await _train_page(
            client, scorer, config, posts, limit=limit - trained,
        )
        last = result.rows[-1]
        cursor = (str(last["domain"]), str(last["post_id"]))

    logger.info("Category training complete posts=%d", trained)

    return trained


async def _train_page(
    client: ClickHouseClient,
    scorer: CategoryScorer,
    config: CategorySettings,
    rows: list[Post],
    *,
    limit: int,
) -> int:
    trained = 0
    offset = 0

    while offset < len(rows) and trained < limit:
        size = min(config.concurrency, limit - trained)
        batch = rows[offset : offset + size]
        offset += size
        pending: list[Coroutine[object, object, bool]] = []
        seen: set[tuple[str, tuple[float, ...]]] = set()

        for row in batch:
            vector = category_embedding(row)
            key = (str(row.embedding_algorithm), vector)

            if not vector or key in seen:
                continue

            seen.add(key)
            pending.append(_train_example(client, scorer, config, row, vector))

        # Finish all writes before propagating a sibling's failure.
        outcomes = await asyncio.gather(*pending, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome

            trained += int(outcome)

    return trained


async def _train_example(
    client: ClickHouseClient,
    scorer: CategoryScorer,
    config: CategorySettings,
    row: Post,
    vector: tuple[float, ...],
) -> bool:
    algorithm = str(row.embedding_algorithm)
    existing = await client.fetch_json(
        "SELECT DISTINCT category FROM category_training "
        "WHERE embedding_algorithm = %(algorithm)s "
        "AND embedding = %(embedding)s",
        {"algorithm": algorithm, "embedding": list(vector)},
    )
    known = {str(item["category"]) for item in existing.rows}
    missing = tuple(c for c in config.definitions if c.name not in known)

    if not missing:
        return False

    payload: PostTrainingData = {
        "url": row.url,
        "author": row.author,
        "title": row.title[:MAX_TITLE_CHARS],
        "html": row.html[: config.max_content_chars],
    }
    try:
        scores = await scorer.score(payload, missing)
    except AnswerError as error:
        logger.warning(
            "Skipping category training post domain=%s post_id=%s: %s",
            row.domain, row.post_id, error,
        )

        # Failed evidence still consumes budget, bounding work per invocation.
        return True
    rows = [
        {
            "embedding": list(vector),
            "embedding_algorithm": algorithm,
            "category": name,
            "confidence": score,
        }
        for name, score in scores.items()
        if score > 0
    ]

    if rows:
        await client.insert_rows("category_training", rows)

    return True
