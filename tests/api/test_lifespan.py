"""The FastAPI lifespan against a real, throwaway ClickHouse database."""

import pytest

from dank.api.app import create_app
from dank.api.lifespan import Resources
from dank.config import ClickHouseSettings, Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_query import POST_COLUMNS

pytestmark = pytest.mark.clickhouse

# Created by the startup steps; schema.sql doesn't have them.
STARTUP_TABLES = {
    "post_urls",
    "post_links",
    "link_index_versions",
    "post_reference_evidence",
    "reference_index_versions",
}


async def tables(client: ClickHouseClient) -> set[str]:
    result = await client.fetch_json("SHOW TABLES")

    return {str(row["name"]) for row in result.rows}


async def test_startup_prepares_storage_and_shutdown_stops_refresh(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    app = create_app(
        settings._replace(clickhouse=clickhouse_settings), page_size=50,
    )

    async with ClickHouseClient(clickhouse_settings) as client:
        assert not STARTUP_TABLES & await tables(client)

        async with app.router.lifespan_context(app) as state:
            assert state is not None
            resources: Resources = state["resources"]

            assert STARTUP_TABLES <= await tables(client)
            assert resources.assets_dir.is_dir()
            assert resources.source_icons.directory.parent == (
                resources.assets_dir
            )
            assert resources.refresh.scheduler is not None
            assert not resources.refresh.scheduler.done()

    assert resources.refresh.scheduler.done()


async def test_startup_upgrades_a_database_from_before_categories(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    app = create_app(
        settings._replace(clickhouse=clickhouse_settings), page_size=50,
    )

    async with ClickHouseClient(clickhouse_settings) as client:
        # schema.sql only reaches a new database, so undo what it added.
        for statement in (
            "ALTER TABLE posts DROP COLUMN categories",
            "ALTER TABLE posts DROP COLUMN embedding_algorithm",
            "DROP TABLE category_training",
        ):
            await client.execute(statement)

        async with app.router.lifespan_context(app):
            await client.fetch_json(f"SELECT {POST_COLUMNS} FROM posts")

            assert "category_training" in await tables(client)


async def test_each_running_app_owns_its_collection_gates(
    settings: Settings, clickhouse_settings: ClickHouseSettings,
) -> None:
    configured = settings._replace(clickhouse=clickhouse_settings)
    first = create_app(configured, page_size=50)
    second = create_app(configured, page_size=50)

    async with (
        first.router.lifespan_context(first) as one,
        second.router.lifespan_context(second) as other,
    ):
        assert one is not None
        assert other is not None
        ours: Resources = one["resources"]
        theirs: Resources = other["resources"]

        assert ours.metadata_gate is not ours.transcript_gate
        assert ours.metadata_gate.lock is not theirs.metadata_gate.lock
        assert ours.metadata_gate.limiter is not theirs.metadata_gate.limiter
        assert ours.transcript_gate.lock is not theirs.transcript_gate.lock
        assert (
            ours.transcript_gate.limiter is not theirs.transcript_gate.limiter
        )
