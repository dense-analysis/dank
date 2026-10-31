"""The FastAPI lifespan against a real, throwaway ClickHouse database."""

import pytest

from dank.api.app import create_app
from dank.api.lifespan import Resources
from dank.config import ClickHouseSettings, Settings
from dank.storage.clickhouse import ClickHouseClient

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
            assert resources.refresh.worker is not None
            assert not resources.refresh.worker.done()

    assert resources.refresh.worker.done()
