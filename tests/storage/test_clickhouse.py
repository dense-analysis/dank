import asyncio
from typing import Any, NamedTuple

from dank.config import ClickHouseSettings
from dank.storage.clickhouse import ClickHouseClient


class _RawResult(NamedTuple):
    column_names: tuple[str, ...]
    result_rows: list[tuple[Any, ...]]


class _DummyClickHouseClient(ClickHouseClient):
    def __init__(self, result: _RawResult) -> None:
        super().__init__(
            ClickHouseSettings(
                host="localhost",
                port=8123,
                database="dank",
                username="default",
                password="",
                secure=False,
                use_http=True,
            ),
        )
        self._result = result

    async def _query(
        self,
        query: str,
        params: dict[str, Any] | None,
    ) -> Any:
        del query
        del params

        return self._result


def test_fetch_json_converts_embedding_lists_to_tuples() -> None:
    client = _DummyClickHouseClient(
        _RawResult(
            column_names=("post_id", "title_embedding", "misc_values"),
            result_rows=[("1", [0.1, 0.2], [1, 2])],
        ),
    )

    result = asyncio.run(client.fetch_json("SELECT 1"))

    assert result.rows == [
        {
            "post_id": "1",
            "title_embedding": (0.1, 0.2),
            "misc_values": [1, 2],
        },
    ]
