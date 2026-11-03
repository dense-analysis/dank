import datetime as dt
from types import SimpleNamespace
from typing import Any, NamedTuple, cast

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


async def test_fetch_json_converts_embedding_lists_to_tuples() -> None:
    client = _DummyClickHouseClient(
        _RawResult(
            column_names=("post_id", "title_embedding", "misc_values"),
            result_rows=[("1", [0.1, 0.2], [1, 2])],
        ),
    )

    result = await client.fetch_json("SELECT 1")

    assert result.rows == [
        {
            "post_id": "1",
            "title_embedding": (0.1, 0.2),
            "misc_values": [1, 2],
        },
    ]


class _RecordingDriver:
    """Stands in for the driver's client and keeps what it was asked to run."""

    def __init__(self) -> None:
        self.client = SimpleNamespace(server_tz=dt.UTC)
        self.sent: list[tuple[str, Any]] = []

    async def query(self, query: str, parameters: Any = None) -> _RawResult:
        self.sent.append((query, parameters))

        return _RawResult(column_names=(), result_rows=[])

    async def command(self, cmd: str, parameters: Any = None) -> None:
        self.sent.append((cmd, parameters))


def _recording_client() -> tuple[ClickHouseClient, _RecordingDriver]:
    client = _DummyClickHouseClient(_RawResult((), []))
    driver = _RecordingDriver()
    client._client = cast(Any, driver)  # pyright: ignore[reportPrivateUsage]

    return client, driver


# The driver reads {.+:.+} as a server-side bind, so it would skip %(name)s.
REPEATED_REGEX = "'^https?://{1,15}' = '^https?://{1,15}' AND a = %(a)s"


async def test_queries_bind_when_sql_looks_like_a_server_side_bind() -> None:
    client, driver = _recording_client()

    await ClickHouseClient._query(  # pyright: ignore[reportPrivateUsage]
        client, f"SELECT 1 WHERE {REPEATED_REGEX}", {"a": "it's"},
    )

    assert driver.sent == [(
        "SELECT 1 WHERE '^https?://{1,15}' = '^https?://{1,15}' "
        "AND a = 'it\\'s'",
        None,
    )]


async def test_commands_bind_when_sql_looks_like_a_server_side_bind() -> None:
    client, driver = _recording_client()

    await client.execute(f"ALTER TABLE t DELETE WHERE {REPEATED_REGEX}", {
        "a": dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC),
    })

    assert driver.sent == [(
        "ALTER TABLE t DELETE WHERE '^https?://{1,15}' = '^https?://{1,15}' "
        "AND a = '2026-10-30 12:00:00'",
        None,
    )]


async def test_queries_without_parameters_are_left_alone() -> None:
    client, driver = _recording_client()

    await ClickHouseClient._query(  # pyright: ignore[reportPrivateUsage]
        client, "SELECT 100 % 3", None,
    )

    assert driver.sent == [("SELECT 100 % 3", None)]
