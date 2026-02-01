from __future__ import annotations

import datetime
from collections.abc import Iterable
from types import TracebackType
from typing import Any, NamedTuple, cast

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from dank.config import ClickHouseSettings


class QueryResult(NamedTuple):
    rows: list[dict[str, Any]]


class ClickHouseClient:
    def __init__(
        self,
        settings: ClickHouseSettings,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not settings.use_http:
            raise ValueError("Only HTTP ClickHouse connections are supported")

        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> ClickHouseClient:
        if self._client is not None:
            return self

        self._client = await clickhouse_connect.get_async_client(  # type: ignore
            host=self._settings.host,
            port=self._settings.port,
            username=self._settings.username or "",
            password=self._settings.password or "",
            database=self._settings.database,
            secure=self._settings.secure,
            connect_timeout=self._timeout_seconds,
            send_receive_timeout=self._timeout_seconds,
        )

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._client is None:
            return

        await self._client.close()
        self._client = None

    async def execute(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        await self._command(query, params)

    async def fetch_json(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> QueryResult:
        result = await self._query(query, params)
        column_names = cast(tuple[str, ...], result.column_names)  # type: ignore

        rows = [
            dict(zip(column_names, row, strict=True))
            for row in result.result_rows
        ]

        return QueryResult(rows=rows)

    async def insert_rows(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        data = list(rows)

        if not data:
            return

        column_names = list(data[0].keys())
        values = [
            [row.get(column) for column in column_names]
            for row in data
        ]

        await self._ensure_client().insert(
            table,
            values,
            column_names=column_names,
        )

    async def _command(
        self,
        query: str,
        params: dict[str, Any] | None,
    ) -> None:
        await self._ensure_client().command(  # type: ignore
            query,
            parameters=params,
        )

    async def _query(
        self,
        query: str,
        params: dict[str, Any] | None,
    ):
        return await self._ensure_client().query(  # type: ignore
            query,
            parameters=params,
        )

    def _ensure_client(self) -> AsyncClient:
        if self._client is None:
            raise RuntimeError("ClickHouse client is not initialized")

        return self._client


def parse_datetime(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, str) and value:
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return None

    return None
