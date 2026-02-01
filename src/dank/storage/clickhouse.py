from __future__ import annotations

import datetime
import json
from collections.abc import Iterable
from types import TracebackType
from typing import Any, NamedTuple

import aiohttp

from dank.config import ClickHouseSettings


class QueryResult(NamedTuple):
    rows: list[dict[str, Any]]


class ClickHouseClient:
    def __init__(
        self,
        settings: ClickHouseSettings,
        timeout_seconds: float = 30.0,
    ):
        if not settings.use_http:
            raise ValueError("Only HTTP ClickHouse connections are supported")

        self._settings = settings
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None
        self._base_url = (
            f"https://{settings.host}:{settings.port}"
            if settings.secure
            else f"http://{settings.host}:{settings.port}"
        )

    async def __aenter__(self) -> ClickHouseClient:
        auth = None

        if self._settings.username or self._settings.password:
            auth = aiohttp.BasicAuth(
                login=self._settings.username,
                password=self._settings.password,
            )

        self._session = aiohttp.ClientSession(timeout=self._timeout, auth=auth)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def execute(self, query: str) -> None:
        await self._request(query)

    async def fetch_json(self, query: str) -> QueryResult:
        text = await self._request(self._ensure_json_format(query))
        rows: list[dict[str, Any]] = []

        for line in text.splitlines():
            line = line.strip()

            if not line:
                continue

            rows.append(json.loads(line))

        return QueryResult(rows=rows)

    async def insert_json_rows(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        lines = [
            json.dumps(
                {
                    key: (
                        format_datetime(value)
                        if isinstance(value, datetime.datetime) else
                        value
                    )
                    for key, value in
                    row.items()
                },
                separators=(",", ":"),
            )
            for row in rows
        ]

        if not lines:
            return

        payload = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(
            lines,
        )
        await self._request(payload)

    async def _request(self, query: str) -> str:
        if self._session is None:
            raise RuntimeError("ClickHouse client is not initialized")

        params = {"database": self._settings.database}

        async with self._session.post(
            self._base_url,
            params=params,
            data=query,
        ) as response:
            response.raise_for_status()

            return await response.text()

    @staticmethod
    def _ensure_json_format(query: str) -> str:
        if "FORMAT" in query.upper():
            return query

        return f"{query.rstrip()}\nFORMAT JSONEachRow"


def format_datetime(value: datetime.datetime | None) -> str | None:
    if value is None:
        return None

    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)

    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_datetime(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, str) and value:
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return None

    return None
