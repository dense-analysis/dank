import asyncio
import datetime
import logging
import os
import pathlib
from email.utils import format_datetime
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from dank.model import AssetDiscovery
from dank.scrape.assets.http import download_file_http
from dank.scrape.rss import _fetch_text  # pyright: ignore[reportPrivateUsage]


class _Response:
    def __init__(
        self, url: str, status: int, body: str, retry_after: str | None,
    ) -> None:
        self.url = url
        self.status = status
        self.body = body
        self.retry_after = retry_after
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def text(self) -> str:
        return self.body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            headers = CIMultiDictProxy(CIMultiDict(
                {"Retry-After": self.retry_after} if self.retry_after else {},
            ))
            request = aiohttp.RequestInfo(
                URL(self.url), "GET", headers, URL(self.url),
            )
            raise aiohttp.ClientResponseError(
                request, (), status=self.status,
                message="source refused request", headers=headers,
            )


class _Client:
    def __init__(
        self,
        status: int | list[int],
        *,
        retry_after: str | None = "30",
        body: str = "",
    ) -> None:
        self.statuses = [status] if isinstance(status, int) else status
        self.retry_after = retry_after
        self.body = body
        self.responses: list[_Response] = []
        self.headers: list[object] = []

    def get(self, url: str, **kwargs: object) -> _Response:
        index = min(len(self.responses), len(self.statuses) - 1)
        status = self.statuses[index]
        response = _Response(url, status, self.body, self.retry_after)
        self.responses.append(response)
        self.headers.append(kwargs.get("headers"))

        return response


@pytest.mark.parametrize("status", [403, 429])
async def test_http_failure_logs_status_url_and_retry_after_at_warning(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", AsyncMock())
    url = "https://example.test/feed/"
    result = await _fetch_text(
        cast(Any, _Client(status)), url, accept=["application/rss+xml"],
    )

    assert result is None
    warnings = [
        row.message for row in caplog.records
        if row.levelno >= logging.WARNING
    ]
    assert any(f"HTTP {status}" in row and url in row for row in warnings)
    assert any("Retry-After: 30" in row for row in warnings)


async def test_empty_fetch_is_visible_at_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = await _fetch_text(
        cast(Any, _Client(200)), "https://example.test/empty",
        accept=["text/html"],
    )

    assert result == ""
    assert "Empty response fetching https://example.test/empty" in caplog.text


async def test_timeout_is_visible_at_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _TimeoutClient:
        def get(self, *args: object, **kwargs: object) -> _Response:
            raise TimeoutError("request exceeded time limit")

    result = await _fetch_text(
        cast(Any, _TimeoutClient()), "https://example.test/slow",
        accept=["text/html"],
    )

    assert result is None
    assert "TimeoutError" in caplog.text
    assert "https://example.test/slow" in caplog.text


async def test_asset_http_error_is_visible_and_partial_file_is_removed(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    url = "https://example.test/image.jpg"
    result = await download_file_http(
        discovery=AssetDiscovery(
            "rss", "example.test", "1", url, "image", None,
        ),
        target_dir=tmp_path,
        http_client=cast(Any, _Client(403)),
        max_asset_bytes=100,
        timestamp=datetime.datetime.now(datetime.UTC),
    )

    assert result is not None and result.local_path == ""
    assert "403" in caplog.text and url in caplog.text
    assert await asyncio.to_thread(os.listdir, tmp_path) == []


async def test_rate_limit_recovers_with_exponential_waits(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    client = _Client([429, 429, 200], retry_after=None, body="<rss/>")
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        assert client.responses[-1].closed
        delays.append(delay)

    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    result = await _fetch_text(
        cast(Any, client), "https://example.test/feed/",
        accept=["application/rss+xml"],
    )

    assert result == "<rss/>"
    assert delays == [2, 4]
    assert len(client.responses) == 3
    assert client.headers == [{"Accept": "application/rss+xml"}] * 3
    assert "retry 1/3" in caplog.text and "retry 2/3" in caplog.text
    assert any(
        record.levelno == logging.INFO
        and "after 2 rate-limit retries" in record.message
        for record in caplog.records
    )


async def test_persistent_rate_limit_stops_after_three_retries(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    client = _Client(429, retry_after=None)
    result = await _fetch_text(
        cast(Any, client), "https://example.test/feed/", accept=["text/xml"],
    )

    assert result is None
    assert len(client.responses) == 4
    assert [call.args[0] for call in sleep.await_args_list] == [2, 4, 8]
    assert "Giving up" in caplog.text
    assert "after 3 HTTP 429 retries" in caplog.text


@pytest.mark.parametrize("header, delay", [
    ("12", 12), ("30", 30), ("0", 2), ("1", 2), (" 10 ", 10),
    ("invalid", 2), ("-1", 2), ("1.5", 2),
    ("Wed, 21 Oct 2015 07:28:00 GMT", 2),
    ("Wed, 21 Oct 2099 07:28:00", 2),
])
async def test_retry_after_controls_wait_without_disabling_backoff(
    monkeypatch: pytest.MonkeyPatch,
    header: str,
    delay: float,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    client = _Client([429, 200], retry_after=header, body="ok")
    result = await _fetch_text(
        cast(Any, client), "https://example.test/article",
        accept=["text/html"],
    )

    assert result == "ok"
    assert len(client.responses) == 2
    sleep.assert_awaited_once_with(delay)


async def test_retry_after_http_date_is_respected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    date = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=20)
    client = _Client(
        [429, 200], retry_after=format_datetime(date, usegmt=True), body="ok",
    )
    result = await _fetch_text(
        cast(Any, client), "https://example.test/article",
        accept=["text/html"],
    )

    assert result == "ok"
    sleep.assert_awaited_once()
    assert 18 <= sleep.await_args_list[0].args[0] <= 20


@pytest.mark.parametrize("header", [
    "31", "86400", "9" * 1000, "Thu, 01 Jan 2099 00:00:00 GMT",
])
async def test_long_retry_after_skips_without_retrying_early(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    header: str,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    client = _Client(429, retry_after=header)
    result = await _fetch_text(
        cast(Any, client), "https://example.test/feed/", accept=["text/xml"],
    )

    assert result is None
    assert len(client.responses) == 1
    sleep.assert_not_awaited()
    assert "exceeds the 30s retry wait limit" in caplog.text


@pytest.mark.parametrize("status", [200, 403, 404, 500, 503])
async def test_other_statuses_do_not_retry(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    client = _Client(status, body="ok")
    result = await _fetch_text(
        cast(Any, client), "https://example.test/article",
        accept=["text/html"],
    )

    assert result == ("ok" if status == 200 else None)
    assert len(client.responses) == 1
    sleep.assert_not_awaited()


async def test_cancellation_during_backoff_stops_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr("dank.scrape.rss.asyncio.sleep", sleep)
    client = _Client(429, retry_after=None)

    with pytest.raises(asyncio.CancelledError):
        await _fetch_text(
            cast(Any, client), "https://example.test/feed/",
            accept=["text/xml"],
        )

    assert len(client.responses) == 1
    assert client.responses[0].closed
