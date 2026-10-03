import datetime
import logging
import pathlib
from typing import Any, cast

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from dank.model import AssetDiscovery
from dank.scrape.assets.http import download_file_http
from dank.scrape.rss import _fetch_text  # pyright: ignore[reportPrivateUsage]


class _Response:
    def __init__(self, url: str, status: int) -> None:
        self.url = url
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def text(self) -> str:
        return ""

    def raise_for_status(self) -> None:
        if self.status >= 400:
            headers = CIMultiDictProxy(CIMultiDict({"Retry-After": "30"}))
            request = aiohttp.RequestInfo(
                URL(self.url), "GET", headers, URL(self.url),
            )
            raise aiohttp.ClientResponseError(
                request, (), status=self.status,
                message="source refused request", headers=headers,
            )


class _Client:
    def __init__(self, status: int) -> None:
        self.status = status

    def get(self, url: str, **kwargs: object) -> _Response:
        return _Response(url, self.status)


@pytest.mark.parametrize("status", [403, 429])
async def test_http_failure_logs_status_url_and_retry_after_at_warning(
    caplog: pytest.LogCaptureFixture,
    status: int,
) -> None:
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
    assert not (tmp_path / "image.jpg.part").exists()
