import asyncio
import glob
import pathlib
from typing import Any, cast

import pytest

from dank.config import SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import http_asset_path
from dank.scrape.metrics import SourceMetrics


class Response:
    def __init__(self, length: int | None) -> None:
        self.content_length = length
        self.content = self
        self.streamed = False
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        pass

    async def iter_chunked(self, size: int):
        self.streamed = True
        yield b"abc"
        yield b"def"


@pytest.mark.parametrize("declared_size", [10, None, 2])
async def test_size_exclusions_keep_reference_without_failing_source(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture,
    declared_size: int | None,
) -> None:
    caplog.set_level("INFO", logger="dank.scrape.assets")
    response = Response(declared_size)

    class Client:
        def get(self, *args: object, **kwargs: object):
            return response

    discovery = AssetDiscovery(
        "reddit", "reddit.com", "t3_one", "https://i.redd.it/large.gif",
        "image", None,
    )
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=4, owners={discovery: stats},
    )
    assert len(result) == 1
    assert result[0].url == discovery.url
    assert result[0].post_id == discovery.post_id
    assert result[0].local_path == ""
    assert stats.counts["media_skipped"] == 1
    assert stats.counts["files_failed"] == 0
    assert stats.counts["files_downloaded"] == 0
    assert stats.counts["downloaded_file_bytes"] == 0
    assert stats.active_media == 0
    assert response.closed
    assert response.streamed == (declared_size != 10)
    assert not await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.part"), recursive=True,
    )
    assert not http_asset_path(
        tmp_path / discovery.domain / discovery.post_id, discovery.url,
    ).exists()
    assert "Media skipped (size limit)" in caplog.text
    assert not any(
        record.levelname in {"WARNING", "ERROR"} for record in caplog.records
    )
    stats.counts["posts_saved"] = 1
    stats.collected()
    assert stats.status == "completed"


@pytest.mark.parametrize("limit", [6, 7, None, 0, -1])
async def test_allowed_download_keeps_bytes_and_counts_success(
    tmp_path: pathlib.Path, limit: int | None,
) -> None:
    response = Response(6)

    class Client:
        def get(self, *args: object, **kwargs: object):
            return response

    discovery = AssetDiscovery(
        "reddit", "reddit.com", "t3_one", "https://i.redd.it/allowed.gif",
        "image", None,
    )
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=limit, owners={discovery: stats},
    )
    body = await asyncio.to_thread(
        pathlib.Path(result[0].local_path).read_bytes,
    )
    assert body == b"abcdef"
    assert stats.counts["media_skipped"] == 0
    assert stats.counts["files_failed"] == 0
    assert stats.counts["files_downloaded"] == 1
    assert stats.counts["downloaded_file_bytes"] == 6
