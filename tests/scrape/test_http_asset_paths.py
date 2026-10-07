from __future__ import annotations

import asyncio
import datetime
import glob
import os
import pathlib
from typing import Any, cast

import pytest

from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import download_file_http


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.content_length = len(body)
        self.content = self

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    def raise_for_status(self) -> None:
        pass

    async def iter_chunked(self, size: int):
        yield self.body[:1]
        await asyncio.sleep(0)
        yield self.body[1:]


class _Client:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, url: str, **kwargs: object) -> _Response:
        self.calls += 1

        return _Response(url.encode())


async def _download(
    directory: pathlib.Path, client: _Client, url: str,
):
    return await download_file_http(
        discovery=AssetDiscovery("rss", "example.test", "post", url,
                                 "image", None),
        target_dir=directory, http_client=cast(Any, client),
        max_asset_bytes=1024, timestamp=datetime.datetime.now(datetime.UTC),
    )


async def test_query_and_host_variants_have_distinct_files(
    tmp_path: pathlib.Path,
) -> None:
    urls = [
        "https://one.test/_image?id=first",
        "https://one.test/_image?id=second",
        "https://two.test/_image?id=first",
        "https://one.test/other/_image?id=first",
    ]
    client = _Client()
    results = await asyncio.gather(*(
        _download(tmp_path, client, url) for url in urls
    ))
    assert all(result is not None for result in results)
    paths = [pathlib.Path(result.local_path) for result in results if result]
    assert len(set(paths)) == len(urls)
    assert [path.read_bytes() for path in paths] == [
        url.encode() for url in urls
    ]
    await asyncio.gather(*(
        _download(tmp_path, client, url) for url in urls
    ))
    assert client.calls == 4
    assert not await asyncio.to_thread(glob.glob, str(tmp_path / "*.part"))


async def test_simultaneous_same_url_requests_use_separate_temporary_files(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    url = "https://example.test/picture.png"
    results = await asyncio.gather(
        _download(tmp_path, client, url), _download(tmp_path, client, url),
    )
    assert results[0] is not None and results[1] is not None
    assert results[0].local_path == results[1].local_path
    path = pathlib.Path(results[0].local_path)
    assert path.suffix == ".png"
    assert await asyncio.to_thread(path.read_bytes) == url.encode()
    assert not await asyncio.to_thread(glob.glob, str(tmp_path / "*.part"))


async def test_failed_publish_is_a_logged_download_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("cannot publish file")

    monkeypatch.setattr(pathlib.Path, "replace", fail)
    result = await _download(tmp_path, _Client(), "https://example.test/a.jpg")
    assert result is not None and result.local_path == ""
    assert "cannot publish file" in caplog.text
    assert await asyncio.to_thread(os.listdir, tmp_path) == []


async def test_cancelled_batch_stops_its_other_downloads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def download(
        discovery: AssetDiscovery, *args: Any,
    ) -> None:
        if discovery.url.endswith("fail"):
            await started.wait()
            raise RuntimeError("unexpected downloader failure")

        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr("dank.scrape.assets._download_one", download)
    assets = [
        AssetDiscovery("rss", "example.test", "post", url, "image", None)
        for url in ("https://example.test/fail", "https://example.test/wait")
    ]

    with pytest.raises(RuntimeError, match="unexpected downloader failure"):
        await download_assets(
            assets, assets_dir=tmp_path, http_client=cast(Any, None),
        )

    assert stopped.is_set()
