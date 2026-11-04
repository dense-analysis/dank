import asyncio
import glob
import pathlib
from typing import Any, cast

import pytest

from dank.config import MediaSizeLimits, SourceConfig
from dank.model import AssetDiscovery, RawAsset
from dank.scrape.assets import asset_size_limit, download_assets
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


VIDEOS = MediaSizeLimits(
    kinds={"video": 8},
    sites={"youtube.com": {"video": 10}, "x.com": {"image": 3}},
)


def found(domain: str, asset_type: str, source: str = "rss") -> AssetDiscovery:
    return AssetDiscovery(
        source, domain, "post", f"https://{domain}/file", asset_type, None,
    )


@pytest.mark.parametrize(("domain", "asset_type", "expected"), [
    # X saves its photos as "photo" and its GIFs as short videos.
    ("x.com", "photo", 3),
    ("x.com", "image", 3),
    ("x.com", "animated_gif", 8),
    ("x.com", "video", 8),
    ("reddit.com", "photo", 6),
    ("reddit.com", "video", 8),
    # A YouTube video is YouTube's, whichever post embedded it.
    ("youtube.com", "youtube", 10),
    ("blog.codinghorror.com", "youtube", 10),
    ("www.hillelwayne.com", "youtube", 10),
    # A video a blog hosts itself is only that blog's.
    ("blog.codinghorror.com", "video", 8),
    # Types outside image, audio and video take the default.
    ("x.com", "link", 6),
    ("reddit.com", "iframe", 6),
])
def test_each_file_finds_its_limit(
    domain: str, asset_type: str, expected: int,
) -> None:
    limit = asset_size_limit(found(domain, asset_type), 6, VIDEOS)

    assert limit == expected


async def test_a_site_limit_lets_a_file_past_a_lower_default(
    tmp_path: pathlib.Path,
) -> None:
    response = Response(6)

    class Client:
        def get(self, *args: object, **kwargs: object):
            return response

    discovery = found("reddit.com", "image")
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=4, owners={discovery: stats},
        size_limits=MediaSizeLimits(sites={"reddit.com": {"image": 10}}),
    )

    assert result[0].local_path
    assert stats.counts["files_downloaded"] == 1
    assert stats.counts["media_skipped"] == 0


async def test_a_site_limit_skips_a_file_the_default_allows(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO", logger="dank.scrape.assets")
    response = Response(6)

    class Client:
        def get(self, *args: object, **kwargs: object):
            return response

    discovery = found("reddit.com", "image")
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=100, owners={discovery: stats},
        size_limits=MediaSizeLimits(sites={"reddit.com": {"image": 4}}),
    )

    assert result[0].local_path == ""
    assert stats.counts["media_skipped"] == 1
    assert stats.counts["files_failed"] == 0
    # The log names the limit that applied, not only the configured default.
    assert "4-byte limit" in caplog.text


async def test_another_sites_limit_changes_nothing_here(
    tmp_path: pathlib.Path,
) -> None:
    response = Response(6)

    class Client:
        def get(self, *args: object, **kwargs: object):
            return response

    discovery = found("reddit.com", "image")
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=100,
        size_limits=MediaSizeLimits(sites={"x.com": {"image": 4}}),
    )

    assert result[0].local_path


async def test_an_estimate_over_a_site_limit_never_starts_the_download(
    tmp_path: pathlib.Path,
) -> None:
    class Client:
        def get(self, *args: object, **kwargs: object):
            raise AssertionError("The estimate alone should have skipped this")

    discovery = found("reddit.com", "image")._replace(estimated_size_bytes=10)
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    result = await download_assets(
        [discovery], assets_dir=tmp_path, http_client=cast(Any, Client()),
        max_asset_bytes=100, owners={discovery: stats},
        size_limits=MediaSizeLimits(sites={"reddit.com": {"image": 4}}),
    )

    assert result[0].local_path == ""
    assert stats.counts["media_skipped"] == 1


async def test_an_embedded_youtube_video_downloads_under_youtubes_limit(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    limits: list[int | None] = []

    async def download(
        *, discovery: AssetDiscovery, max_asset_bytes: int | None,
        timestamp: Any, **kwargs: Any,
    ) -> RawAsset:
        limits.append(max_asset_bytes)

        return RawAsset(
            discovery.domain, discovery.post_id, discovery.url,
            discovery.asset_type, timestamp, discovery.source, "",
        )

    monkeypatch.setattr(
        "dank.scrape.assets.download_audio_video_asset", download,
    )
    await download_assets(
        [found("blog.codinghorror.com", "youtube")],
        assets_dir=tmp_path, http_client=cast(Any, None),
        max_asset_bytes=6, size_limits=VIDEOS,
    )

    assert limits == [10]
