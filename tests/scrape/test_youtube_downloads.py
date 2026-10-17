import asyncio
import glob
import pathlib
from typing import Any, cast

import pytest

from dank.config import SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.assets.audio_video import (
    _download_with_yt_dlp,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.assets.limits import AssetSizeExceeded
from dank.scrape.metrics import SourceMetrics


def output(arguments: list[str]) -> pathlib.Path:
    return pathlib.Path(
        arguments[arguments.index("--output") + 1].replace("%(ext)s", "mp4"),
    )


async def collect(tmp_path: pathlib.Path, *, limit: int = 100):
    discovery = AssetDiscovery(
        "rss", "example.test", "post", "https://youtu.be/id", "youtube", None,
    )
    stats = SourceMetrics(1, SourceConfig("example.test", ()))
    result = await download_assets(
        [discovery],
        assets_dir=tmp_path,
        browser_profile_dir=tmp_path,
        http_client=cast(Any, None),
        max_asset_bytes=limit,
        owners={discovery: stats},
    )
    return result, stats


async def test_download_is_cached_by_url_and_only_completed_file_is_published(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[list[str]] = []

    def download(arguments: list[str]):
        calls.append(arguments)
        path = output(arguments)
        path.write_bytes(b"complete")
        path.with_suffix(".info.json").write_text("{}")
        return {"requested_downloads": [{"filepath": str(path)}]}

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video._download_with_yt_dlp", download,
    )
    result, stats = await collect(tmp_path)
    cached, next_stats = await collect(tmp_path)
    assert len(calls) == 1
    assert result[0].local_path == cached[0].local_path
    assert stats.counts["files_downloaded"] == 1
    assert next_stats.counts["files_cached"] == 1
    assert (
        await asyncio.to_thread(
            pathlib.Path(result[0].local_path).read_bytes,
        )
        == b"complete"
    )
    assert not await asyncio.to_thread(
        glob.glob,
        str(tmp_path / "**/.download-*"),
        recursive=True,
    )


@pytest.mark.parametrize("kind", ["size", "failed", "incomplete", "oversized"])
async def test_failed_or_excluded_download_cleans_only_its_own_files(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
):
    unrelated = tmp_path / "example.test/post/other.mp4"
    unrelated.parent.mkdir(parents=True)
    await asyncio.to_thread(unrelated.write_bytes, b"keep")
    calls = 0

    def download(arguments: list[str]):
        nonlocal calls
        calls += 1
        path = output(arguments)
        path.with_suffix(".mp4.part").write_bytes(b"partial")

        if kind == "size":
            raise AssetSizeExceeded("header rejected")
        if kind == "oversized":
            path.write_bytes(b"x" * 101)
            return {"filepath": str(path)}
        if kind == "incomplete":
            path.with_name("media.f137.mp4").write_bytes(b"silent video")
            return {"filepath": str(unrelated)}

        return None

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video._download_with_yt_dlp", download,
    )
    result, stats = await collect(tmp_path)
    assert calls == 1  # No second extraction with cookies removed.
    assert result[0].local_path == ""
    assert stats.counts["media_skipped"] == int(kind in {"size", "oversized"})
    assert stats.counts["files_failed"] == int(
        kind in {"failed", "incomplete"},
    )
    assert await asyncio.to_thread(unrelated.read_bytes) == b"keep"
    assert not await asyncio.to_thread(
        glob.glob,
        str(tmp_path / "**/.download-*"),
        recursive=True,
    )
    assert not await asyncio.to_thread(
        glob.glob,
        str(tmp_path / "**/*.part"),
        recursive=True,
    )
    assert not await asyncio.to_thread(
        glob.glob,
        str(tmp_path / "**/media.mp4"),
        recursive=True,
    )


def test_ytdlp_header_size_abort_is_reported_as_a_skip(
    monkeypatch: pytest.MonkeyPatch,
):
    class Downloader:
        def __init__(self, options: dict[str, Any]):
            self.options = options

        def __enter__(self):
            return self

        def __exit__(self, *args: object):
            pass

        def extract_info(
            self, *args: object, **kwargs: object,
        ) -> dict[str, object]:
            self.options["logger"].debug(
                "[download] File is larger than max-filesize "
                "(101 bytes > 100 bytes). Aborting.",
            )
            return {}

    monkeypatch.setattr("dank.scrape.assets.audio_video.YoutubeDL", Downloader)
    with pytest.raises(AssetSizeExceeded):
        _download_with_yt_dlp(["--max-filesize", "100", "https://youtu.be/id"])


def test_real_extractor_boundary_does_not_swallow_size_exclusions(
    monkeypatch: pytest.MonkeyPatch,
):
    from yt_dlp import YoutubeDL as RealYoutubeDL
    from yt_dlp.extractor.common import InfoExtractor

    class FixtureIE(InfoExtractor):
        _VALID_URL = r"fixture:(?P<id>clip)"

        def _real_extract(self, url: str) -> Any:
            return {
                "id": "clip", "title": "Fixture",
                "formats": [{
                    "format_id": "combined", "url": "https://unused.test/v.mp4",
                    "ext": "mp4", "vcodec": "avc1", "acodec": "mp4a.40.2",
                    "filesize": 1000,
                }],
            }

    def downloader(options: dict[str, Any]):
        result = RealYoutubeDL(cast(Any, options), auto_init=False)
        result.add_info_extractor(FixtureIE())
        return result

    monkeypatch.setattr("dank.scrape.assets.audio_video.YoutubeDL", downloader)

    with pytest.raises(AssetSizeExceeded):
        _download_with_yt_dlp(["--max-filesize", "100", "fixture:clip"])
