import asyncio
import logging
import pathlib
from typing import Any, NamedTuple, cast

import pytest

from dank.config import MEDIA_DOWNLOAD_TYPES, SourceConfig
from dank.model import AssetDiscovery
from dank.scrape.assets import download_assets
from dank.scrape.metrics import SourceMetrics


class _ParsedOptions(NamedTuple):
    urls: list[str]
    ydl_opts: dict[str, object]


class _UnusedHttpClient:
    def get(self, *_args: Any, **_kwargs: Any):
        raise AssertionError("HTTP client should not be used for yt-dlp")


@pytest.mark.parametrize("asset_source,asset_type,url", [
    ("rss", "youtube", "https://www.youtube.com/watch?v=abc123"),
    ("reddit", "video", "https://v.redd.it/abc/HLSPlaylist.m3u8"),
])
@pytest.mark.parametrize("download_types", [MEDIA_DOWNLOAD_TYPES, ("video",)])
async def test_video_sites_use_yt_dlp_and_preserve_url_identity(
    monkeypatch: Any,
    tmp_path: Any,
    download_types: tuple[str, ...],
    asset_source: str, asset_type: str, url: str,
) -> None:
    assets_dir = tmp_path / "assets"
    browser_profile_dir = tmp_path / "browser-profile"
    browser_profile_dir.mkdir(parents=True, exist_ok=True)
    created_path = assets_dir / "example.com" / "post-1" / "video-id.mp4"
    parsed_arguments: list[str] = []

    def _fake_parse_options(arguments: list[str]) -> _ParsedOptions:
        parsed_arguments.extend(arguments)

        return _ParsedOptions(urls=[arguments[-1]], ydl_opts={
            "outtmpl": arguments[arguments.index("--output") + 1].replace(
                "%(ext)s", "mp4",
            ),
        })

    class _DummyYoutubeDL:
        def __init__(self, _options: dict[str, object]) -> None:
            self.options = _options

        def __enter__(self) -> Any:
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc: object,
            _tb: object,
        ) -> None:
            return None

        def extract_info(
            self,
            *_args: Any,
            **_kwargs: Any,
        ) -> dict[str, object]:
            nonlocal created_path
            created_path = pathlib.Path(str(self.options["outtmpl"]))
            created_path.parent.mkdir(parents=True, exist_ok=True)
            created_path.write_bytes(b"video")

            return {"filepath": str(created_path)}

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.parse_options",
        _fake_parse_options,
    )
    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.YoutubeDL",
        _DummyYoutubeDL,
    )

    discovery = AssetDiscovery(
        source=asset_source,
        domain="example.com",
        post_id="post-1",
        url=url,
        asset_type=asset_type,
        estimated_size_bytes=None,
    )
    assets = await download_assets(
        [discovery],
        assets_dir=assets_dir,
        browser_profile_dir=browser_profile_dir,
        http_client=cast(Any, _UnusedHttpClient()),
        max_asset_bytes=1_024,
        download_types=download_types,
    )

    assert assets
    assert assets[0].url == url
    assert assets[0].asset_type == asset_type
    assert "bestvideo+bestaudio/best" in parsed_arguments
    assert await asyncio.to_thread(
        pathlib.Path(assets[0].local_path).read_bytes,
    ) == b"video"
    assert pathlib.Path(assets[0].local_path).is_relative_to(assets_dir)
    assert not created_path.exists()
    assert "--add-metadata" in parsed_arguments
    assert ("--write-thumbnail" in parsed_arguments) == (
        "image" in download_types
    )
    assert "--write-info-json" in parsed_arguments
    assert "--max-filesize" in parsed_arguments
    assert "--cookies-from-browser" in parsed_arguments
    assert (
        f"chromium:{browser_profile_dir.resolve()}"
        in parsed_arguments
    )


async def test_download_assets_deletes_oversized_youtube_file(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    assets_dir = tmp_path / "assets"
    created_path = assets_dir / "example.com" / "post-1" / "video-id.mp4"

    def _fake_parse_options(arguments: list[str]) -> _ParsedOptions:
        return _ParsedOptions(urls=[arguments[-1]], ydl_opts={
            "outtmpl": arguments[arguments.index("--output") + 1].replace(
                "%(ext)s", "mp4",
            ),
        })

    class _DummyYoutubeDL:
        def __init__(self, _options: dict[str, object]) -> None:
            self.options = _options

        def __enter__(self) -> Any:
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc: object,
            _tb: object,
        ) -> None:
            return None

        def extract_info(
            self,
            *_args: Any,
            **_kwargs: Any,
        ) -> dict[str, object]:
            nonlocal created_path
            created_path = pathlib.Path(str(self.options["outtmpl"]))
            created_path.parent.mkdir(parents=True, exist_ok=True)
            created_path.write_bytes(b"0123456789")

            return {"filepath": str(created_path)}

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.parse_options",
        _fake_parse_options,
    )
    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.YoutubeDL",
        _DummyYoutubeDL,
    )

    discovery = AssetDiscovery(
        source="rss",
        domain="example.com",
        post_id="post-1",
        url="https://www.youtube.com/watch?v=abc123",
        asset_type="youtube",
        estimated_size_bytes=None,
    )
    stats = SourceMetrics(1, SourceConfig("example.com", ()))
    assets = await download_assets(
        [discovery],
        assets_dir=assets_dir,
        http_client=cast(Any, _UnusedHttpClient()),
        max_asset_bytes=4,
        owners={discovery: stats},
    )

    assert assets
    assert assets[0].local_path == ""
    assert stats.counts["media_skipped"] == 1
    assert stats.counts["files_failed"] == 0
    assert not created_path.exists()


async def test_download_assets_skips_oversized_estimated_asset(
    tmp_path: Any,
) -> None:
    assets_dir = tmp_path / "assets"
    discovery = AssetDiscovery(
        source="x",
        domain="x.com",
        post_id="post-1",
        url="https://video.twimg.com/video.mp4",
        asset_type="video",
        estimated_size_bytes=10,
    )
    stats = SourceMetrics(1, SourceConfig("example.com", ()))
    assets = await download_assets(
        [discovery],
        assets_dir=assets_dir,
        http_client=cast(Any, _UnusedHttpClient()),
        max_asset_bytes=4,
        owners={discovery: stats},
    )

    assert assets
    assert assets[0].local_path == ""
    assert stats.counts["media_skipped"] == 1
    assert stats.counts["files_failed"] == 0


async def test_download_assets_skips_loader_gif_by_filename(
    tmp_path: Any,
) -> None:
    assets_dir = tmp_path / "assets"
    discovery = AssetDiscovery(
        source="rss",
        domain="example.com",
        post_id="post-1",
        url="https://example.com/images/loader.gif",
        asset_type="image",
        estimated_size_bytes=None,
    )
    assets = await download_assets(
        [discovery],
        assets_dir=assets_dir,
        http_client=cast(Any, _UnusedHttpClient()),
    )

    assert assets
    assert assets[0].local_path == ""


async def test_download_assets_passes_js_runtimes_to_yt_dlp(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    assets_dir = tmp_path / "assets"
    created_path = assets_dir / "example.com" / "post-1" / "video-id.mp4"
    parsed_arguments: list[str] = []

    def _fake_parse_options(arguments: list[str]) -> _ParsedOptions:
        parsed_arguments.extend(arguments)

        return _ParsedOptions(urls=[arguments[-1]], ydl_opts={
            "outtmpl": arguments[arguments.index("--output") + 1].replace(
                "%(ext)s", "mp4",
            ),
        })

    class _DummyYoutubeDL:
        def __init__(self, _options: dict[str, object]) -> None:
            self.options = _options

        def __enter__(self) -> Any:
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc: object,
            _tb: object,
        ) -> None:
            return None

        def extract_info(
            self,
            *_args: Any,
            **_kwargs: Any,
        ) -> dict[str, object]:
            nonlocal created_path
            created_path = pathlib.Path(str(self.options["outtmpl"]))
            created_path.parent.mkdir(parents=True, exist_ok=True)
            created_path.write_bytes(b"video")

            return {"filepath": str(created_path)}

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.parse_options",
        _fake_parse_options,
    )
    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.YoutubeDL",
        _DummyYoutubeDL,
    )

    def _which(runtime: str) -> str | None:
        return {
            "deno": "/usr/bin/deno",
            "node": "/usr/local/bin/node",
        }.get(runtime)

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.shutil.which",
        _which,
    )

    discovery = AssetDiscovery(
        source="rss",
        domain="example.com",
        post_id="post-1",
        url="https://www.youtube.com/watch?v=abc123",
        asset_type="youtube",
        estimated_size_bytes=None,
    )
    assets = await download_assets(
        [discovery],
        assets_dir=assets_dir,
        http_client=cast(Any, _UnusedHttpClient()),
    )

    assert assets
    assert "--js-runtimes" in parsed_arguments

    js_runtime_index = parsed_arguments.index("--js-runtimes")
    assert parsed_arguments[js_runtime_index + 1] == (
        "deno:/usr/bin/deno,node:/usr/local/bin/node"
    )


async def test_download_assets_logs_yt_dlp_messages_with_prefix(
    monkeypatch: Any,
    tmp_path: Any,
    caplog: Any,
) -> None:
    assets_dir = tmp_path / "assets"
    created_path = assets_dir / "example.com" / "post-1" / "video-id.mp4"

    def _fake_parse_options(arguments: list[str]) -> _ParsedOptions:
        return _ParsedOptions(urls=[arguments[-1]], ydl_opts={
            "outtmpl": arguments[arguments.index("--output") + 1].replace(
                "%(ext)s", "mp4",
            ),
        })

    class _DummyYoutubeDL:
        def __init__(self, options: dict[str, object]) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc: object,
            _tb: object,
        ) -> None:
            return None

        def extract_info(
            self,
            *_args: Any,
            **_kwargs: Any,
        ) -> dict[str, object]:
            logger = cast(Any, self.options["logger"])
            logger.debug("[info] Downloading")
            logger.debug("[debug] extractor details")
            logger.warning("WARNING: runtime warning")
            logger.error("ERROR: extract failed")
            nonlocal created_path
            created_path = pathlib.Path(str(self.options["outtmpl"]))
            created_path.parent.mkdir(parents=True, exist_ok=True)
            created_path.write_bytes(b"video")

            return {"filepath": str(created_path)}

    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.parse_options",
        _fake_parse_options,
    )
    monkeypatch.setattr(
        "dank.scrape.assets.audio_video.YoutubeDL",
        _DummyYoutubeDL,
    )

    discovery = AssetDiscovery(
        source="rss",
        domain="example.com",
        post_id="post-1",
        url="https://www.youtube.com/watch?v=abc123",
        asset_type="youtube",
        estimated_size_bytes=None,
    )

    with caplog.at_level(
        logging.DEBUG,
        logger="dank.scrape.assets.audio_video",
    ):
        assets = await download_assets(
            [discovery],
            assets_dir=assets_dir,
            http_client=cast(Any, _UnusedHttpClient()),
        )

    assert assets
    assert any(
        record.levelno == logging.DEBUG
        and record.getMessage() == "[yt-dlp] [info] Downloading"
        for record in caplog.records
    )
    assert any(
        record.levelno == logging.DEBUG
        and record.getMessage() == "[yt-dlp] [debug] extractor details"
        for record in caplog.records
    )
    assert any(
        record.levelno == logging.WARNING
        and record.getMessage() == "[yt-dlp] WARNING: runtime warning"
        for record in caplog.records
    )
    assert any(
        record.levelno == logging.ERROR
        and record.getMessage() == "[yt-dlp] ERROR: extract failed"
        for record in caplog.records
    )
