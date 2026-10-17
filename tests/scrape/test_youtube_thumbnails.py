from __future__ import annotations

import asyncio
import datetime as dt
import glob
import io
import pathlib
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from PIL import Image, ImageDraw

from dank.config import SourceConfig, load_settings
from dank.model import AssetDiscovery
from dank.scrape import runner
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import http_asset_path, youtube_thumbnail_urls
from dank.scrape.metrics import SourceMetrics
from dank.thumbnails import (
    ThumbnailSignature,
    similar_thumbnail_signatures,
    thumbnail_signature,
)

VIDEO = "abcdefghijk"
CAPTURED = f"https://i.ytimg.com/vi/{VIDEO}/hq720.jpg?sqp=expired"
NOW = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)


def picture(color: str = "red", *, width: int = 640) -> bytes:
    image = Image.new("RGB", (width, round(width * 9 / 16)), color)
    draw = ImageDraw.Draw(image)
    draw.rectangle(
        (width // 8, width // 8, width // 2, width // 3), fill="white",
    )
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=95)

    return output.getvalue()


class Response:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body, self.status = body, status
        self.content_length = len(body)
        self.content = self

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                cast(Any, SimpleNamespace(real_url=CAPTURED)), (),
                status=self.status, message="upstream failure",
            )

    async def iter_chunked(self, size: int):
        yield self.body


class Client:
    def __init__(self, responses: dict[str, Response]) -> None:
        self.responses = responses
        self.urls: list[str] = []

    def get(self, url: str, **kwargs: object) -> Response:
        self.urls.append(url)

        return self.responses[url]


def discovery(url: str = CAPTURED) -> AssetDiscovery:
    return AssetDiscovery("youtube", "youtube.com", f"video:{VIDEO}", url,
                          "image", None)


async def capture(directory: pathlib.Path, client: Client):
    return (await download_assets(
        [discovery()], assets_dir=directory, http_client=cast(Any, client),
        scraped_at=NOW, download_types=("image",),
    ))[0]


async def test_missing_thumbnail_recovers_same_video_and_records_provenance(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture,
) -> None:
    urls = youtube_thumbnail_urls(discovery())
    client = Client({url: Response(picture(), 404) for url in urls})
    recovered_url = f"https://i.ytimg.com/vi/{VIDEO}/hqdefault.jpg"
    client.responses[recovered_url] = Response(picture())
    owner = SourceMetrics(0, SourceConfig("youtube.com", ()))
    result = (await download_assets(
        [discovery()], assets_dir=tmp_path, http_client=cast(Any, client),
        scraped_at=NOW, download_types=("image",),
        owners={discovery(): owner},
    ))[0]

    assert owner.counts["files_failed"] == 0
    assert owner.counts["files_downloaded"] == 1
    assert client.urls == list(urls[:4])
    assert result.url == CAPTURED
    assert await asyncio.to_thread(
        pathlib.Path(result.local_path).read_bytes,
    ) == picture()
    assert result.thumbnail is not None
    assert result.thumbnail.captured_url == CAPTURED
    assert result.thumbnail.fetched_url == recovered_url
    assert result.thumbnail.observed_at == NOW
    assert "Asset not downloaded" not in caplog.text
    assert len(await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.jpg"), recursive=True,
    )) == 1
    assert not await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.part"), recursive=True,
    )


async def test_same_url_refetches_and_preserves_changed_bytes(
    tmp_path: pathlib.Path,
) -> None:
    client = Client({CAPTURED: Response(picture())})
    original = await capture(tmp_path, client)
    repeated = await capture(tmp_path, client)
    client.responses[CAPTURED] = Response(picture("blue"))
    changed = await capture(tmp_path, client)

    assert client.urls == [CAPTURED] * 3
    assert original.local_path == repeated.local_path
    assert original.local_path != changed.local_path
    assert await asyncio.to_thread(
        pathlib.Path(original.local_path).read_bytes,
    ) == picture()
    assert await asyncio.to_thread(
        pathlib.Path(changed.local_path).read_bytes,
    ) == picture("blue")
    assert len(await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.jpg"), recursive=True,
    )) == 2


async def test_failed_refresh_keeps_previous_snapshot(
    tmp_path: pathlib.Path,
) -> None:
    client = Client({CAPTURED: Response(picture())})
    original = await capture(tmp_path, client)
    client.responses[CAPTURED] = Response(b"forbidden", 403)
    failure = await capture(tmp_path, client)

    assert failure.local_path == ""
    assert failure.thumbnail is None
    assert client.urls == [CAPTURED, CAPTURED]
    assert await asyncio.to_thread(
        pathlib.Path(original.local_path).read_bytes,
    ) == picture()
    assert not await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.part"), recursive=True,
    )


async def test_html_success_does_not_poison_thumbnail_history(
    tmp_path: pathlib.Path,
) -> None:
    result = await capture(
        tmp_path, Client({CAPTURED: Response(b"<html>error")}),
    )

    assert result.local_path == ""
    assert result.thumbnail is None
    assert not await asyncio.to_thread(
        glob.glob, str(tmp_path / "**/*.jpg"), recursive=True,
    )


@pytest.mark.parametrize("url", [
    "https://i.ytimg.com/vi/ZYXWVUTSRQP/hqdefault.jpg",
    "https://i.ytimg.com.evil.test/vi/abcdefghijk/hqdefault.jpg",
    "https://i.ytimg.com:444/vi/abcdefghijk/hqdefault.jpg",
    "https://user@i.ytimg.com/vi/abcdefghijk/hqdefault.jpg",
    "https://yt3.ggpht.com/channel-avatar",
])
def test_fallback_requires_exact_video_and_known_thumbnail_host(
    url: str,
) -> None:
    assert youtube_thumbnail_urls(discovery(url)) == ()


def test_signature_groups_resizing_but_keeps_different_artwork(
    tmp_path: pathlib.Path,
) -> None:
    signatures: list[ThumbnailSignature] = []

    for name, body in (("original", picture()),
                       ("resized", picture(width=320)),
                       ("different", picture("blue"))):
        path = tmp_path / f"{name}.jpg"
        path.write_bytes(body)
        signatures.append(thumbnail_signature(path))

    assert similar_thumbnail_signatures(signatures[0], signatures[1])
    assert not similar_thumbnail_signatures(signatures[0], signatures[2])


async def test_flush_preserves_legacy_and_separates_observation_metadata(
    tmp_path: pathlib.Path,
) -> None:
    target = tmp_path / "youtube.com" / f"video:{VIDEO}"
    target.mkdir(parents=True)
    old = http_asset_path(target, CAPTURED)
    old.write_bytes(picture())
    previous = NOW - dt.timedelta(days=1)
    client = Mock(fetch_json=AsyncMock(return_value=SimpleNamespace(rows=[{
        "post_id": f"video:{VIDEO}", "url": CAPTURED,
        "local_path": str(old), "observed_at": previous,
    }])), insert_rows=AsyncMock())
    totals = await runner._flush_assets(  # pyright: ignore[reportPrivateUsage]
        client, cast(Any, Client({CAPTURED: Response(picture("blue"))})),
        [discovery()], assets_dir=tmp_path, browser_profile_dir=tmp_path,
        max_asset_bytes=None,
    )
    calls = client.insert_rows.call_args_list
    old_observation = calls[0].args[1][0]
    raw = calls[1].args[1][0]
    fresh_observation = calls[2].args[1][0]

    assert totals.assets_available == 1
    assert old_observation["observed_at"] == previous
    assert await asyncio.to_thread(
        pathlib.Path(old_observation["local_path"]).read_bytes,
    ) == picture()
    assert old_observation["captured_url"] == CAPTURED
    assert "thumbnail" not in raw
    assert fresh_observation["local_path"] == raw["local_path"]
    assert fresh_observation["content_hash"] != old_observation["content_hash"]
    assert old.read_bytes() == picture()


async def test_targeted_retry_does_not_launch_collection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path, media_download_types=("image",),
    )
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.fetch_json.side_effect = [
        SimpleNamespace(rows=[]),
        SimpleNamespace(rows=[{"post_id": f"video:{VIDEO}", "url": CAPTURED}]),
    ]
    flush = AsyncMock(return_value=runner.ScrapeTotals(0, 1, 0))
    monkeypatch.setattr(runner, "ClickHouseClient", Mock(return_value=client))
    monkeypatch.setattr(runner, "_flush_assets", flush)
    totals = await runner.retry_youtube_thumbnails(
        settings, post_ids=(f"video:{VIDEO}",),
    )

    assert totals.assets_available == 1
    assert client.fetch_json.call_args.args[1] == {"ids": [f"video:{VIDEO}"]}
    assert "WHERE local_path = ''" in client.fetch_json.call_args.args[0]
    assert flush.call_args.args[2] == [discovery()]


async def test_snapshot_sniffs_avif_returned_for_jpeg_url(
    tmp_path: pathlib.Path,
) -> None:
    output = io.BytesIO()
    Image.new("RGB", (240, 135), "red").save(output, format="AVIF")
    result = await capture(
        tmp_path, Client({CAPTURED: Response(output.getvalue())}),
    )

    assert result.local_path.endswith(".avif")
    assert await asyncio.to_thread(
        pathlib.Path(result.local_path).read_bytes,
    ) == output.getvalue()


def test_cli_passes_targeted_thumbnail_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.scrape.__main__ import main

    run = Mock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr("sys.argv", [
        "scrape", "--retry-youtube-thumbnails",
        "--youtube-thumbnail-id", VIDEO,
        "--youtube-thumbnail-id", "bVRiUa04bHE",
    ])
    main()

    assert run.call_args.kwargs["retry_youtube_thumbnails_only"] is True
    assert run.call_args.kwargs["youtube_thumbnail_ids"] == (
        f"video:{VIDEO}", "video:bVRiUa04bHE",
    )


@pytest.mark.parametrize("args", [
    ["--youtube-thumbnail-id", VIDEO],
    ["--retry-youtube-thumbnails", "--youtube-thumbnail-id", "invalid"],
    ["--retry-youtube-thumbnails", "--domains", "reddit.com"],
    ["--retry-youtube-thumbnails", "--x-details"],
])
def test_cli_rejects_ambiguous_thumbnail_repair(
    monkeypatch: pytest.MonkeyPatch, args: list[str],
) -> None:
    from dank.scrape.__main__ import main

    run = Mock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr("sys.argv", ["scrape", *args])

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    run.assert_not_called()


async def test_same_image_url_keeps_each_listing_title_and_capture_time(
    tmp_path: pathlib.Path,
) -> None:
    from dank.model import YouTubeThumbnailCapture

    earlier = NOW - dt.timedelta(minutes=3)
    discoveries = [
        discovery()._replace(youtube_capture=YouTubeThumbnailCapture(
            "Title A", earlier,
        )),
        discovery()._replace(youtube_capture=YouTubeThumbnailCapture(
            "Title B", NOW,
        )),
    ]
    client = Client({CAPTURED: Response(picture())})
    saved = await download_assets(
        discoveries, assets_dir=tmp_path, http_client=cast(Any, client),
        scraped_at=NOW, download_types=("image",),
    )
    assert len(saved) == 2
    assert saved[0].local_path == saved[1].local_path
    assert saved[0].thumbnail is not None
    assert saved[1].thumbnail is not None
    assert saved[0].thumbnail.title == "Title A"
    assert saved[0].thumbnail.title_observed_at == earlier
    assert saved[1].thumbnail.title == "Title B"
    assert saved[1].thumbnail.title_observed_at == NOW


def test_discovered_thumbnail_carries_its_original_listing_title() -> None:
    import json

    from dank.config import YouTubeSettings
    from dank.model import RawPost, YouTubeThumbnailCapture
    from dank.scrape.youtube.payloads import discover_assets

    post = RawPost(
        "youtube.com", f"video:{VIDEO}", "https://www.youtube.com/watch",
        None, NOW, "youtube", "https://www.youtube.com/feed/hype",
        json.dumps({"kind": "video", "title": "Observed together",
                    "thumbnail_url": CAPTURED}),
    )
    assets = discover_assets(post, YouTubeSettings())
    assert assets[0].youtube_capture == YouTubeThumbnailCapture(
        "Observed together", NOW,
    )
