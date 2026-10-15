import base64
import datetime
from pathlib import Path

import pytest

from dank.model import RawAsset
from dank.process.assets import convert_raw_asset
from dank.web.app import AssetRow, PostRow
from dank.web.reader_api import post_payload

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8A"
    "AwMCAO+jRZkAAAAASUVORK5CYII=",
)


def test_extensionless_png_is_reused_by_the_reader(tmp_path: Path) -> None:
    image = tmp_path / "hashed-image"
    image.write_bytes(PNG)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    url = "https://publisher.test/images/opaque-id?revision=4"
    raw = RawAsset("feed.test", "post", url, "image", now, "rss", str(image))
    asset = convert_raw_asset(raw)

    assert asset is not None
    assert asset.content_type == "image/png"
    assert asset.size_bytes == len(PNG)
    assert asset.url == url
    assert image.read_bytes() == PNG
    post = PostRow("feed.test", "post", "https://publisher.test/story", "",
                   "Story", f'<p>Body.</p><img src="{url}">', now, now, "rss")
    payload = post_payload(post, [AssetRow(
        asset.post_id, asset.url, asset.local_path, asset.content_type,
        asset.size_bytes,
    )], tmp_path)

    assert payload["thumbnail"] == "/assets/hashed-image"
    assert '<img src="/assets/hashed-image">' in payload["html"]
    assert payload["media"] == [
        {"url": "/assets/hashed-image", "content_type": "image/png"},
    ]


@pytest.mark.parametrize("data, asset_type", [
    (b"", "image"), (b"\x89PNG", "image"),
    (b"<html>Denied</html>", "image"), (PNG, "video"),
])
def test_unknown_extensionless_content_stays_unclassified(
    tmp_path: Path, data: bytes, asset_type: str,
) -> None:
    file = tmp_path / "unknown"
    file.write_bytes(data)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    asset = convert_raw_asset(RawAsset(
        "feed.test", "post", "https://publisher.test/image", asset_type, now,
        "rss", str(file),
    ))

    assert asset is not None
    assert asset.content_type == ""


def test_filename_content_type_stays_authoritative(tmp_path: Path) -> None:
    file = tmp_path / "attachment.txt"
    file.write_bytes(PNG)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    asset = convert_raw_asset(RawAsset(
        "feed.test", "post", "https://publisher.test/file", "image", now,
        "rss", str(file),
    ))

    assert asset is not None
    assert asset.content_type == "text/plain"
