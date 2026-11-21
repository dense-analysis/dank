import base64
import datetime
from pathlib import Path

import pytest

from dank.api.posts.model import AssetRow, PostRow
from dank.model import RawAsset
from dank.process.assets import convert_raw_asset
from dank.web.reader_posts import post_payload

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8A"
    "AwMCAO+jRZkAAAAASUVORK5CYII=",
)
WEBP = base64.b64decode(
    "UklGRiIAAABXRUJQVlA4IBYAAAAwAQCdASoBAAEAAUAmJaQAA3AA/vuUAAA=",
)


@pytest.mark.parametrize("data, content_type", [
    (PNG, "image/png"), (WEBP, "image/webp"),
])
def test_extensionless_images_are_reused_by_the_reader(
    memory_path: Path, data: bytes, content_type: str,
) -> None:
    image = memory_path / "hashed-image"
    image.write_bytes(data)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    url = "https://publisher.test/images/opaque-id?revision=4"
    raw = RawAsset("feed.test", "post", url, "image", now, "rss", str(image))
    asset = convert_raw_asset(raw)

    assert asset is not None
    assert asset.content_type == content_type
    assert asset.size_bytes == len(data)
    assert asset.url == url
    assert image.read_bytes() == data
    post = PostRow("feed.test", "post", "https://publisher.test/story", "",
                   "Story", f'<p>Body.</p><img src="{url}">', now, now, "rss")
    payload = post_payload(post, [AssetRow(
        asset.post_id, asset.url, asset.local_path, asset.content_type,
        asset.size_bytes,
    )], memory_path)

    assert payload["thumbnail"] == "/assets/hashed-image"
    assert '<img src="/assets/hashed-image">' in payload["html"]
    assert payload["media"] == [
        {"url": "/assets/hashed-image", "content_type": content_type},
    ]


@pytest.mark.parametrize("data, asset_type", [
    (b"", "image"), (b"\x89PNG", "image"),
    (b"<html>Denied</html>", "image"), (PNG, "video"),
    (b"RIFF0000WAVE", "image"), (b"RIFF0000WEB", "image"),
])
def test_unknown_extensionless_content_stays_unclassified(
    memory_path: Path, data: bytes, asset_type: str,
) -> None:
    file = memory_path / "unknown"
    file.write_bytes(data)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    asset = convert_raw_asset(RawAsset(
        "feed.test", "post", "https://publisher.test/image", asset_type, now,
        "rss", str(file),
    ))

    assert asset is not None
    assert asset.content_type == ""


def test_filename_content_type_stays_authoritative(memory_path: Path) -> None:
    file = memory_path / "attachment.txt"
    file.write_bytes(PNG)
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    asset = convert_raw_asset(RawAsset(
        "feed.test", "post", "https://publisher.test/file", "image", now,
        "rss", str(file),
    ))

    assert asset is not None
    assert asset.content_type == "text/plain"


def test_file_scrubbed_between_existence_check_and_stat_is_skipped(
    memory_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = memory_path / "media.png"
    path.write_bytes(PNG)
    now = datetime.datetime(2026, 11, 21, tzinfo=datetime.UTC)
    raw = RawAsset("site.test", "post", "https://media/a", "image", now,
                   "rss", str(path))
    path_type = type(path)
    original = path_type.is_file

    def exists(candidate: Path) -> bool:
        if candidate == path:
            candidate.unlink()

            return True

        return original(candidate)

    monkeypatch.setattr(path_type, "is_file", exists)

    assert convert_raw_asset(raw) is None


def test_file_scrubbed_while_reading_its_image_signature_is_skipped(
    memory_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = memory_path / "opaque-media"
    path.write_bytes(PNG)
    now = datetime.datetime(2026, 11, 21, tzinfo=datetime.UTC)
    raw = RawAsset("site.test", "post", "https://media/a", "image", now,
                   "rss", str(path))
    path_type = type(path)
    original = path_type.is_file

    def exists(candidate: Path) -> bool:
        if candidate == path:
            # stat still succeeds, but opening the file must tolerate expiry.
            monkeypatch.setattr(path_type, "open", missing)

        return original(candidate)

    def missing(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("File was scrubbed")

    monkeypatch.setattr(path_type, "is_file", exists)

    assert convert_raw_asset(raw) is None
