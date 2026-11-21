import datetime as dt
import pathlib
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.api.posts.model import AssetRow, PostRow
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_posts import post_payload
from dank.web.reader_youtube import (
    _local_media,  # pyright: ignore[reportPrivateUsage]
)


def post() -> PostRow:
    now = dt.datetime(2026, 11, 21, tzinfo=dt.UTC)

    return PostRow(
        "example.test", "post", "https://example.test/article", "Reporter",
        "Article", "<p>Saved article text</p>", now, now, "rss",
    )


def test_scrubbed_youtube_attachment_keeps_a_safe_embed_destination(
    tmp_path: pathlib.Path,
) -> None:
    video = tmp_path / "video.mp4"
    video.write_bytes(b"saved video")
    assets = [AssetRow(
        "post", "https://youtu.be/dQw4w9WgXcQ", str(video), "video/mp4", 11,
    )]
    before = post_payload(post(), assets, tmp_path)
    assert before["media"] == [{
        "url": "/assets/video.mp4", "content_type": "video/mp4",
    }]
    video.unlink()
    after = post_payload(post(), assets, tmp_path)

    assert after["html"] == before["html"] == "<p>Saved article text</p>"
    assert after["media"] == [{
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "content_type": "video/youtube",
    }]


@pytest.mark.parametrize("url", [
    "https://youtube.com.evil.test/watch?v=dQw4w9WgXcQ",
    "https://youtube.com@evil.test/watch?v=dQw4w9WgXcQ",
    "javascript:alert(1)", "https://media.example.test/video.mp4",
])
def test_missing_assets_do_not_turn_untrusted_urls_into_embeds(
    tmp_path: pathlib.Path, url: str,
) -> None:
    payload = post_payload(post(), [AssetRow(
        "post", url, str(tmp_path / "missing.mp4"), "video/mp4", 11,
    )], tmp_path)

    assert payload["media"] == []


async def test_youtube_collection_stops_advertising_a_removed_download(
    tmp_path: pathlib.Path,
) -> None:
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    client: Any = AsyncMock()
    client.fetch_json.return_value = QueryResult([{
        "post_id": "video:dQw4w9WgXcQ", "url": "https://youtu.be/dQw4w9WgXcQ",
        "local_path": str(video), "asset_type": "video",
        "scraped_at": dt.datetime(2026, 11, 21, tzinfo=dt.UTC),
    }])
    assert await _local_media(
        cast(ClickHouseClient, client), ["video:dQw4w9WgXcQ"], tmp_path,
    )
    video.unlink()

    assert await _local_media(
        cast(ClickHouseClient, client), ["video:dQw4w9WgXcQ"], tmp_path,
    ) == {}
