from __future__ import annotations

import json

import pytest
from clickhouse_connect.driver.binding import (
    bind_query,  # pyright: ignore[reportUnknownVariableType]
)

from dank.web.reader_youtube import (
    _item,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
    encode_youtube_cursor,
)

VIDEO = "NnV_cWeoo5Q"


def test_query_enriches_linked_titles_but_preserves_selected_feed_capture():
    linked, _ = build_youtube_query(YouTubeFilters(feed="linked"))
    feed, _ = build_youtube_query(YouTubeFilters(feed="https://www.youtube.com/feed/hype"))
    assert "youtube_video_metadata" in linked
    last_success = "argMaxIf(v.payload, v.observed_at, v.status = 'captured')"
    assert last_success in linked
    assert "if(notEmpty(JSONExtractString(i.payload, 'title'))" not in linked
    assert "if(notEmpty(JSONExtractString(i.payload, 'title'))" in feed
    assert "youtube_video_metadata" in feed


def test_metadata_and_full_details_have_separate_honest_statuses():
    item = _item({"item_id": f"video:{VIDEO}", "payload": json.dumps({
        "title": "Security in the LLM age",
        "metadata_capture": {
            "status": "failed", "available": True,
            "captured_at": "2026-10-10T11:00:00+00:00",
            "observed_at": "2026-10-17T11:00:00+00:00",
            "provider": "youtube_oembed",
        },
    })})
    assert item["title"] == "Security in the LLM age"
    assert item["metadata_capture"]["available"] is True
    assert item["metadata_capture"]["status"] == "failed"
    assert item["detail_capture"]["status"] == "disabled"


@pytest.mark.parametrize("feed", ["", "linked", "https://www.youtube.com/feed/hype"])
@pytest.mark.parametrize("count", [False, True])
@pytest.mark.parametrize("paged", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_real_driver_binds_every_metadata_query_shape(
    feed: str, *, count: bool, paged: bool, filtered: bool,
):
    filters = YouTubeFilters(
        feed=feed, item_id=f"video:{VIDEO}" if filtered else "",
        kind="video" if filtered else "",
        channel="https://www.youtube.com/@KernelRecipes" if filtered else "",
        q="Provider's title {subject: security}" if filtered else "",
        limit=17,
    )

    if paged:
        filters = filters._replace(cursor=encode_youtube_cursor(
            filters, "2026-10-17T12:00:00+00:00", f"video:{VIDEO}", 1,
        ))

    query, params = build_youtube_query(filters, count=count)
    bound, external = bind_query(query, params)
    assert not external
    assert "%(" not in bound

    if filtered:
        assert VIDEO in bound
        assert "@KernelRecipes" in bound

    if not count:
        assert "LIMIT 18" in bound
