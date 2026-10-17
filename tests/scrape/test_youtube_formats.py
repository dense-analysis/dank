from typing import Any

import pytest

from dank.scrape.assets.limits import AssetSizeExceeded, VideoSizeLimit
from dank.scrape.assets.video_formats import youtube_format


def video(name: str, size: int | None, *, combined: bool = False):
    return {
        "format_id": name,
        "ext": "mp4",
        "protocol": "https",
        "vcodec": "avc1.64001f",
        "acodec": "mp4a.40.2" if combined else "none",
        "filesize": size,
    }


def audio(name: str, size: int):
    return {
        "format_id": name,
        "ext": "m4a",
        "protocol": "https",
        "vcodec": "none",
        "acodec": "mp4a.40.2",
        "filesize": size,
    }


def select(formats: list[dict[str, Any]], maximum: int | None):
    return next(youtube_format({"formats": formats}, maximum=maximum))


def test_separate_streams_choose_best_complete_pair_within_combined_budget():
    small_audio, large_audio = audio("a1", 100), audio("a2", 400)
    low, high = video("v1", 300), video("v2", 800)
    result = select([small_audio, large_audio, low, high], 1000)
    assert result["format_id"] == "v2+a1"
    assert result["requested_formats"] == [high, small_audio]
    assert result["ext"] == "mp4"
    assert result["protocol"] == "https+https"


def test_smaller_video_is_selected_when_largest_pair_does_not_fit():
    result = select(
        [audio("a", 200), video("small", 500), video("large", 900)], 1000,
    )
    assert result["format_id"] == "small+a"


def test_combined_mp4_is_supported_and_mux_margin_is_reserved():
    small, exact = (
        video("small", 900, combined=True),
        video("exact", 1000, combined=True),
    )
    assert select([small, exact], 1000) == small
    assert select([small, exact], None) == exact


def test_known_oversized_formats_do_not_fall_back_to_unmeasured_hls():
    formats = [audio("a", 200), video("known", 900), video("hls", None)]

    with pytest.raises(AssetSizeExceeded):
        select(formats, 1000)


def test_unknown_sizes_can_be_attempted_with_runtime_byte_guard():
    unknown = video("unknown", None)
    assert select([audio("a", 200), unknown], 1000)["format_id"] == "unknown+a"


@pytest.mark.parametrize(
    "formats", [[], [video("silent", 200)], [audio("audio", 100)]],
)
def test_missing_complete_video_is_failure_not_size_skip(
    formats: list[dict[str, Any]],
):
    with pytest.raises(ValueError, match="No complete"):
        select(formats, 1000)


def test_non_browser_codec_is_not_selected():
    av1 = video("av1", 100)
    av1["vcodec"] = "av01"
    assert (
        select([audio("a", 100), video("h264", 200), av1], 1000)["format_id"]
        == "h264+a"
    )


def test_actual_download_bytes_are_aggregated_without_double_counting():
    limit = VideoSizeLimit(100)
    limit(
        {"status": "downloading", "filename": "video", "downloaded_bytes": 60},
    )
    limit({"status": "finished", "filename": "video", "downloaded_bytes": 60})
    limit(
        {"status": "downloading", "filename": "audio", "downloaded_bytes": 40},
    )

    with pytest.raises(AssetSizeExceeded):
        limit(
            {
                "status": "downloading",
                "filename": "audio",
                "downloaded_bytes": 41,
            },
        )
