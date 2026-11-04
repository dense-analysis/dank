"""YouTube rules, with plain values."""

from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Any

import pytest

from dank.api.youtube.model import (
    ChannelCursor,
    PlaylistPage,
    YouTubeFilters,
    channel_cursor,
    decode_youtube_cursor,
    encode_youtube_cursor,
    not_collected,
    parse_channel_cursor,
    parse_cursor,
    playlist_cursor,
    playlist_page,
    require_channel_page,
    timestamp_text,
    transcript_view,
    youtube_cursor_snapshot,
    youtube_filters,
)
from dank.errors import InvalidInput

HOME = "https://www.youtube.com/"
HYPE = "https://www.youtube.com/feed/hype"
ANCHOR = "2026-10-23T12:00:00+00:00"
EARLIER = "2026-10-23T09:00:00+00:00"
ITEM = "video:aaaaaaaaaaa"
STAMP = "2026-10-25T18:00:00.123456+00:00"
NOW = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)


def test_filters_trim_what_a_person_typed_but_not_opaque_values() -> None:
    filters = youtube_filters(YouTubeFilters(
        feed=f" {HYPE} ", channel=" UC1 ", q=" words ", item_id=" id ",
    ))

    assert filters == YouTubeFilters(
        feed=HYPE, channel="UC1", q="words", item_id=" id ",
    )


@pytest.mark.parametrize(("field", "limit"), [
    ("feed", 2000), ("channel", 2000), ("q", 2000), ("item_id", 2000),
])
def test_filters_allow_up_to_the_length_limit(field: str, limit: int) -> None:
    fine, too_long = "a" * limit, "a" * (limit + 1)
    youtube_filters(YouTubeFilters()._replace(**{field: fine}))

    with pytest.raises(InvalidInput, match="2000 characters"):
        youtube_filters(YouTubeFilters()._replace(**{field: too_long}))


@pytest.mark.parametrize(("changes", "message"), [
    ({"limit": 0}, "Invalid limit or content kind"),
    ({"limit": 501}, "Invalid limit or content kind"),
    ({"kind": "live"}, "Invalid limit or content kind"),
    ({"sort": "invalid"}, "Invalid sort for this YouTube feed"),
    ({"sort": "most-linked"}, "Invalid sort for this YouTube feed"),
    ({"feed": HYPE, "sort": "oldest"}, "Invalid sort for this YouTube feed"),
    ({"channel_scope": "old"}, "Invalid channel scope"),
    ({"cursor": "not base64"}, "Invalid cursor for these filters"),
])
def test_filter_rules_keep_their_wording(
    changes: dict[str, Any], message: str,
) -> None:
    with pytest.raises(InvalidInput, match=message):
        youtube_filters(YouTubeFilters(**changes))


@pytest.mark.parametrize("sort", ["newest", "oldest", "most-linked"])
def test_only_the_linked_feed_sorts(sort: str) -> None:
    assert youtube_filters(YouTubeFilters(feed="linked", sort=sort)).sort == (
        sort
    )


def test_cursors_bind_filters_and_their_position() -> None:
    filters = YouTubeFilters(feed="linked", kind="video")
    cursor = encode_youtube_cursor(filters, ANCHOR, ITEM, 7, 150)
    paged = filters._replace(cursor=cursor)

    assert decode_youtube_cursor(paged) == (ANCHOR, ITEM, 7, 150)

    for other in (
        paged._replace(feed="different"),
        paged._replace(kind="short"),
        paged._replace(sort="oldest"),
        paged._replace(channel_scope="configured"),
    ):
        with pytest.raises(InvalidInput, match="Invalid cursor"):
            decode_youtube_cursor(other)


def test_only_recommendation_cursors_carry_a_snapshot() -> None:
    other = YouTubeFilters(feed=HYPE)
    home = YouTubeFilters(feed=HOME)

    assert len(json.loads(base64.urlsafe_b64decode(
        encode_youtube_cursor(other, ANCHOR, ITEM),
    ))) == 5
    assert len(json.loads(base64.urlsafe_b64decode(
        encode_youtube_cursor(home, ANCHOR, ITEM),
    ))) == 6


def test_a_recommendation_cursor_keeps_its_cutoff_as_it_moves_back() -> None:
    filters = YouTubeFilters(feed=HOME, kind="video")
    cursor = encode_youtube_cursor(
        filters, EARLIER, ITEM, 4, snapshot_time=ANCHOR,
    )
    paged = filters._replace(cursor=cursor)

    assert decode_youtube_cursor(paged)[0] == EARLIER
    assert youtube_cursor_snapshot(paged) == ANCHOR


@pytest.mark.parametrize("snapshot", [
    "not-a-time", "2026-10-23T10:00:00", "2026-10-22T10:00:00+00:00",
])
def test_recommendation_cursors_reject_a_bad_cutoff(snapshot: str) -> None:
    filters = YouTubeFilters(feed=HOME)
    cursor = encode_youtube_cursor(
        filters, EARLIER, ITEM, snapshot_time=snapshot,
    )

    with pytest.raises(InvalidInput, match="Invalid cursor"):
        decode_youtube_cursor(filters._replace(cursor=cursor))


@pytest.mark.parametrize("changes", [
    {"limit": 51}, {"item_id": "video:x"},
])
def test_channel_pages_are_bounded(changes: dict[str, Any]) -> None:
    with pytest.raises(InvalidInput, match="up to 50 channels"):
        require_channel_page(YouTubeFilters(**changes))


def test_channel_cursors_bind_filters_and_their_position() -> None:
    filters = YouTubeFilters(feed=HOME, q="ambient", limit=20)
    cursor = channel_cursor(filters, STAMP, 8, "UCexample")

    assert parse_channel_cursor(filters, cursor) == ChannelCursor(
        STAMP, 8, "UCexample",
    )

    with pytest.raises(InvalidInput, match="Invalid channel cursor"):
        parse_channel_cursor(
            filters._replace(channel_scope="configured"), cursor,
        )


@pytest.mark.parametrize(("snapshot", "count", "key"), [
    ("2026-10-23T10:00:00", 8, "UC1"),
    ("not-a-time", 8, "UC1"),
    (STAMP, 0, "UC1"),
    (STAMP, True, "UC1"),
    (STAMP, 8, "k" * 2001),
])
def test_channel_cursors_reject_bad_values(
    snapshot: str, count: Any, key: str,
) -> None:
    filters = YouTubeFilters(feed=HOME)
    cursor = channel_cursor(filters, snapshot, count, key)

    with pytest.raises(InvalidInput, match="Invalid channel cursor"):
        parse_channel_cursor(filters, cursor)


def test_playlist_pages_trim_searches_and_default_to_the_saved_order() -> None:
    assert playlist_page(limit=30, q="  music ") == PlaylistPage(q="music")


@pytest.mark.parametrize(("changes", "message"), [
    ({"sort": "newest"}, "Playlist sort must be recent or az"),
    ({"limit": 0}, "Invalid playlist limit or search"),
    ({"limit": 101}, "Invalid playlist limit or search"),
    ({"q": "a" * 2001}, "Invalid playlist limit or search"),
    ({"playlist_id": "PL.bad"}, "Invalid YouTube playlist identifier"),
    ({"playlist_id": "a" * 201}, "Invalid YouTube playlist identifier"),
    ({"cursor": "bad"}, "Invalid cursor for this playlist view"),
])
def test_playlist_rules_keep_their_wording(
    changes: dict[str, Any], message: str,
) -> None:
    values: dict[str, Any] = {"limit": 30, **changes}

    with pytest.raises(InvalidInput, match=message):
        playlist_page(**values)


def test_a_library_cursor_binds_its_search_and_snapshot() -> None:
    cursor = playlist_cursor("", "music", STAMP, ("saved music", "PLexample"))
    page = playlist_page(limit=1, q="music", cursor=cursor)

    assert page.snapshot == STAMP
    assert page.after == ("saved music", "PLexample")

    with pytest.raises(InvalidInput):
        playlist_page(limit=1, q="changed", cursor=cursor)


def test_a_library_cursor_binds_its_sort() -> None:
    cursor = playlist_cursor("", "", STAMP, ("music", "PLid"), "az")

    assert playlist_page(limit=1, sort="az", cursor=cursor).sort == "az"

    with pytest.raises(InvalidInput):
        playlist_page(limit=1, cursor=cursor)


def test_a_detail_cursor_binds_its_playlist_and_search() -> None:
    cursor = playlist_cursor("PLexample", "O'Reilly", STAMP, 77)
    page = playlist_page(
        limit=1, q="O'Reilly", cursor=cursor, playlist_id="PLexample",
    )

    assert page.after == 77

    for changes in (
        {"q": "O'Reilly", "playlist_id": "PLdifferent"},
        {"q": "", "playlist_id": "PLexample"},
        {"q": "changed", "playlist_id": "PLexample"},
    ):
        with pytest.raises(InvalidInput):
            playlist_page(limit=1, cursor=cursor, **changes)


@pytest.mark.parametrize(("identity", "after"), [
    ("PLexample", 0),
    ("PLexample", True),
    ("PLexample", "3"),
    ("", ["x"]),
    ("", 2),
    ("", ["a" * 4001, "b"]),
])
def test_playlist_cursors_reject_a_bad_boundary(
    identity: str, after: Any,
) -> None:
    cursor = playlist_cursor(identity, "", STAMP, after)

    with pytest.raises(InvalidInput):
        parse_cursor(cursor, identity, "")


def test_a_saved_transcript_shows_its_words_but_not_its_raw_data() -> None:
    view = transcript_view("abc", {
        "status": "captured", "observed_at": NOW,
        "retry_after": NOW + dt.timedelta(days=1),
        "track_payload": "private provider URL", "error": "private error",
        "payload": json.dumps({
            "language": "en", "language_name": "English", "generated": True,
            "segments": [{"start": 1, "duration": 2, "text": "Saved words"}],
        }),
    })

    assert view == {
        "video_id": "abc",
        "status": "captured",
        "observed_at": NOW.isoformat(),
        # A captured transcript is final, so there's nothing to retry.
        "retry_after": "",
        "language": "en",
        "language_name": "English",
        "generated": True,
        "segments": [{"start": 1, "duration": 2, "text": "Saved words"}],
    }


def test_a_failed_transcript_says_when_to_retry() -> None:
    retry = NOW + dt.timedelta(hours=1)
    view = transcript_view("abc", {
        "status": "failed", "observed_at": NOW, "retry_after": retry,
        "payload": json.dumps({"segments": [{"text": "Left over"}]}),
    })

    assert view["retry_after"] == retry.isoformat()
    assert view["segments"] == []


def test_a_video_with_no_row_is_not_collected() -> None:
    assert not_collected("abc") == {
        "video_id": "abc", "status": "not_collected", "observed_at": "",
        "retry_after": "", "language": "", "language_name": "",
        "generated": False, "segments": [],
    }


@pytest.mark.parametrize(("value", "text"), [
    (NOW, NOW.isoformat()),
    (NOW.replace(tzinfo=None), NOW.isoformat()),
    (NOW.isoformat(), NOW.isoformat()),
    ("not a date", ""),
    ("", ""),
    (None, ""),
])
def test_timestamps_are_iso_text_or_empty(value: object, text: str) -> None:
    assert timestamp_text(value) == text
