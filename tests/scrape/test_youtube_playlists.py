from __future__ import annotations

import datetime
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import YouTubeSettings
from dank.model import YouTubePlaylistCapture
from dank.scrape import runner
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube import playlist_browser
from dank.scrape.youtube.playlists import (
    continuation_playlist_page,
    discover_playlists,
    initial_playlist_page,
    playlist_capture,
    playlist_metadata,
    playlist_posts,
    snapshot_hash,
)
from dank.storage.clickhouse import ClickHouseClient

URL = "https://www.youtube.com/playlist?list=PLtest"


def video(
    identifier: str = "abcdefghijk",
    *,
    title: str = "Video",
    occurrence: str = "one",
) -> dict[str, Any]:
    return {
        "videoId": identifier,
        "title": {"simpleText": title},
        "setVideoId": occurrence,
        "lengthSeconds": "123",
        "shortBylineText": {
            "runs": [
                {
                    "text": "Owner",
                    "navigationEndpoint": {
                        "browseEndpoint": {"browseId": "UCowner"},
                    },
                },
            ],
        },
    }


def initial(
    entries: list[dict[str, Any]],
    *,
    token: str = "",
    order: str = "Manual",
    reported: int | None = None,
) -> dict[str, Any]:
    contents: list[dict[str, Any]] = [
        {"playlistVideoRenderer": entry} for entry in entries
    ]

    if token:
        contents.append(
            {
                "continuationItemRenderer": {
                    "continuationEndpoint": {
                        "continuationCommand": {"token": token},
                    },
                },
            },
        )

    return {
        "metadata": {"playlistMetadataRenderer": {"title": "Examples"}},
        "header": {
            "playlistHeaderRenderer": {
                "numVideosText": {
                    "simpleText": f"{reported or len(entries)} videos",
                },
            },
        },
        "contents": {
            "playlistVideoListRenderer": {
                "playlistId": "PLtest",
                "contents": contents,
                "sortFilterMenu": {
                    "sortFilterSubMenuRenderer": {
                        "subMenuItems": [{"title": order, "selected": True}],
                    },
                },
            },
        },
    }


def capture(
    *entries: dict[str, Any], status: str = "complete", order: str = "Manual",
) -> YouTubePlaylistCapture:
    data = initial(list(entries), order=order)

    return playlist_capture(
        "PLtest", playlist_metadata("PLtest", data), entries, status=status,
    )


def test_entries_keep_duplicate_occurrences_and_unavailable_slots() -> None:
    result = capture(
        video(),
        video(occurrence="two"),
        video("", title="[Private video]", occurrence="hidden"),
    )
    assert [entry["position"] for entry in result.entries] == [1, 2, 3]
    assert [entry["video_id"] for entry in result.entries] == [
        "abcdefghijk",
        "abcdefghijk",
        "",
    ]
    assert [entry["available"] for entry in result.entries] == [
        True,
        True,
        False,
    ]
    assert result.entries[0]["duration_seconds"] == 123
    assert result.entries[0]["channel_name"] == "Owner"
    assert [
        json.loads(post.payload)["discovery"]["rank"]
        for post in playlist_posts(result)
    ] == [1, 2]


def test_hash_tracks_order_sort_removals_metadata_and_excludes_transport() -> (
    None
):
    first = capture(video(), video("zyxwvutsrqp", occurrence="two"))
    base = snapshot_hash(first.metadata, first.entries)
    changes = [
        capture(video("zyxwvutsrqp", occurrence="two"), video()),
        capture(
            video(),
            video("zyxwvutsrqp", occurrence="two"),
            order="Date added (newest)",
        ),
        capture(video()),
        capture(
            video(title="Changed"), video("zyxwvutsrqp", occurrence="two"),
        ),
    ]
    assert all(
        snapshot_hash(item.metadata, item.entries) != base for item in changes
    )
    changed_payload = tuple(
        {**item, "payload": "transient response tokens"}
        for item in first.entries
    )
    assert snapshot_hash(first.metadata, changed_payload) == base
    saved = {
        **first.metadata,
        "checked_at": datetime.datetime.now(datetime.UTC),
        "snapshot_hash": "old",
        "status": "failed",
        "entry_count": 2,
    }
    metadata = playlist_metadata("PLtest", initial([video()]), fallback=saved)
    assert (
        not {"checked_at", "snapshot_hash", "status", "entry_count"}
        & metadata.keys()
    )
    snapshot_hash(metadata, first.entries)


def test_library_lockups_parse_counts_and_ignore_videos() -> (
    None
):
    payload = {
        "lockupViewModel": {
            "contentId": "PLtest",
            "contentType": "LOCKUP_CONTENT_TYPE_PLAYLIST",
            "metadata": {
                "lockupMetadataViewModel": {"title": {"content": "Saved"}},
            },
            "contentImage": {
                "collectionThumbnailViewModel": {
                    "primaryThumbnail": {
                        "thumbnailViewModel": {
                            "image": {
                                "sources": [
                                    {
                                        "url": "https://i.ytimg.com/image.jpg",
                                        "width": 320,
                                    },
                                ],
                            },
                            "overlays": [
                                {
                                    "thumbnailBadgeViewModel": {
                                        "text": "1,968 videos",
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
    }
    cards = discover_playlists([payload, {"videoRenderer": video()}])
    assert len(cards) == 1
    assert cards[0]["title"] == "Saved"
    assert cards[0]["reported_count"] == 1968
    assert cards[0]["thumbnail_url"] == "https://i.ytimg.com/image.jpg"


def test_initial_and_continuation_preserve_page_sequence() -> None:
    page = initial_playlist_page(initial([video()], token="next"))
    assert page.recognized and page.continuation == "next"
    continuation = continuation_playlist_page(
        {
            "onResponseReceivedActions": [
                {
                    "appendContinuationItemsAction": {
                        "targetId": "playlist-video-list",
                        "continuationItems": [
                            {"playlistVideoRenderer": video(occurrence="two")},
                        ],
                    },
                },
            ],
        },
    )
    assert continuation.recognized and not continuation.continuation
    assert continuation.entries[0]["setVideoId"] == "two"
    assert not initial_playlist_page(
        {"contents": {"shortsLockupViewModel": {}}},
    ).recognized


def install_browser(
    monkeypatch: pytest.MonkeyPatch,
    data: dict[str, Any],
    responses: list[dict[str, Any]] | None = None,
) -> AsyncMock:
    monkeypatch.setattr(playlist_browser, "navigate", AsyncMock())
    monkeypatch.setattr(
        playlist_browser,
        "snapshot",
        AsyncMock(
            return_value={
                "initial": data,
                "signed_in": True,
            },
        ),
    )
    network = AsyncMock()
    network.drain.side_effect = [
        [SimpleNamespace(status=200, body=json.dumps(response))]
        for response in responses or []
    ]
    monkeypatch.setattr(
        playlist_browser, "NetworkCapture", Mock(return_value=network),
    )

    return network


async def test_complete_collection_follows_every_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = initial([video()], token="next", reported=2)
    response = {
        "onResponseReceivedActions": [
            {
                "appendContinuationItemsAction": {
                    "targetId": "playlist-video-list",
                    "continuationItems": [
                        {"playlistVideoRenderer": video(occurrence="two")},
                    ],
                },
            },
        ],
    }
    network = install_browser(monkeypatch, data, [response])
    result = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert result.status == "complete" and len(result.entries) == 2
    network.drain.assert_awaited_once()
    network.stop.assert_awaited_once()


async def test_repeated_continuation_keeps_partial_not_false_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = initial([video()], token="next", reported=2)
    response = {
        "onResponseReceivedActions": [
            {
                "appendContinuationItemsAction": {
                    "targetId": "playlist-video-list",
                    "continuationItems": [
                        {"playlistVideoRenderer": video(occurrence="two")},
                        {
                            "continuationItemRenderer": {
                                "continuationEndpoint": {
                                    "continuationCommand": {"token": "next"},
                                },
                            },
                        },
                    ],
                },
            },
        ],
    }
    install_browser(monkeypatch, data, [response])
    result = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert result.status == "partial"
    assert "Repeated" in result.error_type
    assert not result.exhausted
    assert len(result.entries) == 1


async def test_short_grid_and_hidden_entries_are_not_complete_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_browser(
        monkeypatch, {"playlistMetadataRenderer": {"title": "Shorts"}},
    )
    result = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert result.status == "failed"
    assert not result.exhausted
    install_browser(monkeypatch, initial([video()], reported=2))
    result = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert result.status == "partial"
    assert result.exhausted
    assert "1 entries not shown by YouTube" in result.error_type


class Client:
    def __init__(self) -> None:
        self.previous: list[dict[str, Any]] = []
        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.fail_entries = False

    async def fetch_json(self, *args: Any) -> SimpleNamespace:
        return SimpleNamespace(rows=self.previous)

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        if table == "youtube_playlist_entries" and self.fail_entries:
            raise RuntimeError("Database failure")

        self.tables.setdefault(table, []).extend(rows)

    async def prepare(self, item: YouTubePlaylistCapture):
        return await runner._prepare_youtube_playlists(  # pyright: ignore[reportPrivateUsage]
            cast(ClickHouseClient, self),
            ScrapeBatch([], [], youtube_playlists=(item,)),
        )


async def test_unchanged_snapshot_updates_check_without_posts_or_entries() -> (
    None
):
    client = Client()
    item = capture(video())
    first, rows = await client.prepare(item)
    assert len(first.posts) == 1
    assert (
        "youtube_playlists" not in client.tables
    )  # Pointer still unpublished.
    client.previous = rows
    second, updated = await client.prepare(
        item._replace(
            checked_at=item.checked_at + datetime.timedelta(minutes=1),
        ),
    )
    assert second.posts == [] and second.assets == []
    assert len(client.tables["youtube_playlist_entries"]) == 1
    assert updated[0]["observed_at"] == rows[0]["observed_at"]
    assert updated[0]["checked_at"] > rows[0]["checked_at"]


async def test_failed_and_partial_checks_retain_complete_snapshot() -> None:
    client = Client()
    _, rows = await client.prepare(capture(video(), video(occurrence="two")))
    client.previous = rows

    for status in ("partial", "failed"):
        batch, newer = await client.prepare(capture(video(), status=status))
        assert batch.posts == []
        assert newer[0]["snapshot_hash"] == rows[0]["snapshot_hash"]
        assert newer[0]["entry_count"] == 2
        assert newer[0]["status"] == status


async def test_first_partial_and_complete_empty_snapshot() -> (
    None
):
    client = Client()
    _, catalog = await client.prepare(capture(status="not_collected"))
    client.previous = catalog
    partial, partial_rows = await client.prepare(
        capture(video(), status="partial"),
    )
    assert len(partial.posts) == 1
    assert partial_rows[0]["entry_count"] == 1
    client.previous = partial_rows
    _, empty = await client.prepare(capture())
    assert empty[0]["entry_count"] == 0
    assert empty[0]["snapshot_hash"] != partial_rows[0]["snapshot_hash"]


async def test_membership_write_failure_never_publishes_pointer() -> None:
    client = Client()
    client.fail_entries = True

    with pytest.raises(RuntimeError, match="Database failure"):
        await client.prepare(capture(video()))

    assert "youtube_playlists" not in client.tables


async def test_non_authenticated_library_uses_bounded_oldest_known_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(playlist_browser, "LIBRARY_CHECK_LIMIT", 1)
    collect = AsyncMock(return_value=capture(video()))
    monkeypatch.setattr(playlist_browser, "collect_playlist", collect)
    cards = {
        "PLnew": {
            **playlist_metadata("PLnew", {}),
            "snapshot_hash": "new",
            "checked_at": "2026-10-25",
        },
        "PLold": {
            **playlist_metadata("PLold", {}),
            "snapshot_hash": "old",
            "checked_at": "2026-10-20",
        },
    }
    batches = [
        batch
        async for batch in playlist_browser.collect_playlist_library(
            AsyncMock(),
            YouTubeSettings(),
            cards,
            excluded=set(),
        )
    ]
    assert len(batches[0].youtube_playlists) == 2
    assert collect.await_args is not None
    assert collect.await_args.args[1].endswith("list=PLold")
    assert collect.await_args.kwargs["timeout_seconds"] <= 120


async def test_failed_checks_do_not_starve_complete_playlists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(playlist_browser, "LIBRARY_CHECK_LIMIT", 1)
    collect = AsyncMock(return_value=capture(video()))
    monkeypatch.setattr(playlist_browser, "collect_playlist", collect)
    cards = {
        "PLfailed": {**playlist_metadata("PLfailed", {}),
                     "snapshot_hash": "", "checked_at": "2026-10-25"},
        "PLold": {**playlist_metadata("PLold", {}),
                  "snapshot_hash": "old", "checked_at": "2026-10-20"},
    }
    batches = [
        batch async for batch in playlist_browser.collect_playlist_library(
            AsyncMock(), YouTubeSettings(), cards, excluded=set(),
        )
    ]
    assert batches
    assert collect.await_args is not None
    assert collect.await_args.args[1].endswith("list=PLold")


async def test_exhausted_visible_manifest_updates_despite_reported_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client()
    first_entries = [video(), video("zyxwvutsrqp", occurrence="two")]
    install_browser(monkeypatch, initial(first_entries, reported=3))
    first = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert first.status == "partial" and first.exhausted
    _, old_rows = await client.prepare(first)
    client.previous = old_rows
    reordered = [video("zyxwvutsrqp", occurrence="two"), video(),
                 video("01234567890", occurrence="three")]
    install_browser(monkeypatch, initial(reordered, reported=4))
    current = await playlist_browser.collect_playlist(
        AsyncMock(), URL, YouTubeSettings(),
    )
    assert current.status == "partial" and current.exhausted
    batch, changed = await client.prepare(current)
    assert len(batch.posts) == 3
    assert changed[0]["snapshot_hash"] != old_rows[0]["snapshot_hash"]
    assert changed[0]["entry_count"] == 3
    assert changed[0]["reported_count"] == 4
    assert changed[0]["status"] == "partial"
    assert [entry["video_id"] for entry in client.tables[
        "youtube_playlist_entries"
    ][-3:]] == ["zyxwvutsrqp", "abcdefghijk", "01234567890"]
    client.previous = changed
    unchanged, checked = await client.prepare(current)
    assert not unchanged.posts
    assert checked[0]["snapshot_hash"] == changed[0]["snapshot_hash"]
    interrupted, retained = await client.prepare(
        capture(video(), status="partial"),
    )
    assert not interrupted.posts
    assert retained[0]["snapshot_hash"] == changed[0]["snapshot_hash"]
    assert retained[0]["entry_count"] == 3


async def test_exhausted_visible_snapshot_can_replace_complete() -> None:
    client = Client()
    _, old = await client.prepare(capture(video()))
    client.previous = old
    visible = capture(video("zyxwvutsrqp"), status="partial")._replace(
        exhausted=True,
    )
    changed, rows = await client.prepare(visible)
    assert changed.posts
    assert rows[0]["snapshot_hash"] != old[0]["snapshot_hash"]
    assert rows[0]["status"] == "partial"


def short_card(identifier: str, occurrence: str) -> dict[str, Any]:
    return {"richItemRenderer": {"content": {"shortsLockupViewModel": {
        "entityId": occurrence, "indexInCollection": 0,
        "onTap": {"innertubeCommand": {"reelWatchEndpoint": {
            "videoId": identifier, "playlistId": "YS",
        }}},
        "overlayMetadata": {"primaryText": {"content": "Saved Short"}},
        "thumbnailViewModel": {"thumbnailViewModel": {"image": {"sources": [{
            "url": "https://i.ytimg.com/short.jpg", "width": 270,
        }]}}},
    }}}}


def shorts_initial(cards: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "metadata": {"playlistMetadataRenderer": {"title": "Saved Shorts"}},
        "header": {"playlistHeaderRenderer": {
            "playlistId": "YS", "numVideosText": {"simpleText": "2 videos"},
        }},
        "contents": {"twoColumnBrowseResultsRenderer": {"tabs": [{
            "tabRenderer": {"selected": True, "content": {
                "sectionListRenderer": {"contents": [{"itemSectionRenderer": {
                    "contents": [{"richGridRenderer": {"contents": cards}}],
                }}]},
            }},
        }]}},
    }


async def test_saved_shorts_keeps_array_order_and_original_cards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cards = [short_card("hEOFhU8fD58", "first"),
             short_card("vx8r0nubzsc", "second")]
    initial_data = shorts_initial(cards)
    install_browser(monkeypatch, initial_data)
    result = await playlist_browser.collect_playlist(
        AsyncMock(), "https://www.youtube.com/playlist?list=YS",
        YouTubeSettings(),
    )
    assert result.status == "complete" and result.exhausted
    assert [entry["position"] for entry in result.entries] == [1, 2]
    assert [entry["video_id"] for entry in result.entries] == [
        "hEOFhU8fD58", "vx8r0nubzsc",
    ]
    assert [entry["entry_id"] for entry in result.entries] == [
        "first", "second",
    ]
    assert json.loads(result.entries[0]["payload"]) == cards[0][
        "richItemRenderer"
    ]["content"]
    assert result.entries[0]["thumbnail_url"].endswith("short.jpg")
    posts = playlist_posts(result)
    assert json.loads(posts[0].payload)["kind"] == "short"
    assert json.loads(posts[0].payload)["renderer"] == cards[0][
        "richItemRenderer"
    ]["content"]
    assert not initial_playlist_page(
        initial_data, identifier="PLother",
    ).recognized


def test_short_playlist_scope_and_continuations() -> None:
    card = short_card("hEOFhU8fD58", "first")
    continuation = {"onResponseReceivedActions": [{
        "appendContinuationItemsAction": {
            "targetId": "rich-grid-renderer", "continuationItems": [card],
        },
    }]}
    assert continuation_playlist_page(continuation, identifier="YS").recognized
    assert not continuation_playlist_page(
        continuation, identifier="PLother",
    ).recognized
    wrong = short_card("vx8r0nubzsc", "other")
    wrong["richItemRenderer"]["content"]["shortsLockupViewModel"][
        "onTap"
    ]["innertubeCommand"]["reelWatchEndpoint"]["playlistId"] = "PLsuggested"
    assert not initial_playlist_page(
        shorts_initial([card, wrong]), identifier="YS",
    ).recognized


async def test_partial_catalog_cards_are_persistable_before_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover(page: object, found: dict[str, dict[str, Any]]):
        found["PLtest"] = playlist_metadata("PLtest", {})
        raise playlist_browser.PlaylistIncomplete("catalog stalled")

    monkeypatch.setattr(playlist_browser, "_library_cards", discover)
    batches = playlist_browser.collect_playlist_library(
        AsyncMock(),
        YouTubeSettings(authenticated=True),
        {},
        excluded=set(),
    )
    first = await anext(batches)
    assert first.youtube_playlists[0].playlist_id == "PLtest"
    assert first.youtube_playlists[0].status == "not_collected"
    assert first.youtube_playlist_order is None

    with pytest.raises(playlist_browser.PlaylistIncomplete):
        await anext(batches)


async def test_only_complete_authenticated_catalog_publishes_library_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cards = (playlist_metadata("PLz", {}), playlist_metadata("PLa", {}))
    monkeypatch.setattr(
        playlist_browser, "_library_cards", AsyncMock(return_value=cards),
    )
    monkeypatch.setattr(playlist_browser, "LIBRARY_CHECK_LIMIT", 0)
    batches = [
        batch
        async for batch in playlist_browser.collect_playlist_library(
            AsyncMock(),
            YouTubeSettings(authenticated=True),
            {},
            excluded=set(),
        )
    ]
    order = batches[0].youtube_playlist_order
    assert order is not None
    assert order == ("PLz", "PLa")
    client = AsyncMock()
    await runner._persist_youtube_playlist_order(  # pyright: ignore[reportPrivateUsage]
        client,
        order,
    )
    table, rows = client.insert_rows.call_args.args
    assert table == "youtube_playlist_library"
    assert rows[0]["playlist_ids"] == ["PLz", "PLa"]
    assert isinstance(rows[0]["observed_at"], datetime.datetime)
    unsigned = [
        batch
        async for batch in playlist_browser.collect_playlist_library(
            AsyncMock(),
            YouTubeSettings(),
            {c["playlist_id"]: c for c in cards},
            excluded=set(),
        )
    ]
    assert unsigned[0].youtube_playlist_order is None

    monkeypatch.setattr(
        playlist_browser, "_library_cards", AsyncMock(return_value=()),
    )
    empty = [
        batch async for batch in playlist_browser.collect_playlist_library(
            AsyncMock(), YouTubeSettings(authenticated=True), {},
            excluded=set(),
        )
    ]
    assert empty[0].youtube_playlist_order == ()
