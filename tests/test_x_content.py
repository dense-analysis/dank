from __future__ import annotations

from typing import Any, cast

import pytest

from dank.x_content import (
    http_url,
    media_url,
    post_html,
    post_links,
    post_media,
)


def linked_post() -> dict[str, Any]:
    return {
        "legacy": {
            "full_text": "Some reflections https://t.co/abc",
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/abc",
                        "expanded_url": "https://youtu.be/video",
                    },
                ],
            },
        },
        "card": {
            "legacy": {
                "binding_values": [
                    {
                        "key": "card_url",
                        "value": {"string_value": "https://t.co/abc"},
                    },
                    {
                        "key": "player_url",
                        "value": {
                            "string_value": "https://www.youtube.com/embed/video",
                        },
                    },
                    {"key": "title", "value": {"string_value": "A video"}},
                    {
                        "key": "description",
                        "value": {"string_value": "About math"},
                    },
                    {
                        "key": "player_image_large",
                        "value": {
                            "image_value": {
                                "url": "https://pbs.twimg.com/card_img/1/video.jpg",
                            },
                        },
                    },
                ],
            },
        },
    }


@pytest.mark.parametrize("bindings_as_map", [False, True])
def test_youtube_card_keeps_expanded_destination_and_captured_metadata(
    *,
    bindings_as_map: bool,
) -> None:
    payload = linked_post()

    if bindings_as_map:
        values = payload["card"]["legacy"]["binding_values"]
        payload["card"]["legacy"]["binding_values"] = {
            item["key"]: item["value"] for item in values
        }

    assert post_html(payload) == (
        '<p>Some reflections <a href="https://youtu.be/video">'
        "https://youtu.be/video</a></p>"
    )
    assert post_links(payload, {}) == [
        {
            "url": "https://youtu.be/video",
            "title": "A video",
            "description": "About math",
            "image_url": "https://pbs.twimg.com/card_img/1/video.jpg",
        },
    ]
    assert (
        post_links(
            payload,
            {
                "https://pbs.twimg.com/card_img/1/video.jpg":
                "/assets/video.jpg",
            },
        )[0]["image_url"]
        == "/assets/video.jpg"
    )


def test_note_entities_and_mentions_and_hashtags() -> None:
    payload = linked_post()
    payload["note_tweet"] = {
        "note_tweet_results": {
            "result": {
                "text": "Hi @alice #Math &amp; x@y.com\n\nhttps://t.co/abc",
                "entity_set": {
                    "urls": [
                        {
                            "url": "https://t.co/abc",
                            "expanded_url": "https://example.com/full",
                        },
                    ],
                },
            },
        },
    }
    body = post_html(payload)
    assert 'href="https://example.com/full"' in body
    assert 'href="https://youtu.be/video"' not in body
    assert '<a href="https://x.com/alice">@alice</a>' in body
    assert '<a href="https://x.com/hashtag/Math">#Math</a>' in body
    assert "&amp; x@y.com</p><p>" in body


def test_unknown_and_unsafe_links_never_disappear_or_become_markup() -> None:
    payload = linked_post()
    payload["legacy"]["entities"]["urls"][0]["expanded_url"] = (
        "javascript:alert(1)"
    )
    payload["legacy"]["full_text"] += " https://example.com/?a=1&b=2."
    body = post_html(payload)
    assert '<a href="https://t.co/abc">https://t.co/abc</a>' in body
    assert 'href="https://example.com/?a=1&amp;b=2"' in body
    assert body.endswith("a=1&amp;b=2</a>.</p>")
    assert "javascript:" not in body


def test_media_order_local_video_and_no_duplicate_poster() -> None:
    image = "https://pbs.twimg.com/media/photo.jpg"
    poster = "https://pbs.twimg.com/ext_tw_video_thumb/2/poster.jpg"
    low = "https://video.twimg.com/ext_tw_video/2/low.mp4"
    high = "https://video.twimg.com/ext_tw_video/2/high.mp4"
    payload = {
        "legacy": {
            "extended_entities": {
                "media": [
                    {
                        "type": "photo",
                        "media_url_https": image,
                        "ext_alt_text": "A photo",
                    },
                    {
                        "type": "video",
                        "media_url_https": poster,
                        "video_info": {
                            "variants": [
                                {
                                    "url": high,
                                    "content_type": "video/mp4",
                                    "bitrate": 200,
                                },
                                {
                                    "url": low,
                                    "content_type": "video/mp4",
                                    "bitrate": 100,
                                },
                                {
                                    "url": "https://video.twimg.com/a.m3u8",
                                    "content_type": "application/x-mpegURL",
                                },
                            ],
                        },
                    },
                    {"type": "photo", "media_url_https": image},
                ],
            },
        },
    }
    media = post_media(
        payload, {low: "/assets/low.mp4", poster: "/assets/poster.jpg"},
    )
    assert media == [
        {
            "type": "image",
            "url": image,
            "alt": "A photo",
            "poster_url": None,
            "remote": True,
        },
        {
            "type": "video",
            "url": "/assets/low.mp4",
            "alt": "",
            "poster_url": "/assets/poster.jpg",
            "remote": False,
        },
    ]
    assert post_media(payload, {})[1]["url"] == high


def test_gifs_and_unavailable_video_keep_playable_content_or_poster() -> None:
    payload = {
        "legacy": {
            "entities": {
                "media": [
                    {
                        "type": "animated_gif",
                        "media_url_https": "https://pbs.twimg.com/media/gif.jpg",
                        "video_info": {
                            "variants": [
                                {
                                    "url": "https://video.twimg.com/gif.mp4",
                                    "content_type": "video/mp4",
                                },
                            ],
                        },
                    },
                    {
                        "type": "video",
                        "media_url_https": "https://pbs.twimg.com/media/poster.jpg",
                        "video_info": {
                            "variants": [
                                {
                                    "url": "https://video.twimg.com/a.m3u8",
                                    "content_type": "application/x-mpegURL",
                                },
                            ],
                        },
                    },
                ],
            },
        },
    }
    media = post_media(payload, {})
    assert [item["type"] for item in media] == ["gif", "image"]
    assert media[1]["url"] == "https://pbs.twimg.com/media/poster.jpg"


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        123,
        "javascript:bad()",
        "//example.com/a",
        "https://x.com:bad/a",
        "https://user@x.com/a",
        "https://x.com\\@evil.com/a",
        "https://x.com/\na",
    ],
)
def test_invalid_urls_are_rejected(value: object) -> None:
    assert http_url(value) is None


@pytest.mark.parametrize(
    "value",
    [
        "https://evil.com/a.jpg",
        "http://pbs.twimg.com/a.jpg",
        "https://pbs.twimg.com.evil.com/a.jpg",
        "https://pbs.twimg.com:80/a.jpg",
    ],
)
def test_remote_media_uses_only_secure_x_hosts(value: str) -> None:
    assert media_url(value) is None
    assert media_url(value, video=True) is None


@pytest.mark.parametrize(
    "payload",
    cast(list[dict[str, Any]], [
        {},
        {"legacy": []},
        {"note_tweet": "bad", "card": {"legacy": {"binding_values": [None]}}},
        {"legacy": {"extended_entities": {"media": [None, {}, {"type": []}]}}},
    ]),
)
def test_malformed_capture_fields_are_tolerated(
    payload: dict[str, Any],
) -> None:
    assert post_html(payload) == ""
    assert post_links(payload, {}) == []
    assert post_media(payload, {}) == []
