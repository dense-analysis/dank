import datetime
import json
from typing import Any

from dank.model import RawPost
from dank.process.x import convert_raw_x_post


def _raw_post(payload: dict[str, Any]) -> RawPost:
    return RawPost(
        domain="x.com",
        post_id="123",
        url="https://x.com/alice/status/123",
        post_created_at=None,
        scraped_at=datetime.datetime(2026, 2, 1, 2, 0, tzinfo=datetime.UTC),
        source="x",
        request_url="https://x.com/i/api/graphql/Example",
        payload=json.dumps(payload),
    )


def test_convert_raw_x_post_keeps_unknown_trailing_tco() -> None:
    raw = _raw_post({
        "legacy": {
            "full_text": "Hello world https://t.co/abc123",
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "Hello world https://t.co/abc123"
    assert post.title == "Hello world"
    assert post.author == "alice"


def test_convert_raw_x_post_uses_note_tweet_text() -> None:
    raw = _raw_post({
        "note_tweet": {
            "note_tweet_results": {
                "result": {"text": "Long note\nline 2 https://t.co/xyz987"},
            },
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    raw = raw._replace(
        post_created_at=datetime.datetime(
            2026,
            1,
            27,
            23,
            0,
            tzinfo=datetime.UTC,
        ),
    )

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "Long note\nline 2 https://t.co/xyz987"
    assert post.title == "Long note"
    assert post.created_at == raw.post_created_at


def test_convert_raw_x_post_filters_sparse_payloads() -> None:
    raw = _raw_post({
        "__typename": "Tweet",
        "rest_id": "2019269738728468765",
    })

    post = convert_raw_x_post(raw)

    assert post is None

def test_convert_raw_x_post_unescapes_html() -> None:
    raw = _raw_post({
        "legacy": {
            "full_text": "&lt;Sam &amp; Max&gt;",
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.title == "<Sam & Max>"


def test_convert_raw_x_post_expands_urls_and_strips_trailing_media() -> None:
    raw = _raw_post({
        "legacy": {
            "full_text": (
                "Bluey update: https://t.co/article01 https://t.co/media02"
            ),
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/article01",
                        "expanded_url": "https://nichegamer.com/bluey-gold-pen/",
                    },
                ],
                "media": [{"url": "https://t.co/media02"}],
            },
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "Bluey update: https://nichegamer.com/bluey-gold-pen/"


def test_convert_raw_x_post_strips_trailing_self_link() -> None:
    raw = _raw_post({
        "legacy": {
            "full_text": "Thread update https://t.co/self01",
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/self01",
                        "expanded_url": "https://x.com/alice/status/123/photo/1",
                    },
                ],
            },
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "Thread update"


def test_convert_raw_x_post_title_strips_expanded_links() -> None:
    raw = _raw_post({
        "legacy": {
            "full_text": "News drop https://t.co/article01",
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/article01",
                        "expanded_url": "https://nichegamer.com/news-drop/",
                    },
                ],
            },
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    })

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "News drop https://nichegamer.com/news-drop/"
    assert post.title == "News drop"
