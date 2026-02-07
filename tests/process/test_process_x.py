import datetime
import json
from typing import Any

from dank.model import RawPost
from dank.process.x import convert_raw_x_post


def _raw_post(*, payload: Any) -> RawPost:
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


def test_convert_raw_x_post_strips_trailing_tco() -> None:
    payload = {
        "legacy": {
            "full_text": "Hello world https://t.co/abc123",
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    }
    raw = _raw_post(payload=payload)

    post = convert_raw_x_post(raw)

    assert post is not None
    assert post.html == "Hello world"
    assert post.title == "Hello world"
    assert post.author == "alice"


def test_convert_raw_x_post_uses_note_tweet_text() -> None:
    payload = {
        "note_tweet": {
            "note_tweet_results": {
                "result": {"text": "Long note\nline 2 https://t.co/xyz987"},
            },
        },
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
    }
    raw = _raw_post(payload=payload)._replace(
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
    assert post.html == "Long note\nline 2"
    assert post.title == "Long note"
    assert post.created_at == raw.post_created_at
