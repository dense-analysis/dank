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


def test_convert_raw_x_post_preserves_unexpanded_trailing_link() -> None:
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
    assert post.html == (
        '<p>Hello world <a href="https://t.co/abc123">'
        'https://t.co/abc123</a></p>'
    )
    assert post.title == "Hello world https://t.co/abc123"
    assert post.author == "alice"


def test_convert_raw_x_post_uses_note_tweet_text() -> None:
    raw = _raw_post({
        "legacy": {"full_text": "Short legacy preview..."},
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
    assert post.html == (
        '<p>Long note<br>line 2 <a href="https://t.co/xyz987">'
        'https://t.co/xyz987</a></p>'
    )
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


def test_x_text_escapes_literal_markup_and_keeps_paragraphs() -> None:
    raw = _raw_post({"legacy": {"full_text": (
        '&lt;script&gt;bad()&lt;/script&gt; &amp; <img src=x onerror=bad()>\n'
        'still same paragraph\n\nAnother paragraph'
    )}})
    post = convert_raw_x_post(raw)
    assert post is not None
    assert post.html == (
        '<p>&lt;script&gt;bad()&lt;/script&gt; &amp; '
        '&lt;img src=x onerror=bad()&gt;<br>still same paragraph</p>'
        '<p>Another paragraph</p>'
    )


def test_convert_raw_x_post_preserves_media_only_without_author() -> None:
    post = convert_raw_x_post(_raw_post({"legacy": {
        "full_text": "", "extended_entities": {"media": [{
            "type": "photo", "media_url_https": "https://pbs.twimg.com/media/a.jpg",
        }]},
    }}))
    assert post is not None
    assert post.html == ""


def test_undated_x_posts_keep_capture_date_when_reprocessed() -> None:
    raw = _raw_post({"legacy": {"full_text": "No published date"}})
    first = convert_raw_x_post(raw)
    replayed = convert_raw_x_post(raw)
    assert first is not None and replayed is not None
    assert first.created_at == replayed.created_at == raw.scraped_at
