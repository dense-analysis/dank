from __future__ import annotations

import datetime
import json
from typing import Any

import pytest

from dank.model import RawPost
from dank.process.reddit import convert_raw_reddit_post

PERMALINK = "https://www.reddit.com/r/LocalLLaMA/comments/abc123/new_model/"
SCRAPED_AT = datetime.datetime(2026, 10, 15, 12, tzinfo=datetime.UTC)


def _raw_post(payload: dict[str, Any]) -> RawPost:
    return RawPost(
        domain="reddit.com",
        post_id="t3_abc123",
        url=PERMALINK,
        post_created_at=None,
        scraped_at=SCRAPED_AT,
        source="reddit",
        request_url="https://www.reddit.com/r/LocalLLaMA/best/",
        payload=json.dumps(payload),
    )


def test_convert_reddit_self_post_preserves_body_and_identity() -> None:
    raw = _raw_post({
        "title": "Small & capable model",
        "author": "alice",
        "subreddit": "LocalLLaMA",
        "created_utc": 1792065600,
        "selftext_html": (
            '&lt;div class="md"&gt;&lt;p&gt;Useful &amp;amp; local.&lt;/p&gt;'
            "&lt;pre&gt;&lt;code&gt;run model&lt;/code&gt;&lt;/pre&gt;"
            "&lt;/div&gt;"
        ),
        "is_self": True,
        "url": PERMALINK,
        "raw": {"original": "retained"},
    })
    original_payload = raw.payload

    post = convert_raw_reddit_post(raw)

    assert post is not None
    assert post.domain == "reddit.com"
    assert post.post_id == "t3_abc123"
    assert post.url == PERMALINK
    assert post.source == "reddit"
    assert post.author == "alice"
    assert post.title == "Small & capable model"
    assert post.created_at == datetime.datetime(
        2026, 10, 15, 12, tzinfo=datetime.UTC,
    )
    assert post.updated_at == SCRAPED_AT
    assert "<p>Useful &amp; local.</p>" in post.html
    assert "<pre><code>run model</code></pre>" in post.html
    assert "<a " not in post.html
    assert raw.payload == original_payload


def test_convert_reddit_plain_text_is_escaped_and_preserves_paragraphs(
) -> None:
    post = convert_raw_reddit_post(_raw_post({
        "title": "Text post",
        "selftext": (
            "Keep <script>alert(1)</script> literal.\nNext line.\n\nEnd."
        ),
    }))

    assert post is not None
    assert post.html == (
        "<p>Keep &lt;script&gt;alert(1)&lt;/script&gt; literal.<br>"
        "Next line.</p>\n<p>End.</p>"
    )
    assert post.created_at == SCRAPED_AT


def test_convert_reddit_html_keeps_literal_code_and_resolves_links() -> None:
    post = convert_raw_reddit_post(_raw_post({
        "title": "Code example",
        "selftext_html": (
            '<p><a href="/r/LocalLLaMA/">Community</a></p>'
            '<pre><code>&lt;script&gt;literal&lt;/script&gt;</code></pre>'
            '<script>unwanted()</script>'
        ),
    }))

    assert post is not None
    assert 'href="https://www.reddit.com/r/LocalLLaMA/"' in post.html
    assert "&lt;script&gt;literal&lt;/script&gt;" in post.html
    assert "unwanted()" not in post.html


def test_convert_reddit_link_post_keeps_destination_separate_from_permalink(
) -> None:
    post = convert_raw_reddit_post(_raw_post({
        "title": "Release &amp; benchmarks",
        "url": 'https://example.com/release?a=1&b="two"',
        "is_self": False,
        "selftext_html": "<p>Discussion context.</p>",
    }))

    assert post is not None
    assert post.url == PERMALINK
    assert post.html == (
        '<p>Discussion context.</p>\n<p><a href="'
        'https://example.com/release?a=1&amp;b=&quot;two&quot;">'
        'https://example.com/release?a=1&amp;b=&quot;two&quot;</a></p>'
    )


@pytest.mark.parametrize("url", [
    "javascript:alert(1)", "data:text/html,unsafe", "//example.com/path",
    "https://[invalid", "https://example.com:bad/path", "", None,
])
def test_convert_reddit_rejects_invalid_destination_urls(url: object) -> None:
    post = convert_raw_reddit_post(_raw_post({"title": "Title", "url": url}))

    assert post is not None
    assert post.html == ""


def test_convert_reddit_images_use_downloaded_asset_urls_without_duplicates(
) -> None:
    image_url = "https://i.redd.it/image.png?x=1&y=2"
    post = convert_raw_reddit_post(_raw_post({
        "title": "Gallery",
        "selftext_html": f'<p><img src="{image_url}"></p>',
        "media": [
            {"url": image_url, "asset_type": "photo"},
            {"url": "https://i.redd.it/second.png", "asset_type": "photo"},
            {"url": "https://i.redd.it/second.png", "asset_type": "photo"},
            {"url": "javascript:alert(1)", "asset_type": "photo"},
            {"url": "https://v.redd.it/video.mp4", "asset_type": "video"},
            None,
        ],
    }))

    assert post is not None
    assert post.html.count("<img ") == 2
    assert 'src="https://i.redd.it/second.png"' in post.html
    assert "javascript:" not in post.html
    assert "<video" not in post.html


@pytest.mark.parametrize("payload", [
    "invalid JSON", "null", "[]", "123", "{}",
    '{"title": 123}', '{"title": "   "}',
])
def test_convert_reddit_rejects_malformed_or_untitled_posts(
    payload: str,
) -> None:
    raw = _raw_post({})._replace(payload=payload)

    assert convert_raw_reddit_post(raw) is None


@pytest.mark.parametrize("created_utc", [None, "invalid", True, 1e100, -1e100])
def test_convert_reddit_invalid_timestamps_use_capture_time(
    created_utc: object,
) -> None:
    post = convert_raw_reddit_post(_raw_post({
        "title": "Timestamp fallback", "created_utc": created_utc,
    }))

    assert post is not None
    assert post.created_at == SCRAPED_AT


def test_convert_reddit_uses_record_creation_time_before_payload() -> None:
    created_at = SCRAPED_AT - datetime.timedelta(hours=2)
    raw = _raw_post({"title": "Title", "created_utc": 1})._replace(
        post_created_at=created_at,
    )

    post = convert_raw_reddit_post(raw)

    assert post is not None
    assert post.created_at == created_at
    assert post.updated_at == SCRAPED_AT


def test_convert_reddit_preserves_plain_text_entities_in_normalized_title(
) -> None:
    post = convert_raw_reddit_post(_raw_post({
        "title": "How to write &lt;script&gt; and &amp; entities",
        "author": "alice",
    }))

    assert post is not None
    assert post.title == "How to write &lt;script&gt; and &amp; entities"
