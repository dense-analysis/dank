from __future__ import annotations

import datetime as dt
import html
import json
from typing import Any

import pytest

from dank.youtube_reference_html import content_links
from dank.youtube_references import reference_rows

VIDEO = "NnV_cWeoo5Q"
URL = f"https://www.youtube.com/watch?v={VIDEO}"
STAMP = dt.datetime(2026, 10, 16, tzinfo=dt.UTC)


def raw(
    source: str, payload: dict[str, Any], post_id: str = "123",
) -> dict[str, Any]:
    return {
        "source": source,
        "domain": source + ".example",
        "post_id": post_id,
        "url": "https://example.com/post/123",
        "scraped_at": STAMP,
        "payload": json.dumps(payload),
    }


def test_post_and_comment_are_separate_occurrences_with_exact_evidence():
    row = raw(
        "reddit",
        {
            "title": "Game demo",
            "url": URL,
            "comment_capture": {
                "comments": [
                    {
                        "id": "t1_ph4vac2",
                        "parent_id": "t3_1x7ijzc",
                        "author": "James20k",
                        "body_html": (
                            "<p>Unrelated introduction. "
                            + "Context. " * 60
                            + "</p>"
                            '<p>Kernel developers have <a href="https://youtu.be/'
                            + VIDEO
                            + '?t=1468">seen it happen in real time</a>.</p>'
                        ),
                    },
                ],
            },
        },
        "t3_1x7ijzc",
    )
    links = reference_rows(row, raw=True)
    assert {r["kind"] for r in links} == {"post", "comment"}
    mention = next(r for r in links if r["kind"] == "comment")
    assert mention["author"] == "James20k"
    assert mention["matched_url"].endswith("?t=1468")
    assert mention["reference_url"].endswith("/1x7ijzc/comment/ph4vac2/")
    assert "seen it happen in real time" in mention["excerpt"]
    assert "Unrelated introduction" not in mention["excerpt"]
    assert "<a" not in mention["excerpt"]
    assert mention["observed_at"] == STAMP


def test_hacker_news_comment_requires_ancestry_to_the_story():
    row = raw(
        "hacker_news",
        {
            "story": {"title": "Kernel statistics"},
            "comments": [
                {"id": 456, "parent": 123, "text": "Parent"},
                {
                    "id": 789,
                    "parent": 456,
                    "by": "zdw",
                    "text": (
                        '<a href="https:&#x2F;&#x2F;www.youtube.com'
                        '&#x2F;watch?v='
                        + VIDEO
                        + '">Greg KH talk</a>'
                    ),
                },
                {"id": 999, "parent": 987, "text": URL},
                {"id": 987, "parent": 999, "text": URL},
                {"id": 1000, "parent": 123, "deleted": True, "text": URL},
            ],
        },
    )
    links = reference_rows(row, raw=True)
    assert len(links) == 1
    assert (
        links[0]["reference_url"] == "https://news.ycombinator.com/item?id=789"
    )
    assert links[0]["kind"] == "comment"
    assert links[0]["excerpt"] == "Greg KH talk"


@pytest.mark.parametrize(
    "content",
    [
        f'<script>"{URL}"</script>',
        f'<nav><a href="{URL}">Ad</a></nav>',
        f'<div class="related-posts"><a href="{URL}">Related</a></div>',
        f'<div data-link="{URL}">Metadata</div>',
        f'<a href="https://example.com/?redirect={URL}">Other site</a>',
        f'<a href="https://example.com/">{URL}</a>',
        f'<div hidden><a href="{URL}">Hidden</a></div>',
        URL + "TOOLONG",
        URL + "&v=abcdefghijk",
        "https://youtube.com.evil.example/watch?v=" + VIDEO,
    ],
)
def test_unrelated_metadata_and_invalid_destinations_are_not_evidence(
    content: str,
):
    assert content_links(content) == []


def test_x_uses_owned_text_entities_not_quote_or_profile_urls():
    row = raw(
        "x",
        {
            "legacy": {
                "full_text": "Watch this https://t.co/video",
                "entities": {
                    "urls": [
                        {"url": "https://t.co/video", "expanded_url": URL},
                    ],
                },
            },
            "quoted_status_result": {"result": {"url": URL + "x"}},
            "profile": {"url": "https://youtu.be/abcdefghijk"},
        },
    )
    links = reference_rows(row, raw=True)
    assert [r["video_id"] for r in links] == [VIDEO]
    row["payload"] = json.dumps(
        {
            "legacy": {"full_text": "A quote"},
            "quoted_status_result": {"result": {"url": URL}},
        },
    )
    assert reference_rows(row, raw=True) == []


@pytest.mark.parametrize("source", ["rss", "linked_article"])
def test_articles_use_owned_scope_and_ignore_other_articles(source: str):
    row = raw(
        source,
        {
            "feed_xml": "<item><title>Actual article</title></item>",
            "page_html": (
                f'<nav><a href="https://youtu.be/abcdefghijk">Menu</a></nav>'
                f'<article><p>Watch <a href="{URL}">the talk</a>.</p>'
                '<div id="comments"><a href="https://youtu.be/abcdefghijk">'
                "Comment</a></div></article>"
                '<article><a href="https://youtu.be/abcdefghijk">Other</a></article>'
            ),
        },
    )
    assert [r["video_id"] for r in reference_rows(row, raw=True)] == [VIDEO]
    row["url"] += "#comment-123"
    assert reference_rows(row, raw=True) == []


@pytest.mark.parametrize(
    "embed",
    [
        f'<iframe src="{URL.replace("watch?v=", "embed/")}"></iframe>',
        f'<iframe data-src="{URL.replace("watch?v=", "embed/")}"></iframe>',
        '<div data-component="youtube-facade" data-src="'
        + URL.replace("watch?v=", "embed/")
        + '"></div>',
        '<iframe srcdoc="'
        + html.escape(f'<iframe src="{URL}"></iframe>', quote=True)
        + '"></iframe>',
    ],
)
def test_real_embeds_have_evidence_and_nearby_context(embed: str):
    links = content_links(f"<p>The demo:</p>{embed}")
    assert len(links) == 1
    assert links[0].video_id == VIDEO
    assert "The demo:" in links[0].excerpt


def test_reddit_raw_page_and_crosspost_metadata_are_not_the_posts_link():
    row = raw(
        "reddit",
        {
            "title": "Crosspost",
            "raw": {"html": URL},
            "crosspost_parent_list": [{"url": URL}],
        },
    )
    assert reference_rows(row, raw=True) == []


@pytest.mark.parametrize("source", ["rss", "linked_article"])
def test_feed_html_and_plain_text_links_preserve_case_and_timestamp(
    source: str,
):
    body = f'<p>Watch <a href="{URL}&amp;t=33">this</a></p>'
    row = raw(
        source,
        {
            "feed_xml": "<item><description>"
            + html.escape(body)
            + "</description></item>",
        },
    )
    assert reference_rows(row, raw=True)[0]["matched_url"] == URL + "&t=33"
    assert content_links(f"See ({URL}).", markup=False)[0].video_id == VIDEO


@pytest.mark.parametrize("source", ["rss", "linked_article"])
def test_article_comments_keep_permalink_and_owned_reply(source: str):
    row = raw(source, {"page_html": (
        '<article><p>No video in the article</p></article>'
        '<li id="comment-1"><div class="comment">Parent without a video</div>'
        '<ul><li id="comment-2"><div class="comment">'
        f'Watch <a href="{URL}">the talk</a></div></li></ul></li>'
    )})
    links = reference_rows(row, raw=True)
    assert len(links) == 1
    assert links[0]["kind"] == "comment"
    assert links[0]["reference_id"] == "comment-2"
    assert links[0]["reference_url"].endswith("#comment-2")
    assert links[0]["excerpt"] == "Watch the talk"
    row["url"] += "#comment-1"
    assert reference_rows(row, raw=True) == []


def test_primary_article_slots_and_authored_asides_remain_attributable():
    for page in (
        '<single-page><template v-slot:content><single-video>'
        f'<template v-slot:video><iframe src="{URL}"></iframe></template>'
        '</single-video></template><template v-slot:leaderboard>'
        '<a href="https://youtu.be/abcdefghijk">Ad</a></template></single-page>',
        '<main><div class="paperBody"><aside>Watch '
        f'<a href="{URL}">this presentation</a></aside></div></main>',
    ):
        links = reference_rows(raw("rss", {"page_html": page}), raw=True)
        assert [link["video_id"] for link in links] == [VIDEO]


@pytest.mark.parametrize("source", ["rss", "linked_article"])
def test_processed_articles_keep_publisher_identity_and_video_evidence(
    source: str,
):
    row = {
        "source": source, "domain": "publisher.example", "post_id": "article",
        "url": "https://publisher.example/article", "updated_at": STAMP,
        "title": "Publisher title", "author": "Article author",
        "html": f'<p>Watch <a href="{URL}">the talk</a>.</p>',
    }
    evidence = reference_rows(row, raw=False)
    assert len(evidence) == 1
    assert evidence[0]["source"] == source
    assert evidence[0]["domain"] == "publisher.example"
    assert evidence[0]["title"] == "Publisher title"
    assert evidence[0]["kind"] == "post"
    assert evidence[0]["reference_url"] == row["url"]
    assert evidence[0]["matched_url"] == URL
    assert "Watch the talk" in evidence[0]["excerpt"]
