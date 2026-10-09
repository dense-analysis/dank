import datetime
import html
import json

import pytest

from dank.html_utils import html_text
from dank.model import RawPost
from dank.process.feed_html import recover_feed_html
from dank.process.rss import convert_raw_post


def test_recovers_paragraphs_and_inline_markup_without_page_noise() -> None:
    feed = "First linked paragraph. A & B Next steps Final paragraph."
    page = (
        '<main><article><header><p>By Example Writer</p></header>'
        '<div class="bodytext"><div><h3>Read more</h3>'
        '<ul><li>Unrelated news</li></ul></div>'
        '<p>First <a href="/source">linked</a> paragraph.</p>'
        '<script>tracking()</script><p>A&nbsp;&amp; B</p>'
        '<div><p>Advertisement</p></div><h3>Next steps</h3>'
        '<p><em>Final</em> paragraph.</p></div>'
        '<footer><p>Related news</p></footer></article></main>'
    )
    recovered = recover_feed_html(feed, page)

    assert recovered == (
        '<p>First <a href="/source">linked</a> paragraph.</p>\n'
        '<p>A\xa0&amp; B</p>\n<h3>Next steps</h3>\n'
        '<p><em>Final</em> paragraph.</p>'
    )
    assert " ".join(html_text(recovered).split()) == feed


def test_keeps_nested_quotes_lists_tables_and_escaped_code() -> None:
    body = (
        '<blockquote><p>A quoted <strong>statement</strong>.</p>'
        '<p>Continued.</p></blockquote>'
        '<ol><li>First item</li><li>Second item</li></ol>'
        '<table><tr><td>A</td><td>B</td></tr></table>'
        '<pre>&lt;script&gt;example()&lt;/script&gt;</pre>'
        '<p>Final<br/>line<img src="/image.png" alt=""></p>'
    )
    text = " ".join(html_text(body).split())
    feed = html.escape(text, quote=False)
    recovered = recover_feed_html(feed, f"<article>{body}</article>")

    assert "<blockquote><p>" in recovered
    assert "<ol><li>" in recovered
    assert "<table><tr><td>" in recovered
    assert "&lt;script&gt;example()&lt;/script&gt;" in recovered
    assert '<br />' in recovered
    assert '<img src="/image.png" alt="">' in recovered
    assert " ".join(html_text(recovered).split()) == text


@pytest.mark.parametrize("media", [
    '<figure><div><picture><source srcset="/large.webp 2x">'
    '<img src="/diagram.webp" alt="Diagram"></picture></div>'
    '<figcaption>Screenshot credit: Example Lab</figcaption></figure>',
    '<picture><source srcset="/large.webp 2x">'
    '<img src="/diagram.webp" alt="Diagram"></picture>',
    '<img src="/diagram.webp" alt="Diagram">',
    '<img src="/diagram.webp" alt="Diagram" />',
])
def test_recovers_media_between_matching_sibling_paragraphs(
    media: str,
) -> None:
    page = '<article><p>Before.</p>' + media + '<p>After.</p></article>'
    recovered = recover_feed_html("Before. After.", page)

    assert 'src="/diagram.webp"' in recovered
    assert recovered.index("Before.") < recovered.index("/diagram.webp")
    assert recovered.index("/diagram.webp") < recovered.index("After.")

    if "figcaption" in media:
        assert '<figcaption>Screenshot credit: Example Lab</figcaption>' in (
            recovered
        )


@pytest.mark.parametrize("page", [
    '<img src="/outside.png"><p>Before.</p><p>After.</p>',
    '<p>Before.</p><p>After.</p><img src="/outside.png">',
    '<p>Before.</p><aside><figure><img src="/outside.png">'
    '</figure></aside><p>After.</p>',
    '<div><p>Before.</p></div><div><img src="/outside.png">'
    '</div><div><p>After.</p></div>',
    '<p>Before.</p><p>Unrelated news.</p><img src="/outside.png">'
    '<p>After.</p>',
    '<p>Before.</p><img src="/outside.png"><p>Unrelated news.</p>'
    '<p>After.</p>',
    '<p>Before.</p><img src="/outside.png" width="1" height="1">'
    '<p>After.</p>',
    '<p>Before.</p><img src="/outside.png" hidden><p>After.</p>',
])
def test_media_outside_the_matched_passage_is_not_imported(page: str) -> None:
    recovered = recover_feed_html(
        "Before. After.", f"<article>{page}</article>",
    )

    assert recovered == '<p>Before.</p>\n<p>After.</p>'


def test_figure_caption_already_in_feed_is_not_duplicated() -> None:
    page = '<article><p>Before.</p><figure><img src="/diagram.png">' \
           '<figcaption>Credit.</figcaption></figure><p>After.</p></article>'
    recovered = recover_feed_html("Before. Credit. After.", page)

    assert recovered.count("Credit.") == 1
    assert recovered.count('<img') == 1
    assert " ".join(html_text(recovered).split()) == "Before. Credit. After."


def test_media_cannot_rescue_an_incomplete_feed_match() -> None:
    assert recover_feed_html(
        "Before. After.",
        '<article><p>Before.</p><figure><img src="/diagram.png">'
        '<figcaption>Credit.</figcaption></figure><p>Changed.</p></article>',
    ) == ""


@pytest.mark.parametrize("feed", [
    '<p>First.</p><p>Second.</p>',
    '<a href="/feed-link">First.</a> Second.',
    '<img src="/feed-image.png">First. Second.',
    'First.<br>Second.',
])
def test_existing_feed_markup_is_authoritative(feed: str) -> None:
    assert recover_feed_html(
        feed, '<article><p>First.</p><p>Second.</p></article>',
    ) == ""


@pytest.mark.parametrize(("feed", "page"), [
    ("", "<article><p>First.</p><p>Second.</p></article>"),
    ("First. Second.", ""),
    ("A comment.", "<article><p>Parent article.</p></article>"),
    ("First. Second.", "<article><p>First.</p></article>"),
    ("First. Second.", "<article><p>Second.</p><p>First.</p></article>"),
    ("First. Second.", "<article><p>First.</p><p>Changed.</p></article>"),
    ("First. Second.", "<article><p>First. Second.</p></article>"),
    ("First. Second.", "<article><p>First.</p><p>Second.</article>"),
    ("First. Second.", "<article><p><em>First.</p><p>Second.</p></article>"),
    ("First. Second.", "<article><p>Fir</p><p>st. Second.</p></article>"),
])
def test_incomplete_or_unreliable_matches_keep_the_feed(
    feed: str, page: str,
) -> None:
    assert recover_feed_html(feed, page) == ""


@pytest.mark.parametrize("feed_xml", [
    '<item xmlns:content="http://purl.org/rss/1.0/modules/content/">'
    '<title>Example</title><content:encoded>First paragraph. Next steps '
    'Final paragraph.</content:encoded></item>',
    '<entry xmlns="http://www.w3.org/2005/Atom"><title>Example</title>'
    '<content type="text">First paragraph. Next steps '
    'Final paragraph.</content></entry>',
])
def test_full_feed_processing_recovers_structure_and_resolves_links(
    feed_xml: str,
) -> None:
    payload = json.dumps({
        "feed_xml": feed_xml,
        "page_html": '<head><base href="/assets/"></head><article>'
        '<p>First <a href="source">paragraph</a>.</p><h3>Next steps</h3>'
        '<p>Final paragraph.</p></article>',
        "page_final_url": "https://publisher.test/story/",
    })
    now = datetime.datetime(2026, 10, 9, tzinfo=datetime.UTC)
    raw = RawPost(
        domain="example.test", post_id="1", url="https://example.test/1",
        post_created_at=now, scraped_at=now, source="rss",
        request_url="https://example.test/feed", payload=payload,
    )
    post = convert_raw_post(raw)

    assert post is not None
    assert post.html == (
        '<p>First <a href="https://publisher.test/assets/source">paragraph'
        '</a>.</p>\n<h3>Next steps</h3>\n<p>Final paragraph.</p>'
    )
    assert post.url == raw.url
    assert post.post_id == raw.post_id
    assert post.created_at == now
    assert post.updated_at == now
    assert raw.payload == payload
