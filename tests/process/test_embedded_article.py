import datetime
import json
from pathlib import Path
from typing import Any

import pytest

from dank.html_utils import html_text
from dank.model import RawPost
from dank.process.page import extract_article_html, extract_page_metadata
from dank.process.rss import convert_raw_post
from dank.web.app import _sanitize_html  # pyright: ignore[reportPrivateUsage]

PAGE = (Path(__file__).parents[1] / "fixtures"
        / "embedded-blog.html").read_text()


def _page_data() -> dict[str, Any]:
    start = PAGE.index('{\n    "page"')
    end = PAGE.index("</script>", start)

    return json.loads(PAGE[start:end])


def _script(data: object) -> str:
    return ('<script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(data).replace("<", "\\u003c") + '</script>')


def test_recovers_only_the_current_blog_article_with_formatting() -> None:
    body = extract_article_html(PAGE)

    assert body.startswith('<P>Developers use Linux desktops')
    assert '<H2>Protect the workstation</H2>' in body
    assert '<LI>Install protection.</LI>' in body
    assert 'href="/guide"' in body
    assert '<figcaption>Deployment options in the portal.</figcaption>' in body
    assert '&lt;script&gt;example()&lt;/script&gt;' in body
    assert not any(text in body for text in (
        'different article', 'reader comment', 'site footer', 'raw draft',
        'applicationStartup', 'summary is not',
    ))
    assert extract_page_metadata(PAGE).author == "A Writer"


@pytest.mark.parametrize("container", ["article", "main"])
def test_existing_rendered_article_remains_preferred(container: str) -> None:
    page = PAGE.replace(
        '<div id="__next">',
        f'<{container}><p>Rendered article.</p></{container}>'
        '<div id="__next">',
    )

    assert extract_article_html(page) == '<p>Rendered article.</p>'


@pytest.mark.parametrize("container", ["article", "main"])
@pytest.mark.parametrize("shell", [
    '<div data-testid="Loader"></div>',
    '<script>loadArticle()</script>',
    '<p>&nbsp;</p>',
])
def test_empty_rendered_shell_does_not_hide_embedded_article(
    container: str, shell: str,
) -> None:
    page = PAGE.replace(
        '<div id="__next">',
        f'<{container}>{shell}</{container}><div id="__next">',
    )

    assert extract_article_html(page) == extract_article_html(PAGE)


def test_media_only_article_is_preserved_without_embedded_recovery() -> None:
    body = '<figure><img src="/diagram.png" alt="A diagram"></figure>'
    page = f'<article>{body}</article>' + _script({"unrelated": "data"})

    assert extract_article_html(page) == body


@pytest.mark.parametrize("data", [
    None, [], {}, {"props": []}, {"query": {"messageId": "4560822"}},
])
def test_ignores_unrecognized_embedded_data(data: object) -> None:
    assert extract_article_html(_script(data)) == ""


@pytest.mark.parametrize("field, value", [
    ("page", "/blogs/BlogLandingPage"),
    ("query", {"messageId": "999999"}),
    ("query", {"messageId": 4560822}),
    ("query", {"messageId": ["4560822"]}),
    ("query", {"messageId": ""}),
    ("query", {"messageId": "../../4560822"}),
    ("props", {"pageProps": {"apolloState": []}}),
])
def test_requires_a_blog_page_with_a_matching_cache_record(
    field: str, value: object,
) -> None:
    data = _page_data()
    data[field] = value

    assert extract_article_html(_script(data)) == ""


@pytest.mark.parametrize("field, value", [
    ("__typename", "BlogReplyMessage"),
    ("id", "message:999"),
    ("entityType", "BLOG_REPLY"),
    ("body", None), ("body", {}), ("body", ""),
    ("body", "<p>&nbsp;</p><script>startup()</script>"),
])
def test_rejects_wrong_or_empty_bodies(field: str, value: object) -> None:
    data = _page_data()
    data['props']['pageProps']['apolloState'][
        'BlogTopicMessage:message:4560822'
    ][field] = value

    assert extract_article_html(_script(data)) == ""


@pytest.mark.parametrize("attributes", [
    'id="__NEXT_DATA__" type="text/javascript"',
    'id="other-state" type="application/json"',
    'type="application/json"',
])
def test_only_reads_the_known_json_script(attributes: str) -> None:
    page = f'<script {attributes}>{json.dumps(_page_data())}</script>'

    assert extract_article_html(page) == ""


@pytest.mark.parametrize("block", [
    '{broken json', 'window.state = {};', '[' * 2000 + ']' * 2000,
])
def test_malformed_json_does_not_break_processing(block: str) -> None:
    script = ('<script id="__NEXT_DATA__" type="application/json">'
              + block + '</script>')

    assert extract_article_html(script) == ""
    assert extract_article_html(script + PAGE).startswith('<P>Developers')


def test_rss_processing_resolves_urls_and_preserves_the_sanitizer_boundary(
) -> None:
    data = _page_data()
    record = data['props']['pageProps']['apolloState'][
        'BlogTopicMessage:message:4560822'
    ]
    record['body'] += (
        '<script>attack()</script><p onclick="attack()">Visible text.</p>'
        '<a href="javascript:attack()">Unsafe link</a>'
    )
    payload = json.dumps({
        'feed_xml': ('<item><title>Feed title</title>'
                     '<author>Feed author</author>'
                     '<description>Summary.</description></item>'),
        'page_html': _script(data),
        'page_final_url': 'https://publisher.test/posts/story/',
    })
    raw = RawPost(
        domain='feed.test', post_id='post-1', url='https://short.test/story',
        post_created_at=datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC),
        scraped_at=datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC),
        source='rss', request_url='https://feed.test/rss', payload=payload,
    )
    post = convert_raw_post(raw)

    assert post is not None
    assert (post.title, post.author) == ('Feed title', 'Feed author')
    assert post.post_id == raw.post_id
    assert post.created_at == raw.post_created_at
    assert 'href="https://publisher.test/guide"' in post.html
    assert ('src="https://publisher.test/posts/story/images/portal.png"'
            in post.html)
    assert '<script>' not in post.html
    assert raw.payload == payload
    rendered = _sanitize_html(post.html)
    assert 'onclick' not in rendered and 'javascript:' not in rendered
    assert ('<figcaption>Deployment options in the portal.</figcaption>'
            in rendered)
    assert '&lt;script&gt;example()&lt;/script&gt;' in rendered
    assert 'Visible text.' in html_text(rendered)
