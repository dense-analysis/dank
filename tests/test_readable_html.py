import pathlib

import pytest

from dank.html_utils import html_base_url, html_text, remove_page_noise
from dank.web.app import (
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)

HTML = (
    '<html><head><title>Metadata title</title><style>.tracking{}</style>'
    '<script>headTracker()</script></head><body>'
    '<p>Real &amp; readable <strong>content</strong>.</p>'
    '<script>bodyTracker()</script><style>.inlineTracking{}</style>'
    '<pre>&lt;script&gt;example()&lt;/script&gt;</pre>'
    '</body></html>'
)


def test_noise_removal_preserves_article_markup_and_escaped_code() -> None:
    cleaned = remove_page_noise(HTML)
    assert '<strong>content</strong>' in cleaned
    assert '&lt;script&gt;example()&lt;/script&gt;' in cleaned
    assert 'Tracker' not in cleaned and 'Tracking' not in cleaned
    assert 'Metadata title' not in cleaned


def test_plain_text_keeps_word_and_paragraph_boundaries() -> None:
    text = html_text('<p>per<strong>form</strong>ance</p><p>Next line.</p>')
    assert text.split() == ['performance', 'Next', 'line.']


def test_preview_and_detail_do_not_display_styles_or_tracking_code() -> None:
    summary = _summarize_html(HTML)
    assert summary == 'Real & readable content. <script>example()</script>'
    cleaned = _sanitize_html(HTML)
    assert 'Tracker' not in cleaned and 'tracking' not in cleaned
    assert '&lt;script&gt;' in cleaned


def test_implicit_end_of_head_does_not_hide_the_body() -> None:
    assert html_text('<head><title>Hidden</title><body>Visible</body>') == (
        'Visible'
    )


@pytest.mark.parametrize("base, expected", [
    ('<base href="https://cdn.test/assets/">', 'https://cdn.test/assets/'),
    ('<base href="../assets/" />', 'https://blog.test/assets/'),
    ('<base href="//cdn.test/assets/">', 'https://cdn.test/assets/'),
    ('<base href="">', 'https://blog.test/posts/article'),
    ('<base href="javascript:bad">', 'https://blog.test/posts/article'),
    ('<base href="http://[">', 'https://blog.test/posts/article'),
    ('<base href="https://bad.test:no/path">', 'https://blog.test/posts/article'),
    ('', 'https://blog.test/posts/article'),
])
def test_html_base_uses_valid_first_href(base: str, expected: str) -> None:
    document = f'<head>{base}</head><body>Article</body>'
    actual = html_base_url(document, 'https://blog.test/posts/article')
    assert actual == expected


def test_only_first_base_href_controls_document_links() -> None:
    document = ('<head><base target="_blank"><base href="/first/">'
                '<base href="https://other.test/"></head>')
    assert html_base_url(document, 'https://blog.test/post') == (
        'https://blog.test/first/'
    )


def test_resolved_links_preserve_entities_code_and_non_http_links() -> None:
    document = (
        '<p><a href="../more?q=1&amp;b=2" title="A &amp; B">Next</a>'
        '<img src="/cover.png" alt="A &quot;quote&quot;" />'
        '<video controls src="clip.mp4" poster="poster.jpg"></video>'
        '<a href="mailto:person@example.test">Mail</a>'
        '<pre>&lt;img src="example.png"&gt;</pre></p>'
    )
    result = remove_page_noise(
        document, base_url='https://blog.test/posts/article/',
    )
    assert 'href="https://blog.test/posts/more?q=1&amp;b=2"' in result
    assert 'title="A &amp; B"' in result
    assert 'src="https://blog.test/cover.png"' in result
    assert 'alt="A &quot;quote&quot;"' in result
    assert ('<video controls src="https://blog.test/posts/article/clip.mp4"'
            in result)
    assert 'poster="https://blog.test/posts/article/poster.jpg"' in result
    assert 'href="mailto:person@example.test"' in result
    assert '&lt;img src="example.png"&gt;' in result


@pytest.mark.parametrize("tag", ["pre", "code"])
def test_code_language_metadata_survives_sanitizing(tag: str) -> None:
    document = (
        f'<{tag} class="language-sql" data-lang="sql" data-language="sql"'
        ' style="color:red" onclick="alert(1)" data-unrelated="discard">'
        'SELECT &lt;example&gt; &amp; 1;'
        f'</{tag}>'
    )

    assert _sanitize_html(document) == (
        f'<{tag} class="language-sql" data-lang="sql" data-language="sql">'
        'SELECT &lt;example&gt; &amp; 1;'
        f'</{tag}>'
    )


def test_language_metadata_is_not_allowed_on_other_elements() -> None:
    document = '<p data-lang="sql" data-language="sql">Article body</p>'

    assert _sanitize_html(document) == '<p>Article body</p>'


def test_standard_table_structure_survives_sanitizing() -> None:
    document = (pathlib.Path(__file__).parent / "fixtures"
                / "reader-table.html").read_text()
    cleaned = _sanitize_html(document)
    for markup in (
        '<caption>Benchmark comparison</caption>',
        '<colgroup span="2">', '<col span="4">', '<thead>', '<tbody>',
        '<tfoot>', '<th colspan="2" scope="colgroup">',
        '<th rowspan="2" scope="rowgroup">',
        '<th scope="col" abbr="Alpha">', '<td colspan="6">',
        '<em>Tools benchmark</em>',
    ):
        assert markup in cleaned

    assert html_text(cleaned).split() == html_text(document).split()
    assert "onclick" not in cleaned and "style=" not in cleaned


@pytest.mark.parametrize("tag", ["td", "th"])
@pytest.mark.parametrize("rowspan", ["0", "2"])
def test_table_cell_spans_are_preserved(tag: str, rowspan: str) -> None:
    document = (
        f'<table><tbody><tr><{tag} colspan="2" rowspan="{rowspan}"'
        f' onclick="attack()">Cell</{tag}></tr></tbody></table>'
    )
    result = _sanitize_html(document)
    assert f'<{tag} colspan="2" rowspan="{rowspan}">' in result
    assert "onclick" not in result


def test_table_attributes_are_not_globally_allowed() -> None:
    document = '<p colspan="2" rowspan="2" scope="row">Text</p>'
    assert _sanitize_html(document) == '<p>Text</p>'
