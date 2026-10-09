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


@pytest.mark.parametrize("wrapper", ["div", "figure"])
@pytest.mark.parametrize("base_url", [None, "https://example.test/story"])
def test_wordpress_captions_become_semantic_figures(
    wrapper: str, base_url: str | None,
) -> None:
    document = (
        f'<{wrapper} class="aligncenter wp-caption" id="attachment_1">'
        '<div><a href="/image.png"><picture><source srcset="/large.png 2x">'
        '<img src="/image.png" alt="Diagram" /></picture></a></div>'
        '<p class="wp-caption-text" id="caption-attachment-1">'
        'Credit: <a href="/credit">A &amp; B</a><br>More detail.</p>'
        f'</{wrapper}><p>Following article paragraph.</p>'
    )
    cleaned = remove_page_noise(document, base_url=base_url)
    assert cleaned.startswith('<figure class="aligncenter wp-caption"')
    assert '<figcaption class="wp-caption-text"' in cleaned
    assert 'More detail.</figcaption></figure><p>Following' in cleaned
    prefix = "https://example.test" if base_url else ""
    assert f'href="{prefix}/credit">A &amp; B</a>' in cleaned
    assert f'src="{prefix}/image.png"' in cleaned
    assert html_text(cleaned).split() == html_text(document).split()
    assert remove_page_noise(cleaned, base_url=base_url) == cleaned


@pytest.mark.parametrize("document", [
    '<div><img src="/image.png"><p>Normal article text.</p></div>',
    '<div class="wp-caption-other"><img src="/image.png">'
    '<p class="wp-caption-text">Not a caption container.</p></div>',
    '<p class="wp-caption-text">An orphan paragraph.</p>',
    '<div class="wp-caption"><img src="/image.png">'
    '<p class="wp-caption-text-other">Normal article text.</p></div>',
    '<div class="wp-caption"><div><p class="wp-caption-text">'
    'Unrelated nested text.</p></div></div>',
])
def test_caption_normalization_does_not_guess_from_image_adjacency(
    document: str,
) -> None:
    cleaned = remove_page_noise(document)
    assert '<figcaption' not in cleaned
    assert html_text(cleaned).split() == html_text(document).split()


def test_native_figures_and_escaped_markup_stay_intact() -> None:
    document = (
        '<figure><img src="/image.png"><figcaption>Existing credit.'
        '</figcaption></figure><pre>&lt;div class="wp-caption"&gt;'
        '&lt;p class="wp-caption-text"&gt;Code example&lt;/p&gt;'
        '&lt;/div&gt;</pre>'
    )
    assert remove_page_noise(document) == document


def test_figure_text_keeps_boundaries_without_source_whitespace() -> None:
    assert html_text(
        'Before<figure><img src="/image.png"><figcaption>Credit'
        '</figcaption></figure>After',
    ).split() == ['Before', 'Credit', 'After']


def test_wordpress_captions_preserve_sanitizer_boundary() -> None:
    document = (
        '<div class="wp-caption" style="width:600px" onclick="attack()">'
        '<img src="/image.png" onerror="attack()">'
        '<p class="wp-caption-text" id="credit" style="position:fixed">'
        'Credit: <a href="javascript:attack()">unsafe</a> '
        '<em>A &amp; B</em><script>attack()</script></p></div>'
    )
    assert _sanitize_html(document) == (
        '<figure><img src="/image.png"><figcaption>Credit: '
        '<a>unsafe</a> <em>A &amp; B</em></figcaption></figure>'
    )
