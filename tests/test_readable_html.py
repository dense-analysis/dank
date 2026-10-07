from dank.html_utils import html_text, remove_page_noise
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
