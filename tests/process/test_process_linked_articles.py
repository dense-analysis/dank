import datetime
import json

from dank.model import RawPost
from dank.process.rss import convert_raw_post


def test_linked_article_extracts_publisher_date_author_and_body() -> None:
    scraped_at = datetime.datetime(2026, 10, 17, tzinfo=datetime.UTC)
    raw = RawPost(
        domain="publisher.test", post_id="saved-article",
        url="https://publisher.test/article", post_created_at=None,
        scraped_at=scraped_at, source="linked_article",
        request_url="https://short.test/go",
        payload=json.dumps({
            "feed_xml": "<item><title>Linked title</title></item>",
            "page_final_url": "https://publisher.test/posts/article",
            "page_html": '<head><meta name="author" '
            'content="Publisher author">'
            '<meta property="article:published_time" '
            'content="2026-10-12T13:14:15Z"></head>'
            '<nav>Unrelated navigation</nav><article><p>Actual article.</p>'
            '<img src="image.jpg"></article>',
            "linked_article": {"status": "captured"},
        }),
    )
    post = convert_raw_post(raw)

    assert post is not None
    assert post.author == "Publisher author"
    assert post.created_at == datetime.datetime(
        2026, 10, 12, 13, 14, 15, tzinfo=datetime.UTC,
    )
    assert post.updated_at == scraped_at
    assert post.source == "linked_article"
    assert "Actual article." in post.html
    assert "Unrelated navigation" not in post.html
    assert 'src="https://publisher.test/posts/image.jpg"' in post.html


def test_failed_linked_article_has_no_fabricated_author_or_body() -> None:
    scraped_at = datetime.datetime(2026, 10, 17, tzinfo=datetime.UTC)
    raw = RawPost(
        "publisher.test", "failed-article", "https://publisher.test/article",
        None, scraped_at, "linked_article", "https://publisher.test/article",
        json.dumps({
            "feed_xml": "<item><title>Known title</title></item>",
            "page_html": "", "page_fetch_status": "failed",
            "linked_article": {"status": "failed"},
        }),
    )
    post = convert_raw_post(raw)

    assert post is not None
    assert post.title == "Known title"
    assert post.author == ""
    assert post.html == ""
