import datetime
import xml.etree.ElementTree as ElementTree

from dank.model import RawPost
from dank.process.rss import convert_raw_post

ATOM_XML = r"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
    <title>Example Feed</title>
    <link href="https://example.com/"/>
    <updated>2026-02-01T00:00:00+00:00</updated>
    <id>https://example.com/</id>
    <entry>
        <title>Atom Post</title>
        <link href="https://example.com/atom-post"/>
        <id>atom-1</id>
        <updated>2026-02-01T01:00:00+00:00</updated>
        <summary>Summary.</summary>
    </entry>
</feed>
"""

RSS2_XML = r"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
    <channel>
        <title>RSS2 Feed</title>
        <link>https://example.com/</link>
        <description>Example</description>
        <item>
            <title>RSS2 Post</title>
            <link>https://example.com/rss2-post</link>
            <guid>rss2-1</guid>
            <pubDate>Sun, 01 Feb 2026 01:00:00 GMT</pubDate>
            <description>RSS2 Summary.</description>
        </item>
    </channel>
</rss>
"""

RSS1_XML = r"""<?xml version="1.0"?>
<rdf:RDF
xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
xmlns="http://purl.org/rss/1.0/">
    <channel rdf:about="https://example.com/">
        <title>RSS1 Feed</title>
        <link>https://example.com/</link>
        <description>Example</description>
    </channel>
    <item rdf:about="https://example.com/rss1-post">
        <title>RSS1 Post</title>
        <link>https://example.com/rss1-post</link>
    </item>
</rdf:RDF>
"""


def _raw_xml(
    xml: str,
    path: str,
    namespaces: dict[str, str] | None = None,
) -> str:
    root = ElementTree.fromstring(xml)
    element = root.find(path, namespaces or {})
    assert element is not None

    return ElementTree.tostring(element, encoding="unicode")


def _raw_post(
    *,
    payload: str,
    url: str,
    scraped_at: datetime.datetime,
    post_created_at: datetime.datetime | None = None,
) -> RawPost:
    return RawPost(
        domain="example.com",
        post_id="post-id",
        url=url,
        post_created_at=post_created_at,
        scraped_at=scraped_at,
        source="rss",
        request_url="https://example.com/feed.xml",
        payload=payload,
    )


def test_convert_raw_post_atom_entry() -> None:
    scraped_at = datetime.datetime(2026, 2, 1, 2, 0, tzinfo=datetime.UTC)
    raw = _raw_post(
        payload=_raw_xml(
            ATOM_XML,
            "atom:entry",
            {"atom": "http://www.w3.org/2005/Atom"},
        ),
        url="https://example.com/atom-post",
        scraped_at=scraped_at,
    )

    post = convert_raw_post(raw)

    assert post is not None
    assert post.domain == "example.com"
    assert post.url == "https://example.com/atom-post"
    assert post.title == "Atom Post"
    assert post.html == "Summary."
    assert post.author == ""
    assert post.created_at == datetime.datetime(
        2026,
        2,
        1,
        1,
        0,
        tzinfo=datetime.UTC,
    )
    assert post.updated_at == scraped_at


def test_convert_raw_post_rss2_item() -> None:
    scraped_at = datetime.datetime(2026, 2, 1, 2, 0, tzinfo=datetime.UTC)
    raw = _raw_post(
        payload=_raw_xml(RSS2_XML, "channel/item"),
        url="https://example.com/rss2-post",
        scraped_at=scraped_at,
    )

    post = convert_raw_post(raw)

    assert post is not None
    assert post.domain == "example.com"
    assert post.url == "https://example.com/rss2-post"
    assert post.title == "RSS2 Post"
    assert post.html == "RSS2 Summary."
    assert post.author == ""
    assert post.created_at == datetime.datetime(
        2026,
        2,
        1,
        1,
        0,
        tzinfo=datetime.UTC,
    )
    assert post.updated_at == scraped_at


def test_convert_raw_post_rss1_item() -> None:
    scraped_at = datetime.datetime(2026, 2, 1, 2, 0, tzinfo=datetime.UTC)
    raw = _raw_post(
        payload=_raw_xml(
            RSS1_XML,
            "rss:item",
            {"rss": "http://purl.org/rss/1.0/"},
        ),
        url="https://example.com/rss1-post",
        scraped_at=scraped_at,
    )

    post = convert_raw_post(raw)

    assert post is not None
    assert post.domain == "example.com"
    assert post.url == "https://example.com/rss1-post"
    assert post.title == "RSS1 Post"
    assert post.html == ""
    assert post.author == ""
    assert post.created_at == scraped_at
    assert post.updated_at == scraped_at
