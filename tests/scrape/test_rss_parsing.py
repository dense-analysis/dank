import xml.etree.ElementTree as ElementTree

from dank.scrape.rss import parse_feed_entries

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


def test_parse_feed_entries_atom() -> None:
    entries = parse_feed_entries(
        ATOM_XML,
        domain="example.com",
        root_url="https://example.com/",
    )

    assert len(entries) == 1

    entry = entries[0]

    assert entry.domain == "example.com"
    assert entry.url == "https://example.com/atom-post"
    assert entry.created_at is not None
    assert entry.payload == _raw_xml(
        ATOM_XML,
        "atom:entry",
        {"atom": "http://www.w3.org/2005/Atom"},
    )


def test_parse_feed_entries_rss2() -> None:
    entries = parse_feed_entries(
        RSS2_XML,
        domain="example.com",
        root_url="https://example.com/",
    )

    assert len(entries) == 1

    entry = entries[0]
    assert entry.domain == "example.com"
    assert entry.url == "https://example.com/rss2-post"
    assert entry.created_at is not None
    assert entry.payload == _raw_xml(RSS2_XML, "channel/item")


def test_parse_feed_entries_rss1() -> None:
    entries = parse_feed_entries(
        RSS1_XML,
        domain="example.com",
        root_url="https://example.com/",
    )

    assert len(entries) == 1

    entry = entries[0]
    assert entry.domain == "example.com"
    assert entry.url == "https://example.com/rss1-post"
    assert entry.created_at is None
    assert entry.payload == _raw_xml(
        RSS1_XML,
        "rss:item",
        {"rss": "http://purl.org/rss/1.0/"},
    )
