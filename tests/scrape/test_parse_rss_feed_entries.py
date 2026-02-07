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
            <description>RSS2 Summary.</description>
        </item>
    </channel>
</rss>
"""

def test_parse_feed_entries_atom() -> None:
    entries = parse_feed_entries(
        ATOM_XML,
        domain="example.com",
        root_url="https://example.com",
    )

    assert entries
    assert entries[0].url == "https://example.com/atom-post"
    assert "entry" in entries[0].payload


def test_parse_feed_entries_rss2() -> None:
    entries = parse_feed_entries(
        RSS2_XML,
        domain="example.com",
        root_url="https://example.com",
    )

    assert entries
    assert entries[0].url == "https://example.com/rss2-post"
    assert "<item" in entries[0].payload
