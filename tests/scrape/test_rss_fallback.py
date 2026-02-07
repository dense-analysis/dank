from dank.scrape.rss import discover_feed_links


def test_discover_feed_links_from_head() -> None:
    html = (
        "<html><head>"
        '<link rel="alternate" type="application/rss+xml" '
        'href="/feed/"/>'
        '<link rel="alternate" type="application/atom+xml" '
        'href="https://example.com/atom.xml"/>'
        "</head><body></body></html>"
    )
    links = discover_feed_links(html, "https://example.com/")
    assert [(link.url, link.feed_type, link.mime_type) for link in links] == [
        ("https://example.com/feed/", "rss2", "application/rss+xml"),
        ("https://example.com/atom.xml", "atom", "application/atom+xml"),
    ]
