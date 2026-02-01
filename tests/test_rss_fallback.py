from dank.scrape.rss import discover_feed_links, select_feed_link


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


def test_select_feed_link_prefers_atom() -> None:
    html = (
        "<html><head>"
        '<link rel="alternate" type="application/rss+xml" '
        'href="/feed/"/>'
        '<link rel="alternate" type="application/atom+xml" '
        'href="https://example.com/atom.xml"/>'
        '<link rel="alternate" type="application/rdf+xml" '
        'href="/rss1.xml"/>'
        "</head><body></body></html>"
    )
    links = discover_feed_links(html, "https://example.com/")
    selected = select_feed_link(links)

    assert selected is not None
    assert selected.url == "https://example.com/atom.xml"
    assert selected.feed_type == "atom"
