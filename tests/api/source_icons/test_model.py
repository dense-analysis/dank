"""Which domains may have an icon, and where it is."""

import pytest

from dank.api.source_icons.model import icon_domain, source_icon_url


@pytest.mark.parametrize("domain", [
    "reddit.com", "www.youtube.com", "x.com", "twitter.com", "127.0.0.1",
    "localhost", "host:8080", "../github.blog", "github.blog/path",
    "bad..example", "", "-bad.example", f"{'a' * 64}.example",
])
def test_platforms_and_malformed_domains_have_no_icon(domain: str) -> None:
    assert source_icon_url(domain) is None
    assert icon_domain(domain) is None


def test_icon_urls_are_query_encoded() -> None:
    assert source_icon_url("github.blog") == (
        "/api/reader/source-icon?domain=github.blog"
    )
    assert source_icon_url("news.ycombinator.com") is not None


def test_requested_domains_are_trimmed_and_lowercased() -> None:
    assert icon_domain("  GitHub.Blog ") == "github.blog"
