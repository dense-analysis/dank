import datetime as dt
import hashlib

import pytest

from dank.links import LinkPost, normalized_url, post_index_rows

NOW = dt.datetime(2026, 10, 17, 19, 30, tzinfo=dt.UTC)
HN_URL = "https://news.ycombinator.com/item?id=123"


@pytest.mark.parametrize("value, expected", [
    ("HTTPS://Publisher.test:443/article#part", "https://publisher.test/article"),
    ("http://publisher.test:80", "http://publisher.test/"),
    ("https://publisher.test:8443/a", "https://publisher.test:8443/a"),
    ("https://publisher.test/a?x=1&amp;y=2#part",
     "https://publisher.test/a?x=1&y=2"),
    ("https://[::1]:8443/a", "https://[::1]:8443/a"),
    ("https://twitter.com/person/status/123?ref=hn",
     "https://x.com/i/status/123"),
    ("https://www.x.com/person/status/123/", "https://x.com/i/status/123"),
    ("https://old.reddit.com/r/python/comments/AbC/title/?context=3",
     "https://www.reddit.com/comments/abc"),
    ("https://m.reddit.com/comments/abc", "https://www.reddit.com/comments/abc"),
    ("https://youtu.be/abcdefghijk?t=12",
     "https://www.youtube.com/watch?v=abcdefghijk"),
])
def test_url_identity_normalizes_only_known_equivalence(
    value: str, expected: str,
) -> None:
    assert normalized_url(value) == expected


@pytest.mark.parametrize("left, right", [
    ("https://publisher.test/Article", "https://publisher.test/article"),
    ("https://publisher.test/article", "https://publisher.test/article/"),
    ("https://publisher.test/a?edition=1", "https://publisher.test/a?edition=2"),
    ("https://publisher.test/a?x=1&y=2", "https://publisher.test/a?y=2&x=1"),
    ("https://publisher.test/a%2Fb", "https://publisher.test/a/b"),
    ("http://publisher.test/a", "https://publisher.test/a"),
    ("https://www.publisher.test/a", "https://publisher.test/a"),
    ("https://x.com:8443/user/status/123", "https://x.com/user/status/123"),
    ("https://reddit.com:8443/comments/abc", "https://reddit.com/comments/abc"),
    ("https://youtube.com:8443/watch?v=abcdefghijk",
     "https://youtube.com/watch?v=abcdefghijk"),
    ("https://reddit.com/r/a/comments/abc/title/def/",
     "https://reddit.com/r/a/comments/abc/title/"),
    ("https://news.ycombinator.com/item?id=1",
     "https://news.ycombinator.com/item?id=2"),
])
def test_distinct_origins_paths_and_queries_do_not_merge(
    left: str, right: str,
) -> None:
    assert normalized_url(left) != normalized_url(right)


@pytest.mark.parametrize("value", [
    "", "/relative/path", "javascript:alert(1)", "file:///private/file",
    "https://user:password@publisher.test/a", "https://user@publisher.test/a",
    "https://[broken/a", "https://publisher.test:invalid/a",
    "https://publisher.test:65536/a", "https://publisher.test/a b",
    "https://publisher.test/a\nb", "https:///article",
])
def test_unsafe_and_malformed_urls_have_no_identity(value: str) -> None:
    assert normalized_url(value) is None


def _post(body: str, *, source: str = "hacker_news") -> LinkPost:
    return LinkPost("news.ycombinator.com", "123", HN_URL, body, source, NOW)


def test_self_post_citation_is_not_a_primary_destination() -> None:
    own, links = post_index_rows(_post(
        'Ask HN: what do you think of <a href="https://publisher.test/a">'
        'this article</a>?',
    ))

    assert own == [{
        "url": HN_URL, "domain": "news.ycombinator.com", "post_id": "123",
        "observed_at": NOW,
    }]
    assert len(links) == 1
    assert links[0]["target_url"] == "https://publisher.test/a"
    assert links[0]["kind"] == "link"


def test_marked_destination_is_primary_even_when_followed_by_a_citation(
) -> None:
    _, links = post_index_rows(_post(
        '<p><a data-dank-hn-destination="1" href="https://publisher.test/a">'
        'Article</a></p><p>Also <a href="https://elsewhere.test/b">context</a>'
        '</p>',
    ))

    assert [(link["target_url"], link["kind"]) for link in links] == [
        ("https://publisher.test/a", "primary"),
        ("https://elsewhere.test/b", "link"),
    ]


@pytest.mark.parametrize("body, expected_kind", [
    ('<p><a href="https://publisher.test/a">https://publisher.test/a</a></p>',
     "primary"),
    ('<p>See <a href="https://publisher.test/a">https://publisher.test/a</a></p>',
     "link"),
    ('<p><a href="https://publisher.test/a">Article</a></p>', "link"),
])
def test_legacy_primary_requires_a_link_only_body(
    body: str, expected_kind: str,
) -> None:
    _, links = post_index_rows(_post(body))

    assert links[0]["kind"] == expected_kind


def test_marker_cannot_make_other_sources_primary() -> None:
    _, links = post_index_rows(_post(
        '<a data-dank-hn-destination="1" href="https://publisher.test/a">'
        'https://publisher.test/a</a>', source="rss",
    ))

    assert links[0]["kind"] == "link"


def test_links_resolve_relative_urls_deduplicate_and_exclude_self() -> None:
    post = LinkPost(
        "publisher.test", "a", "https://publisher.test/posts/article", (
            '<a href="#part">Self</a><a href="../other#part">Other</a>'
            '<a href="https://publisher.test/other">Again</a>'
            '<a href="javascript:alert(1)">Unsafe</a>'
        ), "rss", NOW,
    )
    _, links = post_index_rows(post)

    assert len(links) == 1
    assert links[0]["target_url"] == "https://publisher.test/other"


def test_content_evidence_changes_when_reprocessed_at_same_timestamp() -> None:
    original = _post('<a href="https://publisher.test/old">Original</a>')
    reprocessed = original._replace(
        html='<a href="https://publisher.test/new">Corrected</a>',
    )
    _, previous_links = post_index_rows(original)
    _, current_links = post_index_rows(reprocessed)
    previous = previous_links[0]
    current = current_links[0]

    assert previous["observed_at"] == current["observed_at"]
    assert previous["content_hash"] != current["content_hash"]
    assert current["content_hash"] == hashlib.sha256(
        reprocessed.html.encode(),
    ).hexdigest().upper()
    assert not any(
        link["target_url"] == previous["target_url"] for link in current_links
    )
