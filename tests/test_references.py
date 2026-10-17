from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pytest

from dank.reference_html import content_links
from dank.references import reference_rows

STAMP = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)
TARGET = "https://publisher.test/story?edition=2#evidence"


def raw(source: str, payload: dict[str, Any], *, domain: str = "origin.test",
        post_id: str = "123", url: str = "https://origin.test/article",
        ) -> dict[str, Any]:
    return {"source": source, "domain": domain, "post_id": post_id, "url": url,
            "scraped_at": STAMP, "payload": json.dumps(payload)}


def test_reddit_post_and_nested_comments_preserve_exact_evidence() -> None:
    evidence = reference_rows(raw("reddit", {
        "title": "A discussion", "url": TARGET,
        "selftext_html": '<p>See <a href="' + TARGET + '">the study</a>.</p>',
        "comment_capture": {"comments": [
            {"id": "t1_parent", "parent_id": "t3_thread", "body": "Context"},
            {"id": "t1_child", "parent_id": "t1_parent", "author": "Reader",
             "body_html": (
                 '<p>Read <a href="' + TARGET + '">the results</a>.</p>'
             )},
            {"id": "t1_foreign", "parent_id": "t3_other", "body": TARGET},
            {"id": "t1_deleted", "parent_id": "t3_thread", "body": TARGET,
             "deleted": True},
        ]},
        "raw": {"html": "https://unrelated.test/page"},
        "crosspost_parent_list": [{"url": "https://unrelated.test/quote"}],
    }, post_id="t3_thread"), raw=True)
    assert len(evidence) == 2
    post = next(row for row in evidence if row["kind"] == "post")
    comment = next(row for row in evidence if row["kind"] == "comment")
    assert post["excerpt"] == "See the study ."
    assert comment["author"] == "Reader"
    assert comment["reference_id"] == "t1_child"
    assert comment["reference_url"].endswith("/thread/comment/child/")
    assert comment["matched_url"] == TARGET
    assert comment["target_url"] == TARGET.split("#")[0]
    assert comment["observed_at"] == STAMP


def test_hn_parent_chain_and_owned_story_keep_distinct_destinations() -> None:
    rows = reference_rows(raw("hacker_news", {
        "story": {"title": "Story", "url": TARGET},
        "comments": [
            {"id": 2, "parent": 123, "text": "https://publisher.test/other"},
            {"id": 3, "parent": 2, "text": TARGET},
            {"id": 4, "parent": 5, "text": TARGET},
            {"id": 5, "parent": 4, "text": TARGET},
            {"id": 6, "parent": 123, "dead": True, "text": TARGET},
        ],
    }), raw=True)
    assert {(r["kind"], r["reference_id"]) for r in rows} == {
        ("post", ""), ("comment", "2"), ("comment", "3"),
    }
    assert next(r for r in rows if r["reference_id"] == "3")[
        "reference_url"
    ] == "https://news.ycombinator.com/item?id=3"


def test_x_entities_only_expand_urls_present_in_owned_text() -> None:
    rows = reference_rows(raw("x", {
        "legacy": {"full_text": "Read https://t.co/owned", "entities": {
            "urls": [
                {"url": "https://t.co/owned", "expanded_url": TARGET},
                {"url": "https://t.co/other", "expanded_url": "https://no.test"},
            ],
        }},
        "quoted_status_result": {"result": {"legacy": {
            "full_text": "https://quoted.test/story",
        }}},
        "profile": {"url": "https://profile.test"},
    }), raw=True)
    assert [r["matched_url"] for r in rows] == [TARGET]


@pytest.mark.parametrize("source", ["rss", "linked_article"])
def test_article_scope_relative_urls_and_owned_identified_comments(
    source: str,
) -> None:
    rows = reference_rows(raw(source, {"page_html": (
        '<nav><a href="https://noise.test/nav">Menu</a></nav>'
        '<article><p>Read <a href="/other?edition=2#part">the result</a>.</p>'
        '<div class="related"><a href="https://noise.test/related">Related</a>'
        '</div></article><article><a href="https://noise.test/other">Other</a>'
        '</article><li id="comment-1">Parent<ul><li id="comment-2">'
        '<p>See <a href="' + TARGET + '">the study</a></p></li></ul></li>'
    )}), raw=True)
    assert {r["target_url"] for r in rows} == {
        "https://origin.test/other?edition=2", TARGET.split("#")[0],
    }
    assert next(r for r in rows if r["kind"] == "comment")[
        "reference_id"
    ] == "comment-2"


def test_query_semantics_are_not_fuzzy_and_invalid_urls_are_ignored() -> None:
    links = content_links(
        '<a href="https://publisher.test/?a=1&amp;b=2">First</a>'
        '<a href="https://publisher.test/?b=2&amp;a=1">Second</a>'
        '<a href="https://publisher.test/?a=3&amp;b=2">Third</a>'
        '<a href="javascript:alert(1)">Invalid</a>'
        '<a href="https://user:password@publisher.test/">Credentials</a>'
        '<a href="#toc">Section</a>'
        '<script>https://script.test</script>'
        '<div hidden><a href="https://hidden.test">Hidden</a></div>',
    )
    assert [link.key for link in links] == [
        "https://publisher.test/?a=1&b=2", "https://publisher.test/?b=2&a=1",
        "https://publisher.test/?a=3&b=2",
    ]


def test_self_links_are_removed_but_aggregator_destinations_remain() -> None:
    payload = {"page_html": (
        '<article><a href="https://origin.test/article#part">Self</a></article>'
    )}
    assert reference_rows(raw("rss", payload), raw=True) == []
    aggregated = raw("rss", {}, domain="aggregator.test", url=TARGET)
    assert reference_rows(aggregated, raw=True)[0]["target_url"] == (
        TARGET.split("#")[0]
    )


def test_processed_social_quotes_are_not_reindexed_as_owned_links() -> None:
    for source in ("x", "reddit", "hacker_news"):
        row = {**raw(source, {}), "updated_at": STAMP,
               "html": '<a href="' + TARGET + '">Foreign quote</a>'}
        assert reference_rows(row, raw=False) == []


def test_youtube_community_posts_only_index_explicit_normalized_text():
    row = raw("youtube", {
        "kind": "post", "title": "Update", "text": "Our results " + TARGET,
        "renderer": {"recommendation": "https://noise.test/recommended"},
    }, domain="youtube.com", post_id="post:community",
        url="https://www.youtube.com/post/community")
    assert [r["matched_url"] for r in reference_rows(row, raw=True)] == [
        TARGET,
    ]
    row["payload"] = json.dumps({"kind": "video", "text": TARGET})
    assert reference_rows(row, raw=True) == []


def test_identifiable_article_comment_excludes_author_and_reply_chrome():
    row = raw("rss", {"page_html": (
        '<li id="comment-1"><div class="comment-author">'
        '<a class="url" href="https://author.test">Author</a></div>'
        '<div class="comment-meta"><a href="https://origin.test/other">Date</a>'
        '</div><p>Read <a href="' + TARGET + '">the study</a>.</p>'
        '<div class="reply"><a href="https://origin.test/reply">Reply</a>'
        '</div></li>'
    )})
    assert [r["matched_url"] for r in reference_rows(row, raw=True)] == [
        TARGET,
    ]


def test_saved_page_redirect_and_base_resolve_relative_evidence():
    row = raw("rss", {
        "page_final_url": "https://publisher.test/posts/story/",
        "page_html": '<head><base href="../../references/"></head>'
                     '<article><a href="paper?edition=2#result">Paper</a>'
                     '</article><div id="comment-2">'
                     '<a href="followup">Follow-up</a></div>',
    })
    assert {r["target_url"] for r in reference_rows(row, raw=True)} == {
        "https://publisher.test/references/paper?edition=2",
        "https://publisher.test/references/followup",
    }


@pytest.mark.parametrize("url", [
    "https://en.wikipedia.org/wiki/Function_(mathematics)",
    "https://target.test/paper?text=wow!", "https://target.test/paper?text=end.",
])
def test_plaintext_preserves_balanced_delimiters_and_query_punctuation(
    url: str,
):
    assert content_links("Read " + url, markup=False)[0].url == url


@pytest.mark.parametrize("url", [
    "https://target.test/paper?text=wow!", "https://target.test/paper?text=end.",
    "https://target.test/paper?author=O'Reilly", "https://target.test/path.",
])
def test_x_structured_destinations_are_never_retokenized(url: str):
    row = raw("x", {"legacy": {
        "full_text": "Read https://t.co/exact", "entities": {"urls": [
            {"url": "https://t.co/exact", "expanded_url": url},
        ]},
    }})
    assert [r["matched_url"] for r in reference_rows(row, raw=True)] == [url]


@pytest.mark.parametrize("container", [
    '<aside class="related">{}</aside>', '<div hidden>{}</div>',
    '<nav>{}</nav>', '<footer>{}</footer>', '<template>{}</template>',
    '<div class="recommended-posts">{}</div>',
    '<div aria-hidden="true">{}</div>',
])
def test_article_comment_discovery_rejects_foreign_or_hidden_ancestors(
    container: str,
):
    comment = '<div id="comment-42"><a href="' + TARGET + '">Foreign</a></div>'
    row = raw("rss", {"page_html": '<article>Owned</article>'
                      + container.format(comment)})
    assert reference_rows(row, raw=True) == []


def test_hidden_comment_root_is_not_evidence_and_unquoted_ids_are_supported():
    hidden = raw("rss", {"page_html": '<div id="comment-42" hidden>'
                         '<a href="' + TARGET + '">Hidden</a></div>'})
    assert reference_rows(hidden, raw=True) == []
    visible = raw("rss", {"page_html": '<div id=comment-42>'
                          '<a href="' + TARGET + '">Visible</a></div>'})
    assert reference_rows(visible, raw=True)[0]["reference_id"] == "comment-42"


@pytest.mark.parametrize("container", [
    '<div hidden>{}</div>', '<div class="related-posts">{}</div>',
    '<div aria-hidden="true">{}</div>', '<aside>{}</aside>',
])
def test_raw_article_scope_skips_hidden_or_recommended_ancestors(
    container: str,
):
    foreign = '<article><a href="https://foreign.test">Foreign</a></article>'
    row = raw("rss", {"page_html": (
        container.format(foreign)
        + '<article><a href="' + TARGET + '">Owned</a></article>'
    )})
    assert [r["matched_url"] for r in reference_rows(row, raw=True)] == [
        TARGET,
    ]
