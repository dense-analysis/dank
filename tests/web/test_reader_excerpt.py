import datetime as dt
import pathlib

from dank.web.app import PostRow
from dank.web.reader_api import post_payload
from dank.web.reader_excerpt import search_excerpt


def test_excerpt_finds_a_later_passage_with_more_matching_terms() -> None:
    intro = "An introduction about domain terminology. " + "Background. " * 50
    passage = "A practical domain driven design example explains aggregates."
    result = search_excerpt(
        f"<p>{intro}</p><p>{passage}</p>", "domain driven design",
    )
    assert result is not None
    assert result.startswith("… ")
    assert passage in result
    assert "An introduction" not in result
    assert len(result) <= 283


def test_excerpt_preserves_entities_and_literal_punctuation() -> None:
    result = search_excerpt(
        "<p>Learn C++ &amp; [API] conventions with &lt;example&gt;.</p>",
        "c++ [API]",
    )
    assert result == "Learn C++ & [API] conventions with <example>."


def test_semantic_only_and_empty_queries_keep_the_normal_preview() -> None:
    body = "<p>A model of domain boundaries.</p>"
    assert search_excerpt(body, "architecture") is None
    assert search_excerpt("<p>A model of domain boundaries.</p>", " ") is None


def test_post_payload_changes_only_the_excerpt_for_search(
    tmp_path: pathlib.Path,
) -> None:
    now = dt.datetime.now(dt.UTC)
    body = "<p>" + "Introduction. " * 40 + "</p><p>The needle is here.</p>"
    post = PostRow(
        "example.com", "1", "https://example.com/story", "Author", "Story",
        body, now, now, "rss",
    )
    ordinary = post_payload(post, [], tmp_path)
    searched = post_payload(post, [], tmp_path, "needle")
    assert "needle" not in ordinary["excerpt"]
    assert "needle" in searched["excerpt"]
    assert ordinary["html"] == searched["html"]
    assert ordinary["title"] == searched["title"]
