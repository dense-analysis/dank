from dank.process.page import (
    _content_candidate_kind,  # pyright: ignore[reportPrivateUsage]
)


def test_content_candidate_kind_template_content_slot() -> None:
    kind = _content_candidate_kind(
        "template",
        {"v-slot:content": ""},
        ["body", "single-post"],
    )

    assert kind == "template-content"


def test_content_candidate_kind_template_video_slot() -> None:
    kind = _content_candidate_kind(
        "template",
        {"slot": "video"},
        ["body", "single-video"],
    )

    assert kind == "template-video"


def test_content_candidate_kind_article_in_main() -> None:
    kind = _content_candidate_kind("article", {}, ["body", "main"])

    assert kind == "article-main"


def test_content_candidate_kind_entry_content_class() -> None:
    kind = _content_candidate_kind(
        "div",
        {"class": "hero entry-content rich-text"},
        ["body"],
    )

    assert kind == "entry-content"


def test_content_candidate_kind_id_content_block() -> None:
    kind = _content_candidate_kind(
        "section",
        {"id": "post-content-main"},
        ["body"],
    )

    assert kind == "content-block"


def test_content_candidate_kind_returns_none_for_non_content() -> None:
    kind = _content_candidate_kind("span", {"class": "chip"}, ["body"])

    assert kind is None
