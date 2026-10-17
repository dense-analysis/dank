"""YouTube compatibility adapter over shared, attributable HTML links."""

from typing import NamedTuple

from dank import reference_html
from dank.youtube import canonical_video_id


class Link(NamedTuple):
    video_id: str
    url: str
    excerpt: str


def content_links(
    content: str, *, markup: bool = True, article: bool = False,
) -> list[Link]:
    return [Link(*link) for link in reference_html.content_links(
        content, markup=markup, article=article, identity=canonical_video_id,
    )]


def comment_links(content: str) -> list[tuple[str, Link]]:
    return [(key, Link(*link)) for key, link in reference_html.comment_links(
        content, identity=canonical_video_id,
    )]
