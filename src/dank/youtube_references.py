"""Extract attributable video links from source-owned post/comment fields."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

from dank.youtube import canonical_video_id, payload_object
from dank.youtube_reference_html import Link, comment_links, content_links


def _objects(value: object) -> list[dict[str, Any]]:
    return (
        [payload_object(item) for item in cast(list[object], value)]
        if isinstance(value, list)
        else []
    )


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


class _PostContent(NamedTuple):
    title: str = ""
    author: str = ""
    links: Sequence[Link] = ()
    comments: Sequence[dict[str, Any]] = ()


def _raw_content(row: dict[str, Any]) -> _PostContent:
    payload = payload_object(row.get("payload"))

    match row["source"]:
        case "reddit":
            body = _text(payload.get("selftext_html"))
            links = (
                content_links(body)
                if body
                else content_links(
                    _text(payload.get("selftext")),
                    markup=False,
                )
            )
            return _PostContent(
                _text(payload.get("title")),
                _text(payload.get("author")),
                links + _direct_link(payload.get("url")),
                _objects(
                    payload_object(payload.get("comment_capture")).get(
                        "comments",
                    ),
                ),
            )
        case "hacker_news":
            story = payload_object(payload.get("story"))
            listing = payload_object(payload.get("hckrnews"))

            return _PostContent(
                _text(story.get("title") or listing.get("link_text")),
                _text(story.get("by")),
                content_links(_text(story.get("text")))
                + _direct_link(
                    story.get("url") or listing.get("link"),
                ),
                _objects(payload.get("comments")),
            )
        case "x":
            return _x_content(payload)
        case "rss":
            content = _rss_content(
                _text(payload.get("feed_xml")) or _text(row.get("payload")),
            )
            # Comment feed items may archive their parent article's full page.
            fragment = urlsplit(str(row["url"])).fragment
            page_links = (
                []
                if fragment.startswith("comment")
                else content_links(
                    _text(payload.get("page_html")),
                    article=True,
                )
            )

            return content._replace(links=[*content.links, *page_links])
        case _:
            return _PostContent()


def _x_content(payload: dict[str, Any]) -> _PostContent:
    legacy = payload_object(payload.get("legacy"))
    note = payload_object(
        payload_object(
            payload_object(
                payload.get("note_tweet"),
            ).get("note_tweet_results"),
        ).get("result"),
    )
    body = _text(note.get("text") or legacy.get("full_text"))
    entities = payload_object(
        note.get("entity_set") or legacy.get("entities"),
    )

    for entity in _objects(entities.get("urls")):
        short = _text(entity.get("url"))

        if short:
            body = body.replace(
                short,
                _text(entity.get("expanded_url")) or short,
            )

    return _PostContent(
        body.splitlines()[0] if body else "",
        "",
        content_links(body, markup=False),
    )


def _rss_content(xml: str) -> _PostContent:
    # Page HTML contains recommendations. Use the feed entry here; processed
    # article content is indexed independently at the processing boundary.
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return _PostContent()

    if root.tag.rsplit("}", 1)[-1] not in {"item", "entry"}:
        return _PostContent()

    title = ""
    links: list[Link] = []

    for child in root:
        tag = child.tag.rsplit("}", 1)[-1]

        if tag == "title":
            title = "".join(child.itertext())
        elif tag in {"encoded", "content", "description", "summary"}:
            content = (child.text or "") + "".join(
                ET.tostring(part, encoding="unicode") for part in child
            )
            links += content_links(content)

    return _PostContent(title, "", links)


def reference_rows(row: dict[str, Any], *, raw: bool) -> list[dict[str, Any]]:
    """One archived occurrence per video and post/comment identity."""
    source = str(row["source"])

    if source == "youtube" or row["domain"] == "youtube.com":
        return []

    if raw:
        content = _raw_content(row)
    elif source == "rss":
        content = _PostContent(
            _text(row.get("title")),
            _text(row.get("author")),
            content_links(_text(row.get("html"))),
        )
    else:
        # Social projections can include other posts' quotations/cards.
        # Their own structured raw fields are the reference authority.
        return []

    common = {
        "domain": row["domain"],
        "post_id": row["post_id"],
        "url": row["url"],
        "source": source,
        "title": content.title[:300],
        "observed_at": row["scraped_at" if raw else "updated_at"],
    }
    results = [
        _evidence(common, link, "post", "", str(row["url"]), content.author)
        for link in [*content.links, *_direct_link(row.get("url"))]
    ]
    results += _comment_evidence(common, content.comments)

    if raw and source == "rss":
        payload = payload_object(row.get("payload"))
        origin = urlsplit(str(row["url"]))

        for comment_id, link in comment_links(_text(payload.get("page_html"))):
            if (origin.fragment.startswith("comment")
                    and origin.fragment != comment_id):
                continue

            results.append(_evidence(
                common, link, "comment", comment_id,
                origin._replace(fragment=comment_id).geturl(), "",
            ))

    return list(
        {
            (r["video_id"], r["kind"], r["reference_id"]): r
            for r in reversed(results)
        }.values(),
    )


def _comment_evidence(
    common: dict[str, Any],
    comments: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    hn = common["source"] == "hacker_news"
    parents = {
        str(c.get("id")): str(c.get("parent" if hn else "parent_id"))
        for c in comments
    }
    results: list[dict[str, Any]] = []

    for comment in comments:
        comment_id = str(comment.get("id") or "")

        if comment.get("deleted") or comment.get("dead"):
            continue

        if not _belongs_to_thread(comment_id, parents, str(common["post_id"])):
            continue

        if hn:
            if not comment_id.isdecimal():
                continue

            permalink = "https://news.ycombinator.com/item?id=" + comment_id
            author = _text(comment.get("by"))
            found = content_links(_text(comment.get("text")))
        else:
            if not re.fullmatch(r"t1_[a-z0-9]+", comment_id):
                continue

            post_id = str(common["post_id"]).removeprefix("t3_")
            permalink = (
                f"https://www.reddit.com/comments/{post_id}"
                f"/comment/{comment_id[3:]}/"
            )
            author = _text(comment.get("author"))
            body = _text(comment.get("body_html"))
            found = (
                content_links(body)
                if body
                else content_links(
                    _text(comment.get("body")),
                    markup=False,
                )
            )

        results.extend(
            _evidence(common, link, "comment", comment_id, permalink, author)
            for link in found
        )

    return results


def _belongs_to_thread(
    comment_id: str,
    parents: dict[str, str],
    post_id: str,
) -> bool:
    parent = parents.get(comment_id, "")
    visited = {comment_id}

    while parent in parents and parent not in visited:
        visited.add(parent)
        parent = parents[parent]

    return parent == post_id


def _direct_link(value: object) -> list[Link]:
    target = _text(value)
    video_id = canonical_video_id(target)

    return [Link(video_id, target, target)] if video_id else []


def _evidence(
    common: dict[str, Any],
    link: Link,
    kind: str,
    reference_id: str,
    reference_url: str,
    author: str,
) -> dict[str, Any]:
    return {
        **common,
        "video_id": link.video_id,
        "format_hint": "short" if "/shorts/" in link.url else "video",
        "kind": kind,
        "reference_id": reference_id,
        "reference_url": reference_url,
        "author": author,
        "matched_url": link.url,
        "excerpt": link.excerpt,
    }
