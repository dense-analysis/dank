"""Prepare feed-only YouTube items without fetching watch pages or comments."""

from __future__ import annotations

import html

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.model import Post, RawPost
from dank.youtube import payload_object


def convert_raw_youtube_post(row: RawPost) -> Post | None:
    payload = payload_object(row.payload)
    kind = payload.get("kind")
    title = str(payload.get("title") or payload.get("text") or "").strip()

    if kind not in {"video", "short", "post"} or not title:
        return None

    text = str(payload.get("text") or "").strip()
    body = "".join(
        f"<p>{html.escape(paragraph)}</p>" for paragraph in text.split("\n")
        if paragraph.strip()
    )

    return Post(
        domain=row.domain, post_id=row.post_id, url=row.url,
        created_at=row.post_created_at or row.scraped_at,
        updated_at=row.scraped_at,
        author=str(payload.get("channel_name") or ""), title=title,
        title_embedding=EMPTY_STRING_VECTOR, html=body,
        html_embedding=EMPTY_STRING_VECTOR, source="youtube",
    )
