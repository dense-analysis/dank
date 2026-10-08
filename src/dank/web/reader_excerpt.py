"""Plain-text search previews from collected article content."""

from __future__ import annotations

import re
from collections import Counter

from dank.html_utils import html_text


def search_excerpt(raw_html: str, query: str, limit: int = 280) -> str | None:
    terms = sorted(set(query.split()), key=len, reverse=True)

    if not terms:
        return None

    text = " ".join(html_text(raw_html).split())
    pattern = "|".join(re.escape(term) for term in terms)
    matches = list(re.finditer(pattern, text, re.IGNORECASE))

    if not matches:
        return None

    # Choose a short passage containing the most distinct search terms.
    counts: Counter[str] = Counter()
    left = 0
    best_start = matches[0].start()
    best_count = 0

    for right, match in enumerate(matches):
        counts[match.group().casefold()] += 1

        while (
            left < right
            and match.end() - matches[left].start() > limit - 80
        ):
            word = matches[left].group().casefold()
            counts[word] -= 1

            if not counts[word]:
                del counts[word]

            left += 1

        if len(counts) > best_count:
            best_count = len(counts)
            best_start = matches[left].start()

    return _passage(text, best_start, limit)


def _passage(text: str, match_start: int, limit: int) -> str:
    start = max(0, match_start - 60)

    if start:
        boundary = text.find(" ", start, match_start)

        if boundary >= 0:
            start = boundary + 1

    end = min(len(text), start + limit)

    if end < len(text):
        boundary = text.rfind(" ", match_start, end)

        if boundary >= 0:
            end = boundary

    return (
        ("… " if start else "") + text[start:end].strip()
        + ("…" if end < len(text) else "")
    )
