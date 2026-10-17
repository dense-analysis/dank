"""Stable public media addresses, shared by collection and presentation."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit


def canonical_media_url(value: str) -> str:
    """Use the stable item address when Archive storage nodes move."""
    try:
        url = urlsplit(value)
        parts = url.path.split("/")

        if (
            url.scheme not in {"http", "https"}
            or not re.fullmatch(r"(?:ia|dn)\d+\.us\.archive\.org",
                                url.hostname or "")
            or url.username or url.password or url.port not in {None, 80, 443}
            or len(parts) < 5 or not parts[1].isdigit()
            or parts[2] != "items" or not parts[3] or not parts[4]
        ):
            return value
    except ValueError:
        return value

    # https://archive.org/developers/items.html#archival-urls
    # Item identifiers and filenames remain unchanged, including URL escapes.
    return urlunsplit((
        "https", "archive.org", "/download/" + "/".join(parts[3:]),
        url.query, url.fragment,
    ))
