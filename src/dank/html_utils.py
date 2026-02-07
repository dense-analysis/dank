from __future__ import annotations

from urllib.parse import urlparse

YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
    "www.youtu.be",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
}


def is_youtube_url(url: str) -> bool:
    if not url:
        return False

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if not host:
        return False

    host = host.split(":", 1)[0]

    if host in YOUTUBE_HOSTS:
        return True

    if host.endswith(".youtube.com"):
        return True

    if host.endswith(".youtube-nocookie.com"):
        return True

    return False
