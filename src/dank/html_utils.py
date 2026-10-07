from __future__ import annotations

import html
from html.parser import HTMLParser
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


NOISE_TAGS = {"head", "script", "style", "noscript"}
TEXT_BLOCKS = {
    "article", "blockquote", "br", "div", "h1", "h2", "h3", "h4", "h5",
    "h6", "li", "main", "ol", "p", "pre", "section", "td", "tr", "ul",
}


class _ReadableHTML(HTMLParser):
    def __init__(self, *, text_only: bool = False) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.text_only = text_only
        self.skip: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if self.skip == "head" and tag == "body":
            self.skip = None

        if self.skip:
            return

        if tag in NOISE_TAGS:
            self.skip = tag
        elif self.text_only:
            if tag in TEXT_BLOCKS:
                self.parts.append("\n")
        else:
            self.parts.append(self.get_starttag_text() or "")

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if self.skip or tag in NOISE_TAGS:
            return

        if self.text_only:
            if tag in TEXT_BLOCKS:
                self.parts.append("\n")
        else:
            self.parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag: str) -> None:
        if self.skip:
            if tag == self.skip:
                self.skip = None
        elif tag not in NOISE_TAGS:
            if not self.text_only:
                self.parts.append(f"</{tag}>")
            elif tag in TEXT_BLOCKS:
                self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.skip:
            self.parts.append(
                data if self.text_only else html.escape(data, quote=False),
            )


def remove_page_noise(value: str) -> str:
    parser = _ReadableHTML()
    parser.feed(value)

    return "".join(parser.parts).strip()


def html_text(value: str) -> str:
    parser = _ReadableHTML(text_only=True)
    parser.feed(value)

    return "".join(parser.parts).strip()
