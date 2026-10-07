from __future__ import annotations

import html
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

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


class _BaseURLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.href: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "base" and self.href is None:
            for key, value in attrs:
                if key == "href":
                    self.href = value or ""
                    break


def html_base_url(value: str, response_url: str) -> str:
    parser = _BaseURLParser()
    parser.feed(value)

    try:
        base_url = urljoin(response_url, parser.href or "")
        parsed = urlparse(base_url)
        _ = parsed.port

        if parsed.scheme in {"http", "https"} and parsed.hostname:
            return base_url
    except ValueError:
        pass

    return response_url


class _ReadableHTML(HTMLParser):
    def __init__(
        self, *, text_only: bool = False, base_url: str | None = None,
    ) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.text_only = text_only
        self.base_url = base_url
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
            self.parts.append(self._start_tag(tag, attrs))

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if self.skip or tag in NOISE_TAGS:
            return

        if self.text_only:
            if tag in TEXT_BLOCKS:
                self.parts.append("\n")
        else:
            self.parts.append(self._start_tag(tag, attrs, self_closing=True))

    def _start_tag(
        self, tag: str, attrs: list[tuple[str, str | None]],
        *, self_closing: bool = False,
    ) -> str:
        if not self.base_url:
            return self.get_starttag_text() or ""

        resolved: list[tuple[str, str | None]] = []

        for key, value in attrs:
            if key in {"href", "src", "poster"} and value:
                try:
                    value = urljoin(self.base_url, value)
                except ValueError:
                    pass

            resolved.append((key, value))

        if resolved == attrs:
            return self.get_starttag_text() or ""

        attributes = "".join(
            f' {key}="{html.escape(value, quote=True)}"'
            if value is not None else f" {key}"
            for key, value in resolved
        )
        suffix = " />" if self_closing else ">"

        return f"<{tag}{attributes}{suffix}"

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


def remove_page_noise(value: str, *, base_url: str | None = None) -> str:
    parser = _ReadableHTML(base_url=base_url)
    parser.feed(value)

    return "".join(parser.parts).strip()


def html_text(value: str) -> str:
    parser = _ReadableHTML(text_only=True)
    parser.feed(value)

    return "".join(parser.parts).strip()
