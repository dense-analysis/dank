"""Capture small source favicons on first use and serve local PNG copies."""

from __future__ import annotations

import asyncio
import hashlib
import io
import ipaddress
import pathlib
import re
import socket
import tempfile
import time
import warnings
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin, urlsplit

import aiohttp
from aiohttp import web
from aiohttp.abc import ResolveResult
from PIL import Image, UnidentifiedImageError

from dank.web.app import AppState

# A sidebar must not start an unbounded crawl or download full-size artwork.
MAX_ICON_BYTES = 1024 * 1024
MAX_PAGE_BYTES = 512 * 1024
MAX_ICON_CANDIDATES = 3
MAX_ICON_PIXELS = 4096 * 4096
ICON_CAPTURE_SECONDS = 15
ICON_CONCURRENCY = 3
ICON_FRESH_SECONDS = 30 * 24 * 60 * 60
ICON_RETRY_SECONDS = 6 * 60 * 60
SOCIAL_DOMAINS = {"x.com", "twitter.com", "reddit.com", "youtube.com"}
DOMAIN_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def _domain_valid(domain: str) -> bool:
    if len(domain) > 253 or "." not in domain:
        return False

    try:
        ipaddress.ip_address(domain)
    except ValueError:
        return all(
            DOMAIN_LABEL.fullmatch(label) for label in domain.split(".")
        )

    return False


def source_icon_url(domain: str) -> str | None:
    if (
        not _domain_valid(domain)
        or domain.removeprefix("www.") in SOCIAL_DOMAINS
    ):
        return None

    return "/api/reader/source-icon?" + urlencode({"domain": domain})


class _PublicResolver(aiohttp.ThreadedResolver):
    """Validate the actual connection addresses, including redirected hosts."""

    async def resolve(
        self, host: str, port: int = 0,
        family: socket.AddressFamily = socket.AF_INET,
    ) -> list[ResolveResult]:
        addresses = await super().resolve(host, port, family)

        for address in addresses:
            ip = ipaddress.ip_address(address["host"])

            if not ip.is_global or ip.is_multicast:
                raise OSError("Source icons must resolve to public addresses")

        return addresses


def _public_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)

        return (
            parsed.scheme in {"http", "https"}
            and _domain_valid(parsed.hostname or "")
            and parsed.username is None and parsed.password is None
            and parsed.port in {None, 80, 443}
        )
    except ValueError:
        return False


class _IconLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag != "link":
            return

        values = dict(attrs)
        rel = (values.get("rel") or "").lower().split()
        href = values.get("href")
        media_type = (values.get("type") or "").lower()

        if (
            href and set(rel) & {"icon", "apple-touch-icon"}
            and media_type != "image/svg+xml"
            and not href.lower().split("?", 1)[0].endswith(".svg")
        ):
            self.links.append(href)


def _icon_candidates(page: bytes, page_url: str) -> list[str]:
    parser = _IconLinks()
    parser.feed(page.decode("utf-8", errors="replace"))
    declared: list[str] = []

    for link in parser.links:
        try:
            candidate = urljoin(page_url, link)
        except ValueError:
            continue

        if _public_url(candidate):
            declared.append(candidate)

    fallback = urljoin(page_url, "/favicon.ico")
    candidates = list(dict.fromkeys(declared))

    # Reserve one attempt for the conventional icon if declarations fail.
    declared = [url for url in candidates if url != fallback]

    return [*declared[:MAX_ICON_CANDIDATES - 1], fallback]


async def _fetch_bytes(
    session: aiohttp.ClientSession, url: str, limit: int, *,
    truncate: bool = False,
) -> tuple[bytes, str]:
    for _ in range(4):
        if not _public_url(url):
            raise ValueError("Unsupported source icon URL")

        async with session.get(url, allow_redirects=False) as response:
            if response.status in {301, 302, 303, 307, 308}:
                location = response.headers.get("Location")

                if not location:
                    raise ValueError("Source icon redirect has no destination")

                url = urljoin(url, location)
                continue

            response.raise_for_status()
            data = bytearray()

            async for chunk in response.content.iter_chunked(64 * 1024):
                data.extend(chunk)

                if len(data) > limit:
                    if truncate:
                        return bytes(data[:limit]), str(response.url)

                    raise ValueError("Source icon exceeds size limit")

            return bytes(data), str(response.url)

    raise ValueError("Too many source icon redirects")


def _as_png(data: bytes) -> bytes:
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)

        with Image.open(io.BytesIO(data)) as image:
            if image.width * image.height > MAX_ICON_PIXELS:
                raise ValueError("Source icon exceeds pixel limit")

            image.thumbnail((64, 64), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            image.convert("RGBA").save(output, format="PNG")

            return output.getvalue()


async def _capture_icon(domain: str) -> bytes | None:
    connector = aiohttp.TCPConnector(resolver=_PublicResolver(), limit=1)

    async with aiohttp.ClientSession(
        connector=connector, timeout=aiohttp.ClientTimeout(total=8),
        headers={"User-Agent": "DANK/0.1 (source favicon capture)"},
    ) as session:
        homepage = f"https://{domain}/"
        page = b""

        try:
            page, homepage = await _fetch_bytes(
                session, homepage, MAX_PAGE_BYTES, truncate=True,
            )
        except (aiohttp.ClientError, OSError, ValueError, TimeoutError):
            pass

        for url in _icon_candidates(page, homepage):
            try:
                data, _ = await _fetch_bytes(session, url, MAX_ICON_BYTES)

                return await asyncio.to_thread(_as_png, data)
            except (
                aiohttp.ClientError, OSError, ValueError, TimeoutError,
                UnidentifiedImageError, Image.DecompressionBombError,
                Image.DecompressionBombWarning,
            ):
                continue

    return None


def _write_icon(path: pathlib.Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary = pathlib.Path(file.name)

        try:
            file.write(data)
            file.flush()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


class SourceIconCache:
    def __init__(self, assets_dir: pathlib.Path) -> None:
        self.directory = assets_dir / "source-icons"
        self.locks: dict[str, asyncio.Lock] = {}
        self.retries: dict[str, float] = {}
        self.slots = asyncio.Semaphore(ICON_CONCURRENCY)

    async def get(self, domain: str) -> bytes | None:
        key = hashlib.sha256(domain.encode()).hexdigest()
        path = self.directory / f"{key}.png"

        async with self.locks.setdefault(domain, asyncio.Lock()):
            previous: bytes | None = None

            if path.is_file():
                previous = await asyncio.to_thread(path.read_bytes)

                if time.time() - path.stat().st_mtime < ICON_FRESH_SECONDS:
                    return previous

            if time.monotonic() < self.retries.get(domain, 0):
                return previous

            async with self.slots:
                try:
                    async with asyncio.timeout(ICON_CAPTURE_SECONDS):
                        icon = await _capture_icon(domain)
                except TimeoutError:
                    icon = None

            self.retries[domain] = time.monotonic() + ICON_RETRY_SECONDS

            if icon is not None:
                await asyncio.to_thread(_write_icon, path, icon)

                return icon

            return previous


SOURCE_ICON_CACHE = web.AppKey("source_icon_cache", SourceIconCache)


def add_source_icon_routes(app: web.Application) -> None:
    state: AppState = app["state"]
    app[SOURCE_ICON_CACHE] = SourceIconCache(state.assets_dir)
    app.router.add_get("/api/reader/source-icon", handle_source_icon)


async def handle_source_icon(request: web.Request) -> web.Response:
    domain = request.query.get("domain", "").strip().lower()

    if source_icon_url(domain) is None:
        raise web.HTTPNotFound()

    state: AppState = request.app["state"]

    if not any(source.domain == domain for source in state.settings.sources):
        client = request.app["clickhouse"]
        result = await client.fetch_json(
            "SELECT domain FROM posts FINAL WHERE domain = %(domain)s LIMIT 1",
            {"domain": domain},
        )

        if not result.rows:
            raise web.HTTPNotFound()

    icon = await request.app[SOURCE_ICON_CACHE].get(domain)

    if icon is None:
        raise web.HTTPNotFound()

    return web.Response(
        body=icon, content_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )
