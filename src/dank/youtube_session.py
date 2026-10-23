"""Private persistence for a verified Google/YouTube browser session."""

from __future__ import annotations

import asyncio
import json
import math
import os
import pathlib
import re
import stat
import tempfile
import time
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

import zendriver
from zendriver import cdp
from zendriver.core.connection import ProtocolException

MAX_CACHE_BYTES = 1024 * 1024
MAX_COOKIES = 512
COOKIE_CAPTURE_TIMEOUT_SECONDS = 15.0
COOKIE_REQUEST_SECONDS = 10
AUTH_NAMES = {
    "SID", "SAPISID", "__Secure-1PSID", "__Secure-3PSID",
    "__Secure-1PAPISID", "__Secure-3PAPISID",
}
COOKIE_KEYS = {
    "name", "value", "domain", "path", "secure", "httpOnly", "sameSite",
    "expires", "priority", "sourceScheme", "sourcePort", "partitionKey", "url",
}


def _trusted_domain(value: object) -> bool:
    if not isinstance(value, str):
        return False

    host = value.removeprefix(".").lower()

    return bool(re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)+", host)) and any(
        host == base or host.endswith("." + base)
        for base in ("google.com", "youtube.com")
    )


def _partition(value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError

    data = cast(dict[str, object], value)

    if set(data) != {"topLevelSite", "hasCrossSiteAncestor"}:
        raise ValueError

    site = urlsplit(str(data["topLevelSite"]))

    if (site.scheme != "https" or not _trusted_domain(site.hostname)
            or site.username or site.password or site.port not in {None, 443}
            or site.path not in {"", "/"} or site.query or site.fragment
            or type(data["hasCrossSiteAncestor"]) is not bool):
        raise ValueError


def _attributes(data: dict[str, Any]) -> None:
    for key in ("secure", "httpOnly"):
        if type(data.get(key)) is not bool:
            raise ValueError

    for key, choices in (
        ("sameSite", {"Strict", "Lax", "None"}),
        ("priority", {"Low", "Medium", "High"}),
        ("sourceScheme", {"Unset", "NonSecure", "Secure"}),
    ):
        if key in data and data[key] not in choices:
            raise ValueError

    if "sourcePort" in data and (
        type(data["sourcePort"]) is not int
        or not (data["sourcePort"] == -1 or 1 <= data["sourcePort"] <= 65535)
    ):
        raise ValueError

    if "partitionKey" in data:
        _partition(data["partitionKey"])


def _scope(data: dict[str, Any]) -> None:
    domain = data.get("domain")
    url = data.get("url")

    if bool(domain) == bool(url):
        raise ValueError

    if domain:
        if not _trusted_domain(domain) or not str(domain).startswith("."):
            raise ValueError

        return

    if not isinstance(url, str):
        raise ValueError

    parsed = urlsplit(url)

    if (parsed.scheme not in {"http", "https"}
            or not _trusted_domain(parsed.hostname) or parsed.username
            or parsed.password or parsed.port is not None
            or parsed.query or parsed.fragment
            or parsed.path != data.get("path")
            or (data.get("secure") and parsed.scheme != "https")):
        raise ValueError


def _cookie(value: object, now: float) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise ValueError

    data = cast(dict[str, Any], value)

    if set(data) - COOKIE_KEYS:
        raise ValueError

    for key in ("name", "value", "path"):
        field = data.get(key)

        if (not isinstance(field, str) or len(field.encode()) > 16384
                or any(ord(char) < 32 or ord(char) == 127 for char in field)):
            raise ValueError

    if not data["name"] or not data["path"].startswith("/"):
        raise ValueError

    _attributes(data)
    _scope(data)

    if "expires" in data:
        expiry = data["expires"]

        if (not isinstance(expiry, int | float) or isinstance(expiry, bool)
                or not math.isfinite(expiry) or expiry < 0):
            raise ValueError

        if expiry <= now:
            return None

    return data


def _authenticated(cookies: list[dict[str, Any]]) -> bool:
    return any(cookie["name"] in AUTH_NAMES and cookie["value"]
               for cookie in cookies)


def _write_cache(path: pathlib.Path, cookies: list[dict[str, Any]]) -> None:
    data = json.dumps({"version": 1, "cookies": cookies}).encode()

    if len(data) > MAX_CACHE_BYTES or path.is_symlink():
        raise ValueError

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, name = tempfile.mkstemp(
        prefix=".youtube-session-", dir=path.parent,
    )
    temporary = pathlib.Path(name)

    try:
        with os.fdopen(descriptor, "wb") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())

        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_cache(path: pathlib.Path) -> list[dict[str, Any]]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)

    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())

        if (not stat.S_ISREG(metadata.st_mode)
                or metadata.st_mode & 0o077 or metadata.st_uid != os.geteuid()
                or metadata.st_size > MAX_CACHE_BYTES):
            raise ValueError

        payload = source.read(MAX_CACHE_BYTES + 1)

        if len(payload) > MAX_CACHE_BYTES:
            raise ValueError

        decoded = json.loads(payload)

    if not isinstance(decoded, dict):
        raise ValueError

    document = cast(dict[str, Any], decoded)

    if (type(document.get("version")) is not int or document["version"] != 1
            or set(document) != {"version", "cookies"}):
        raise ValueError

    values: Any = document["cookies"]

    if not isinstance(values, list):
        raise ValueError

    rows = cast(list[object], values)

    if len(rows) > MAX_COOKIES:
        raise ValueError

    now = time.time()
    cookies = [parsed for value in rows
               if (parsed := _cookie(value, now)) is not None]

    if not _authenticated(cookies):
        raise ValueError

    return cookies


async def save_youtube_session(
    browser: zendriver.Browser, path: pathlib.Path,
) -> bool:
    """Save only after the caller has verified a signed-in YouTube page."""
    try:
        now = time.time()
        cookies: list[dict[str, Any]] = []

        captured = await asyncio.wait_for(
            browser.cookies.get_all(), timeout=COOKIE_REQUEST_SECONDS,
        )

        for cookie in captured:
            if (not _trusted_domain(cookie.domain)
                    or cookie.partition_key_opaque):
                continue

            data = {key: value for key, value in cookie.to_json().items()
                    if key in COOKIE_KEYS}

            if cookie.session:
                data.pop("expires", None)
            elif cookie.expires is None:
                continue

            if not cookie.domain.startswith("."):
                https = (cookie.secure or cookie.source_scheme
                         == cdp.network.CookieSourceScheme.SECURE)
                scheme = "https" if https else "http"
                data["url"] = f"{scheme}://{cookie.domain}{cookie.path}"
                data.pop("domain")

            if (parsed := _cookie(data, now)) is not None:
                cookies.append(parsed)

        if len(cookies) > MAX_COOKIES or not _authenticated(cookies):
            return False

        await asyncio.to_thread(_write_cache, path, cookies)

        return True
    except Exception:
        # Cookie APIs and JSON errors can contain authentication values.
        return False


async def restore_youtube_session(
    browser: zendriver.Browser, path: pathlib.Path,
) -> bool:
    try:
        cookies = await asyncio.to_thread(_read_cache, path)
        await asyncio.wait_for(browser.cookies.set_all([
            cdp.network.CookieParam.from_json(cookie) for cookie in cookies
        ]), timeout=COOKIE_REQUEST_SECONDS)

        return True
    except Exception:
        return False


async def save_youtube_session_from_page(
    browser: zendriver.Browser, page: zendriver.Tab, path: pathlib.Path,
) -> bool:
    """Stop page scripts before saving the current authenticated cookies."""
    try:
        async with asyncio.timeout(COOKIE_CAPTURE_TIMEOUT_SECONDS):
            verified = await page.evaluate(
                "location.origin === 'https://www.youtube.com' && "
                "window.ytcfg?.get?.('LOGGED_IN') === true",
            )

            if verified is not True:
                return False

            # YouTube rotates cookies in open tabs. Stop its page scripts
            # before capturing the final snapshot, as recommended upstream:
            # https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies
            previous = await page.evaluate("performance.timeOrigin")
            await page.get("https://www.youtube.com/robots.txt")

            while await page.evaluate(  # noqa: ASYNC110
                "location.origin === 'https://www.youtube.com' && "
                "location.pathname === '/robots.txt' && "
                "document.readyState === 'complete' && "
                f"performance.timeOrigin !== {json.dumps(previous)}",
            ) is not True:
                await asyncio.sleep(0.1)

            return await save_youtube_session(browser, path)
    except Exception:
        return False


SESSION_POLL_SECONDS = 0.5
SESSION_STATE_SCRIPT = """JSON.stringify((() => {
    if (location.origin !== 'https://www.youtube.com' ||
        location.pathname !== '/' || document.readyState !== 'complete')
        return {signed_in: null, document: null};
    const state = window.ytcfg?.get?.('LOGGED_IN');
    return {signed_in: typeof state === 'boolean' ? state : null,
        document: performance.timeOrigin};
})())"""


class SessionState(NamedTuple):
    signed_in: bool | None = None
    document: float | None = None


async def read_youtube_session_state(page: zendriver.Tab) -> SessionState:
    """Only a complete YouTube homepage can establish an auth decision."""
    try:
        decoded = json.loads(str(await page.evaluate(SESSION_STATE_SCRIPT)))

        if not isinstance(decoded, dict):
            return SessionState()

        data = cast(dict[str, object], decoded)
        marker = data.get("document")
        signed_in = data.get("signed_in")

        if (type(signed_in) is not bool
                or not isinstance(marker, int | float)
                or isinstance(marker, bool) or not math.isfinite(marker)
                or marker <= 0):
            return SessionState()

        return SessionState(signed_in, float(marker))
    except (ValueError, ProtocolException, ConnectionError):
        return SessionState()


async def _wait_youtube_state(
    page: zendriver.Tab, previous_document: float | None = None,
) -> SessionState:
    while True:
        state = await read_youtube_session_state(page)

        if (state.signed_in is not None and state.document is not None
                and state.document != previous_document):
            return state

        await asyncio.sleep(SESSION_POLL_SECONDS)


class YouTubeSession:
    """Keep native auth authoritative; try a saved cookie jar at most once."""

    def __init__(
        self, browser: zendriver.Browser, cache_path: pathlib.Path | None,
    ) -> None:
        self.browser = browser
        self.cache_path = cache_path
        self.restore_attempted = False
        self._lock = asyncio.Lock()
        self._pending_document: float | None = None

    async def check(
        self, page: zendriver.Tab, *, timeout_seconds: float = 15,
    ) -> bool | None:
        try:
            async with asyncio.timeout(timeout_seconds), self._lock:
                state = await _wait_youtube_state(page, self._pending_document)
                self._pending_document = None

                if (state.signed_in is True or self.restore_attempted
                        or self.cache_path is None):
                    return state.signed_in

                # Mark first so cancellation cannot replay stale cookies.
                self.restore_attempted = True

                if not await restore_youtube_session(
                    self.browser, self.cache_path,
                ):
                    return False

                self._pending_document = state.document
                await page.reload(ignore_cache=True)
                refreshed = await _wait_youtube_state(page, state.document)
                self._pending_document = None

                return refreshed.signed_in
        except (TimeoutError, ProtocolException, ConnectionError):
            return None
