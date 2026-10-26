"""Private in-memory sessions and bounded subprocesses for yt-dlp listings."""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from http.cookiejar import Cookie, CookieJar
from typing import Any, cast

import zendriver
from zendriver import cdp

MAX_EVENT_BYTES = 2 * 1024 * 1024


AUTH_COOKIE_NAMES = {
    "SID",
    "SAPISID",
    "__Secure-1PAPISID",
    "__Secure-3PAPISID",
}


def has_youtube_session(cookies: list[dict[str, object]]) -> bool:
    return any(
        cookie.get("name") in AUTH_COOKIE_NAMES
        and cookie.get("value")
        and str(cookie.get("domain", "")).lstrip(".")
        in {
            "youtube.com",
            "www.youtube.com",
        }
        for cookie in cookies
    )


def cookie_records(
    cookies: Sequence[Mapping[str, object]] | CookieJar,
) -> list[dict[str, object]]:
    if isinstance(cookies, CookieJar):
        return [
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": cookie.secure,
                "expires": cookie.expires,
                "httpOnly": cookie.has_nonstandard_attr("HttpOnly"),
            }
            for cookie in cookies
        ]

    return [dict(cookie) for cookie in cookies]


def cookie_jar(records: list[dict[str, Any]]) -> CookieJar:
    from yt_dlp.cookies import YoutubeDLCookieJar

    jar = YoutubeDLCookieJar()

    for record in records:
        domain = str(record["domain"])

        if not any(
            domain.lstrip(".") == host or domain.endswith("." + host)
            for host in ("youtube.com", "google.com")
        ):
            continue

        expires = record.get("expires")
        expiry = int(expires) if isinstance(expires, (int, float)) else 0
        jar.set_cookie(
            Cookie(
                version=0,
                name=str(record["name"]),
                value=str(record["value"]),
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=domain.startswith("."),
                domain_initial_dot=domain.startswith("."),
                path=str(record.get("path") or "/"),
                path_specified=True,
                secure=bool(record.get("secure", True)),
                expires=expiry if expiry > 0 else None,
                discard=expiry <= 0,
                comment=None,
                comment_url=None,
                rest={"HttpOnly": ""} if record.get("httpOnly") else {},
                rfc2109=False,
            ),
        )

    return jar


async def session_cookies(page: zendriver.Tab) -> list[dict[str, Any]]:
    async with asyncio.timeout(5):
        return [
            cookie.to_json()
            for cookie in await page.send(cdp.storage.get_cookies())
            if any(
                cookie.domain.lstrip(".") == domain
                or cookie.domain.endswith("." + domain)
                for domain in ("youtube.com", "google.com")
            )
        ]


async def run_worker(
    module: str,
    request: dict[str, Any],
    timeout_seconds: float,
    consume: Callable[[dict[str, Any]], None],
) -> int:
    process: asyncio.subprocess.Process | None = None

    try:
        async with asyncio.timeout(timeout_seconds):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                module,
                "--worker",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=MAX_EVENT_BYTES,
            )
            assert process.stdin is not None
            assert process.stdout is not None
            process.stdin.write(json.dumps(request).encode())
            await process.stdin.drain()
            process.stdin.close()
            request.clear()

            while line := await process.stdout.readline():
                consume(cast(dict[str, Any], json.loads(line)))

            return await process.wait()
    finally:
        request.clear()

        if process is not None and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass

            await process.wait()


def emit(event: dict[str, Any]) -> None:
    print(json.dumps(event, separators=(",", ":"), default=str), flush=True)


class WorkerLogger:
    """Provider messages can contain private request data; emit categories."""

    def __init__(
        self,
        output: Callable[[dict[str, Any]], None] = emit,
    ) -> None:
        self.emit = output

    def debug(self, message: str) -> None:
        if "feed looping" in message.casefold():
            self.emit(
                {"event": "warning", "category": "repeated_continuation"},
            )

    def warning(self, message: str) -> None:
        category = (
            "expired_session"
            if "cookies are no longer valid" in message.casefold()
            else "extractor_warning"
        )
        self.emit({"event": "warning", "category": category})

    def error(self, message: str) -> None:
        category = (
            "feed_redirected_to_home"
            if "redirected to youtube.com home" in message.casefold()
            else "extractor_error"
        )
        self.emit({"event": "warning", "category": category})
