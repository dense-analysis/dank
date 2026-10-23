"""Isolated metadata-only :ytrec capture for comparison with the browser."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from collections.abc import Mapping, Sequence
from http.cookiejar import Cookie, CookieJar
from typing import Any, Literal, NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

MAX_TIMEOUT_SECONDS = 240.0
MAX_ENTRIES = 10000
MAX_EVENT_BYTES = 2 * 1024 * 1024
AUTH_COOKIE_NAMES = {
    "SID", "SAPISID", "__Secure-1PAPISID", "__Secure-3PAPISID",
}
LIMITATIONS = (
    "Flat entries omit shelf provenance; news shelves cannot be reliably "
    "excluded from this comparison capture.",
    "Posts may be omitted or represented only by their linked videos.",
    "Shorts classification depends on the flat entry retaining a Shorts URL.",
    "A finished extractor does not prove the browser homepage was exhausted.",
)


class YtDlpHomeEntry(NamedTuple):
    identity: str
    kind: Literal["video", "short", "post", "unknown"]
    position: int
    raw: dict[str, Any]


class YtDlpHomeResult(NamedTuple):
    entries: tuple[YtDlpHomeEntry, ...]
    elapsed_seconds: float
    first_entry_seconds: float | None
    outcome: Literal["completed", "timeout", "item_limit", "failed"]
    error_type: str = ""
    warnings: tuple[str, ...] = ()
    limitations: tuple[str, ...] = LIMITATIONS


def classify_entry(raw: dict[str, Any], position: int) -> YtDlpHomeEntry:
    """Keep each flat entry and classify its supported identity."""
    url = str(raw.get("url") or raw.get("webpage_url") or "")
    parsed = urlsplit(url)
    host = (parsed.hostname or "").removeprefix("www.")
    path = parsed.path
    identifier = str(raw.get("id") or "")

    if host == "youtube.com" and path.startswith("/post/"):
        identifier = path.removeprefix("/post/").split("/")[0]

        return YtDlpHomeEntry(f"post:{identifier}", "post", position, raw)

    if host == "youtube.com" and path.startswith("/shorts/"):
        identifier = path.removeprefix("/shorts/").split("/")[0]
        kind = "short"
    elif host == "youtube.com" and path == "/watch":
        identifier = parse_qs(parsed.query).get("v", [identifier])[0]
        kind = "video"
    elif host == "youtu.be":
        identifier = path.strip("/")
        kind = "video"
    elif raw.get("ie_key") == "Youtube" and not host:
        kind = "video"
    else:
        return YtDlpHomeEntry("", "unknown", position, raw)

    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", identifier):
        return YtDlpHomeEntry("", "unknown", position, raw)

    return YtDlpHomeEntry(
        f"video:{identifier}", kind,
        position, raw,
    )


def _cookie_records(
    cookies: Sequence[Mapping[str, object]] | CookieJar,
) -> list[dict[str, object]]:
    if isinstance(cookies, CookieJar):
        return [{
            "name": cookie.name, "value": cookie.value,
            "domain": cookie.domain, "path": cookie.path,
            "secure": cookie.secure, "expires": cookie.expires,
            "httpOnly": cookie.has_nonstandard_attr("HttpOnly"),
        } for cookie in cookies]

    return [dict(cookie) for cookie in cookies]


def _youtube_session(cookies: list[dict[str, object]]) -> bool:
    return any(
        cookie.get("name") in AUTH_COOKIE_NAMES and cookie.get("value")
        and str(cookie.get("domain", "")).lstrip(".") in {
            "youtube.com", "www.youtube.com",
        }
        for cookie in cookies
    )


class _Capture:
    def __init__(self, started: float) -> None:
        self.started = started
        self.entries: list[YtDlpHomeEntry] = []
        self.warnings: list[str] = []
        self.first_entry_seconds: float | None = None
        self.outcome: Literal[
            "completed", "timeout", "item_limit", "failed",
        ] = "failed"
        self.error_type = "WorkerExit"

    def consume(self, event: dict[str, Any], now: float) -> None:
        match event.get("event"):
            case "entry":
                raw = event["raw"]

                if not isinstance(raw, dict):
                    raise ValueError("Invalid flat entry")

                self.entries.append(classify_entry(
                    cast(dict[str, Any], raw), len(self.entries) + 1,
                ))
                if self.first_entry_seconds is None:
                    self.first_entry_seconds = now - self.started
            case "warning":
                self.warnings.append(str(event["category"]))
            case "done":
                outcome = event["outcome"]

                if outcome not in {"completed", "item_limit"}:
                    raise ValueError("Invalid worker completion event")

                self.outcome = outcome
                self.error_type = ""
            case "error":
                self.outcome = "failed"
                self.error_type = str(event["error_type"])
            case _:
                raise ValueError("Unknown worker event")

    def result(self, now: float) -> YtDlpHomeResult:
        return YtDlpHomeResult(
            tuple(self.entries), now - self.started, self.first_entry_seconds,
            self.outcome, self.error_type, tuple(dict.fromkeys(self.warnings)),
        )


def _validate_limits(timeout_seconds: float, max_entries: int, region: str):
    if not 0 < timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError("Recommendation timeout must be within 240 seconds")

    if not 0 < max_entries <= MAX_ENTRIES:
        raise ValueError("Recommendation entry limit must be within 10000")

    if not re.fullmatch(r"[A-Z]{2}", region):
        raise ValueError("Recommendation region must be a two-letter code")


async def collect_ytdlp_home(
    cookies: Sequence[Mapping[str, object]] | CookieJar,
    *,
    timeout_seconds: float = MAX_TIMEOUT_SECONDS,
    max_entries: int = MAX_ENTRIES,
    region: str = "GB",
) -> YtDlpHomeResult:
    """Use supplied cookies and kill the worker on timeout or cancellation."""
    _validate_limits(timeout_seconds, max_entries, region)
    records = _cookie_records(cookies)

    if not _youtube_session(records):
        raise ValueError("A verified YouTube session snapshot is required")

    loop = asyncio.get_running_loop()
    started = loop.time()
    capture = _Capture(started)
    process: asyncio.subprocess.Process | None = None

    try:
        async with asyncio.timeout(timeout_seconds):
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", __name__, "--worker",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, limit=MAX_EVENT_BYTES,
            )
            assert process.stdin is not None
            assert process.stdout is not None
            process.stdin.write(json.dumps({
                "cookies": records, "max_entries": max_entries,
                "region": region,
            }).encode())
            await process.stdin.drain()
            process.stdin.close()
            records.clear()

            while line := await process.stdout.readline():
                event = cast(dict[str, Any], json.loads(line))

                capture.consume(event, loop.time())

            await process.wait()

            if process.returncode != 0:
                capture.outcome = "failed"
                capture.error_type = capture.error_type or "WorkerExit"
    except TimeoutError:
        capture.outcome, capture.error_type = "timeout", "TimeoutError"
    except (ValueError, OSError, KeyError, TypeError) as error:
        capture.outcome, capture.error_type = "failed", type(error).__name__
    finally:
        records.clear()

        if process is not None and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass

            await process.wait()

    return capture.result(loop.time())


def _cookie_jar(records: list[dict[str, Any]]) -> CookieJar:
    from yt_dlp.cookies import YoutubeDLCookieJar

    jar = YoutubeDLCookieJar()

    for record in records:
        domain = str(record["domain"])

        if not any(domain.lstrip(".") == host or domain.endswith("." + host)
                   for host in ("youtube.com", "google.com")):
            continue

        expires = record.get("expires")
        expiry = int(expires) if isinstance(expires, (int, float)) else 0
        jar.set_cookie(Cookie(
            version=0, name=str(record["name"]), value=str(record["value"]),
            port=None, port_specified=False, domain=domain,
            domain_specified=domain.startswith("."),
            domain_initial_dot=domain.startswith("."),
            path=str(record.get("path") or "/"), path_specified=True,
            secure=bool(record.get("secure", True)),
            expires=expiry if expiry > 0 else None, discard=expiry <= 0,
            comment=None, comment_url=None,
            rest={"HttpOnly": ""} if record.get("httpOnly") else {},
            rfc2109=False,
        ))

    return jar


def _emit(event: dict[str, Any]) -> None:
    print(json.dumps(event, separators=(",", ":"), default=str), flush=True)


class _WorkerLogger:
    """Provider messages can contain private request data; emit categories."""

    def debug(self, message: str) -> None:
        if "feed looping" in message.casefold():
            _emit({"event": "warning", "category": "repeated_continuation"})

    def warning(self, message: str) -> None:
        _emit({"event": "warning", "category": "extractor_warning"})

    def error(self, message: str) -> None:
        _emit({"event": "warning", "category": "extractor_error"})


def _worker() -> None:
    from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

    request = cast(dict[str, Any], json.load(sys.stdin))
    cookies = _cookie_jar(request["cookies"])
    options: dict[str, Any] = {
        "extract_flat": True, "lazy_playlist": True, "skip_download": True,
        "cachedir": False, "quiet": True, "no_warnings": False,
        "logger": _WorkerLogger(), "socket_timeout": 15,
        "retries": 0, "extractor_retries": 0,
        "extractor_args": {"youtube": {"lang": ["en-GB"]}},
    }

    try:
        with YoutubeDL(cast(Any, options)) as provider:
            downloader = cast(Any, provider)
            # Inject only the caller's cookie snapshot; never load a profile.
            downloader.cookiejar = cookies
            result = downloader.extract_info(":ytrec", download=False,
                                             process=False)

            # :ytrec is a redirect. Resolve that one listing, never entry URLs.
            if result.get("_type") == "url":
                url = str(result["url"])

                if url != "https://www.youtube.com/feed/recommended":
                    raise ValueError("Unexpected recommendation redirect")

                url += f"?gl={request['region']}&hl=en-GB"
                result = downloader.extract_info(url, download=False,
                                                 process=False)

            for position, entry in enumerate(result.get("entries", ()), 1):
                _emit({"event": "entry", "raw": entry})

                if position >= request["max_entries"]:
                    _emit({"event": "done", "outcome": "item_limit"})
                    return

            _emit({"event": "done", "outcome": "completed"})
    except Exception as error:
        _emit({"event": "error", "error_type": type(error).__name__})


if __name__ == "__main__" and sys.argv[1:] == ["--worker"]:
    _worker()
