from __future__ import annotations

import asyncio
import base64
import fcntl
import logging
import pathlib
import re
import shutil
from collections.abc import AsyncIterator, Iterable
from typing import IO, Any, NamedTuple

import zendriver
from zendriver import cdp

from dank.config import Settings

logger = logging.getLogger(__name__)
# Zendriver 0.17.1 sends SIGTERM immediately after CDP Browser.close.
# Let Chromium finish profile cleanup before handing control back to it.
BROWSER_SHUTDOWN_TIMEOUT_SECONDS = 5
BROWSER_SHUTDOWN_POLL_SECONDS = 0.1


class ChromiumConfig(zendriver.Config):
    """Keep Chromium's normal features while retaining driver transport."""

    def __call__(self) -> list[str]:
        # Restore features disabled by Zendriver's launch defaults.
        # https://zendriver.dev/reference/core/config/#zendriver.core.config.Config
        unnecessary_switches = {
            "--disable-breakpad", "--disable-dev-shm-usage",
            "--disable-features", "--disable-infobars", "--no-pings",
            "--no-service-autorun", "--remote-allow-origins",
        }

        return [
            arg for arg in super().__call__()
            if arg.partition("=")[0] not in unnecessary_switches
        ]


class NetworkResponse(NamedTuple):
    url: str
    status: int
    mime_type: str
    body: str
    request_id: str
    resource_type: str


class BrowserConfig(NamedTuple):
    headless: bool = False
    browser_executable_path: str | None = None
    connection_timeout: float | None = None
    connection_max_tries: int | None = None
    keep_open: bool = False
    profile_dir: pathlib.Path | None = None

    @classmethod
    def from_settings(
        cls, settings: Settings, *, headless: bool = False,
        keep_open: bool = False,
    ) -> BrowserConfig:
        return cls(
            headless=headless, keep_open=keep_open,
            profile_dir=settings.data_dir / "browser-profile",
            browser_executable_path=(
                str(settings.browser.executable_path)
                if settings.browser.executable_path else None
            ),
            connection_timeout=settings.browser.connection_timeout,
            connection_max_tries=settings.browser.connection_max_tries,
        )


class BrowserProfileBusyError(RuntimeError):
    pass


class BrowserSession:
    def __init__(self, config: BrowserConfig) -> None:
        self._config = config
        self._browser: zendriver.Browser | None = None
        self._lock = asyncio.Lock()
        self._profile_lock: IO[bytes] | None = None

    async def __aenter__(self) -> BrowserSession:
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        try:
            if exc_type is not None and self._config.keep_open:
                await self.hold_open()
        finally:
            await self.close()

    async def get_browser(self) -> zendriver.Browser:
        async with self._lock:
            if self._browser is None:
                options: dict[str, Any] = {
                    "headless": self._config.headless,
                    "disable_webrtc": False,
                }
                browser_path = _resolve_browser_executable(
                    self._config.browser_executable_path,
                )

                if browser_path is not None:
                    options["browser_executable_path"] = browser_path

                if self._config.profile_dir is not None:
                    self._acquire_profile()
                    options["user_data_dir"] = str(self._config.profile_dir)

                timeout = self._config.connection_timeout
                max_tries = self._config.connection_max_tries

                if timeout is not None:
                    options["browser_connection_timeout"] = timeout

                if max_tries is not None:
                    options["browser_connection_max_tries"] = max_tries

                self._browser = await zendriver.start(
                    config=ChromiumConfig(**options),
                )
                await self._browser.wait(0.5)

            return self._browser

    def _acquire_profile(self) -> None:
        profile = self._config.profile_dir

        if profile is None or self._profile_lock is not None:
            return

        profile.mkdir(parents=True, exist_ok=True, mode=0o700)
        profile.chmod(0o700)
        lock = (profile / ".dank.lock").open("a+b")

        try:
            # Kernel locks survive neither process exit nor container exit.
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise BrowserProfileBusyError(
                "Browser profile is in use. Finish the other sign-in or "
                "browser scrape, then retry.",
            ) from None

        self._profile_lock = lock

    async def close(self) -> None:
        browser = self._browser

        if browser is None:
            self._release_profile()
            return

        connection = browser.connection

        try:
            async with asyncio.timeout(BROWSER_SHUTDOWN_TIMEOUT_SECONDS):
                if (
                    not browser.stopped
                    and connection is not None
                    and not connection.closed
                ):
                    await connection.send(cdp.browser.close())

                    # Zendriver exposes process exit only as a polled property.
                    while not browser.stopped:  # noqa: ASYNC110
                        await asyncio.sleep(BROWSER_SHUTDOWN_POLL_SECONDS)

                    # Avoid a second Browser.close against the exited process.
                    await connection.aclose()
        except TimeoutError:
            logger.debug(
                "Browser graceful shutdown timed out; stopping driver",
            )

            if connection is not None:
                # Do not let driver cleanup resend the same stalled command.
                await connection.aclose()
        except Exception:
            logger.debug(
                "Browser graceful shutdown unavailable; stopping driver",
                exc_info=True,
            )
        finally:
            try:
                await browser.stop()
            finally:
                self._browser = None
                self._release_profile()

    def _release_profile(self) -> None:
        if self._profile_lock is not None:
            self._profile_lock.close()
            self._profile_lock = None

    async def hold_open(self) -> None:
        if self._browser is not None and not self._config.headless:
            logger.warning(
                "Scrape crashed; keeping browser open for inspection.",
            )
            await asyncio.Event().wait()

    @property
    def headless(self) -> bool:
        return self._config.headless


class NetworkCapture:
    def __init__(
        self,
        tab: zendriver.Tab,
        url_patterns: Iterable[str | re.Pattern[str]],
        resource_types: Iterable[cdp.network.ResourceType] | None = None,
    ) -> None:
        self._tab = tab
        self._patterns = [
            pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
            for pattern in url_patterns
        ]
        self._resource_types = tuple(
            resource_types
            if resource_types is not None else
            (
                cdp.network.ResourceType.XHR,
                cdp.network.ResourceType.FETCH,
            ),
        )
        self._pending: dict[
            cdp.network.RequestId,
            cdp.network.ResponseReceived,
        ] = {}
        self._queue: asyncio.Queue[NetworkResponse] = asyncio.Queue()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        tab: Any = self._tab
        tab.add_handler(cdp.network.ResponseReceived, self._on_response)
        tab.add_handler(
            cdp.network.LoadingFinished,
            self._on_loading_finished,
        )
        self._started = True

        try:
            await self._tab.send(cdp.network.enable())
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        if not self._started:
            return

        tab: Any = self._tab
        tab.remove_handlers(
            cdp.network.ResponseReceived,
            self._on_response,
        )
        tab.remove_handlers(
            cdp.network.LoadingFinished,
            self._on_loading_finished,
        )
        self._started = False

    async def drain(self, timeout_seconds: float) -> list[NetworkResponse]:
        responses: list[NetworkResponse] = []

        while True:
            try:
                async with asyncio.timeout(timeout_seconds):
                    response = await self._queue.get()
            except TimeoutError:
                break

            responses.append(response)

        return responses

    async def stream(self) -> AsyncIterator[NetworkResponse]:
        while True:
            yield await self._queue.get()

    async def _on_response(self, event: cdp.network.ResponseReceived) -> None:
        if event.type_ not in self._resource_types:
            return

        url = event.response.url

        if not self._matches(url):
            return

        logger.info(
            (
                "Captured matching response metadata request_id=%s "
                "status=%s type=%s url=%s"
            ),
            event.request_id,
            event.response.status,
            event.type_.value,
            url,
        )

        self._pending[event.request_id] = event

    async def _on_loading_finished(
        self,
        event: cdp.network.LoadingFinished,
    ) -> None:
        response = self._pending.pop(event.request_id, None)

        if response is None:
            return

        try:
            body, is_base64 = await self._tab.send(
                cdp.network.get_response_body(request_id=event.request_id),
            )
        except Exception:
            logger.debug("Failed to read response body", exc_info=True)
            return

        text = _decode_body(body, is_base64=is_base64)
        logger.info(
            "Queued matching response body request_id=%s bytes=%d url=%s",
            event.request_id,
            len(text),
            response.response.url,
        )
        await self._queue.put(
            NetworkResponse(
                url=response.response.url,
                status=response.response.status,
                mime_type=response.response.mime_type,
                body=text,
                request_id=str(event.request_id),
                resource_type=response.type_.value,
            ),
        )

    def _matches(self, url: str) -> bool:
        return any(pattern.search(url) for pattern in self._patterns)


def _decode_body(body: str, *, is_base64: bool) -> str:
    if not is_base64:
        return body

    decoded = base64.b64decode(body)

    return decoded.decode("utf-8", "replace")


def _resolve_browser_executable(explicit_path: str | None) -> str | None:
    if explicit_path:
        expanded = pathlib.Path(explicit_path).expanduser()

        if expanded.is_file():
            return str(expanded)

        resolved = shutil.which(explicit_path)

        if resolved:
            return resolved

    candidates = (
        "thorium-browser",
        "thorium",
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "brave-browser",
        "brave",
    )

    for candidate in candidates:
        resolved = shutil.which(candidate)

        if resolved:
            return resolved

    return None
