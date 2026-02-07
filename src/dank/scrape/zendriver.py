from __future__ import annotations

import asyncio
import base64
import logging
import pathlib
import re
import shutil
from collections.abc import AsyncIterator, Iterable
from typing import Any, NamedTuple

import zendriver
from zendriver import cdp

logger = logging.getLogger(__name__)


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


class BrowserSession:
    def __init__(self, config: BrowserConfig) -> None:
        self._config = config
        self._browser: zendriver.Browser | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> BrowserSession:
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        if exc_type is not None and self._config.keep_open:
            await self.hold_open()

        await self.close()

    async def get_browser(self) -> zendriver.Browser:
        async with self._lock:
            if self._browser is None:
                options: dict[str, Any] = {
                    "headless": self._config.headless,
                }
                browser_path = _resolve_browser_executable(
                    self._config.browser_executable_path,
                )

                if browser_path is not None:
                    options["browser_executable_path"] = browser_path

                if self._config.profile_dir is not None:
                    self._config.profile_dir.mkdir(parents=True, exist_ok=True)
                    options["user_data_dir"] = str(self._config.profile_dir)

                browser_args: list[str] = []

                if browser_args:
                    options["browser_args"] = browser_args

                timeout = self._config.connection_timeout
                max_tries = self._config.connection_max_tries

                if timeout is not None:
                    options["browser_connection_timeout"] = timeout

                if max_tries is not None:
                    options["browser_connection_max_tries"] = max_tries

                self._browser = await zendriver.start(**options)
                await self._browser.wait(0.5)

            return self._browser

    async def close(self) -> None:
        if self._browser is not None:
            await self._browser.stop()

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
        await self._tab.send(cdp.network.enable())
        self._started = True

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
