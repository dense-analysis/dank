from __future__ import annotations

import asyncio
import datetime
import time
from collections import Counter
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit

import aiohttp

from dank.config import ScrapeSettings
from dank.scrape.metrics import request_metrics, retry_sleep

MAX_COOLDOWN_WAIT = 30.0
# Security policies can exceed aiohttp's default 8 KiB header field limit.
MAX_HEADER_FIELD_BYTES = 16 * 1024


class HostCooldownError(Exception):
    pass


class RequestLimiter:
    def __init__(self, settings: ScrapeSettings) -> None:
        self.settings = settings
        self.media = asyncio.Semaphore(settings.media_concurrency)
        self._condition = asyncio.Condition()
        self._active = 0
        self._hosts: Counter[str] = Counter()
        self._cooldowns: dict[str, float] = {}

    def cool_down(self, url: str, delay: float) -> None:
        host = urlsplit(url).hostname or ""
        self._cooldowns[host] = max(
            self._cooldowns.get(host, 0), time.monotonic() + delay,
        )

    @asynccontextmanager
    async def slot(self, url: str) -> AsyncGenerator[None]:
        host = urlsplit(url).hostname or ""
        await self._acquire(host)

        try:
            yield
        finally:
            async with self._condition:
                self._active -= 1
                self._hosts[host] -= 1
                self._condition.notify_all()

    async def _acquire(self, host: str) -> None:
        wait_until: float | None = None

        while True:
            async with self._condition:
                now = time.monotonic()
                delay = self._cooldowns.get(host, 0) - now

                if delay <= 0:
                    if (
                        self._active < self.settings.http_concurrency
                        and self._hosts[host] < self.settings.http_per_host
                    ):
                        self._active += 1
                        self._hosts[host] += 1

                        return

                    await self._condition.wait()
                    continue

            # Cooldowns never occupy HTTP slots or the condition lock.
            if wait_until is None:
                wait_until = now + MAX_COOLDOWN_WAIT

            if now + delay > wait_until:
                raise HostCooldownError(
                    f"{host}: shared cooldown exceeds the 30s wait budget",
                )

            await retry_sleep(delay)


current_limiter: ContextVar[RequestLimiter | None] = ContextVar(
    "scrape_request_limiter", default=None,
)


def retry_after_seconds(value: str | None) -> float:
    if not value:
        return 0.0

    value = value.strip()

    if value.isascii() and value.isdecimal():
        return float(value)

    try:
        date = parsedate_to_datetime(value)

        if date.tzinfo is not None:
            return max(
                0.0,
                (date - datetime.datetime.now(datetime.UTC)).total_seconds(),
            )
    except (TypeError, ValueError, OverflowError):
        pass

    return 0.0


@asynccontextmanager
async def http_response(
    client: aiohttp.ClientSession, url: str, *,
    headers: dict[str, str] | None = None,
    retry: int = 0, max_redirects: int = 5,
) -> AsyncGenerator[aiohttp.ClientResponse]:
    limiter = current_limiter.get()

    if limiter is None:
        with request_metrics(retry=retry > 0):
            async with client.get(
                url, headers=headers, max_redirects=max_redirects,
                max_field_size=MAX_HEADER_FIELD_BYTES,
            ) as response:
                yield response

        return

    # Acquire the destination host's limit at every redirect hop.
    for hop in range(max_redirects + 1):
        async with limiter.slot(url):
            with request_metrics(retry=retry > 0):
                async with client.get(
                    url, headers=headers, allow_redirects=False,
                    max_field_size=MAX_HEADER_FIELD_BYTES,
                ) as response:
                    if response.status == 429:
                        limiter.cool_down(url, max(
                            2.0 * 2 ** retry,
                            retry_after_seconds(
                                response.headers.get("Retry-After"),
                            ),
                        ))

                    if response.status not in {301, 302, 303, 307, 308}:
                        yield response

                        return

                    location = response.headers.get("Location")

                    if not location:
                        yield response

                        return

                    if hop == max_redirects:
                        raise aiohttp.TooManyRedirects(
                            response.request_info, (),
                        )

                    destination = urljoin(str(response.url), location)

                    if urlsplit(destination).scheme not in {"http", "https"}:
                        raise ValueError("Redirect must use HTTP(S)")

                    if urlsplit(destination).netloc != urlsplit(url).netloc:
                        headers = {
                            key: value
                            for key, value in (headers or {}).items()
                            if key.lower() != "authorization"
                        }

                    url = destination
