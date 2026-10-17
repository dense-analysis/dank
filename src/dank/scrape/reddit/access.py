from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar
from urllib.parse import urlsplit

from dank.scrape.http import current_limiter, retry_after_seconds
from dank.scrape.metrics import count
from dank.scrape.zendriver import NetworkResponse

# Avoid immediately repeating browser requests when Reddit omits Retry-After.
DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS = 300.0
MAX_REDDIT_COOLDOWN_WAIT_SECONDS = 600.0
# A run adapts upward after a real 429, rather than taxing every healthy visit.
MAX_ADAPTIVE_INTERVAL_SECONDS = 60.0
logger = logging.getLogger(__name__)


class RedditRequests:
    """One pacing clock and endpoint capability per collection run."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.json_available = True
        self.documents_available = True
        self._next_request = 0.0
        self._lock = asyncio.Lock()
        self.requests = 0

    async def wait(self) -> None:
        async with self._lock:
            delay = self._next_request - time.monotonic()

            if delay > 0:
                await asyncio.sleep(delay)

            self._next_request = time.monotonic() + self.interval
            self.requests += 1


current_requests: ContextVar[RedditRequests | None] = ContextVar(
    "reddit_requests", default=None,
)


@asynccontextmanager
async def request_slot(url: str) -> AsyncGenerator[None]:
    limiter = current_limiter.get()
    slot = limiter.slot(
        url, max_cooldown_wait=MAX_REDDIT_COOLDOWN_WAIT_SECONDS,
    ) if limiter else nullcontext()

    async with slot:
        await pace_request()
        yield


async def pace_request() -> None:
    requests = current_requests.get()

    if requests is not None:
        await requests.wait()



class RedditScrapeError(RuntimeError):
    """A Reddit page could not be collected without user intervention."""


class RedditRateLimitError(RedditScrapeError):
    pass


def check_response_status(
    status: int, url: str, retry_after: str | None = None,
) -> None:
    if status == 429:
        count("http_429")
        requests = current_requests.get()

        if requests is not None:
            requests.interval = min(
                MAX_ADAPTIVE_INTERVAL_SECONDS, requests.interval * 2,
            )
            logger.warning(
                "Reddit pacing increased to %.1fs after HTTP 429",
                requests.interval,
            )

        delay = max(
            DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS,
            retry_after_seconds(retry_after),
        )
        limiter = current_limiter.get()

        if limiter is not None:
            limiter.cool_down(url, delay)

        raise RedditRateLimitError(
            f"Reddit HTTP 429 (rate limited); host cooldown {delay:.0f}s. "
            "This is not an empty listing.",
        )

    if status >= 400:
        raise RedditScrapeError(
            f"Reddit HTTP {status} for {urlsplit(url).path}",
        )


def check_page_responses(
    responses: Iterable[NetworkResponse], request_url: str,
) -> None:
    requested = urlsplit(request_url)

    for response in responses:
        location = urlsplit(response.url)

        if location.hostname != requested.hostname:
            continue

        is_document = (
            response.resource_type == "Document"
            and location.path.rstrip("/") == requested.path.rstrip("/")
        )
        is_pagination = location.path.startswith(
            ("/svc/shreddit/community-more-posts/",
             "/svc/shreddit/more-comments/"),
        )

        if is_document or is_pagination:
            check_response_status(
                response.status, response.url, response.retry_after,
            )
