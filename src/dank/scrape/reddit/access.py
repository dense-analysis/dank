from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlsplit

from dank.scrape.http import current_limiter, retry_after_seconds
from dank.scrape.metrics import count
from dank.scrape.zendriver import NetworkResponse

# Avoid immediately repeating browser requests when Reddit omits Retry-After.
DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS = 30.0


class RedditScrapeError(RuntimeError):
    """A Reddit page could not be collected without user intervention."""


class RedditRateLimitError(RedditScrapeError):
    pass


def check_response_status(
    status: int, url: str, retry_after: str | None = None,
) -> None:
    if status == 429:
        count("http_429")
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
            "/svc/shreddit/community-more-posts/",
        )

        if is_document or is_pagination:
            check_response_status(
                response.status, response.url, response.retry_after,
            )
