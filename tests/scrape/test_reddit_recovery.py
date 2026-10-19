import datetime as dt
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlencode

import pytest

from dank.config import (
    RedditSettings,
    RedditSort,
    RedditTimeFilter,
    ScrapeSettings,
    SourceConfig,
)
from dank.model import RawPost
from dank.scrape.http import HostCooldownError, RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.reddit import (
    _retry_community,  # pyright: ignore[reportPrivateUsage]
    scrape_reddit_communities,
)
from dank.scrape.reddit.access import (
    RedditAccessBlockedError,
    RedditListingUnavailableError,
    RedditRateLimitError,
    RedditRequests,
    RedditScrapeError,
    check_listing_dialogs,
    check_page_responses,
    current_requests,
    request_slot,
)
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import NetworkResponse

URL = "https://www.reddit.com/r/LLMDevs/top/?t=day"
BLOCK = "<p>You&#39;ve been blocked by network security.</p>"


def modal(
    community: str = "LLMDevs", host: str = "www.reddit.com",
    prefix: str = "r",
) -> NetworkResponse:
    query = urlencode(
        {
            "params": urlencode(
                {"prefix": prefix, "subredditName": community, "sort": "top"},
            ),
        },
    )
    return NetworkResponse(
        f"https://{host}/svc/shreddit/partial/token/community-not-found-modal?{query}",
        200,
        "text/html",
        "<div>Community unavailable</div>",
        "modal",
        "Fetch",
    )


def test_successful_http_error_dialog_is_not_an_empty_listing():
    with pytest.raises(
        RedditListingUnavailableError, match="community-not-found",
    ):
        check_listing_dialogs([modal()], URL)


@pytest.mark.parametrize(
    "response", [modal("other"), modal(host="other.test"), modal(prefix="u")],
)
def test_unrelated_modal_does_not_fail_current_community(
    response: NetworkResponse,
):
    check_listing_dialogs([response], URL)


async def test_security_block_exceeds_wait_budget_and_is_not_a_429(
    monkeypatch: pytest.MonkeyPatch,
):
    now = [0.0]
    monkeypatch.setattr(
        "dank.scrape.http.time", SimpleNamespace(monotonic=lambda: now[0]),
    )
    delays: list[float] = []

    async def sleep(delay: float):
        delays.append(delay)
        now[0] += delay

    monkeypatch.setattr("dank.scrape.http.retry_sleep", sleep)
    limiter = RequestLimiter(ScrapeSettings())
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    token = current_limiter.set(limiter)
    source_token = current_source.set(stats)

    try:
        with pytest.raises(RedditAccessBlockedError, match="network-security"):
            check_page_responses(
                [
                    NetworkResponse(
                        URL,
                        403,
                        "text/html",
                        BLOCK,
                        "blocked",
                        "Document",
                        "480",
                    ),
                ],
                URL.lower(),
            )

        with pytest.raises(HostCooldownError):
            async with limiter.slot(URL):
                pytest.fail("Default request budget must still be bounded")

        with pytest.raises(HostCooldownError):
            async with request_slot(URL):
                pytest.fail("Blocked Reddit must not occupy the run")
    finally:
        current_limiter.reset(token)
        current_source.reset(source_token)

    assert delays == []
    assert stats.counts["http_429"] == 0


@pytest.mark.parametrize(
    "body",
    [
        "<p>This community is private.</p>",
        '<script>"you\'ve been blocked by network security"</script>Private',
    ],
)
def test_other_403_responses_do_not_become_retryable_network_blocks(body: str):
    with pytest.raises(RedditScrapeError) as error:
        check_page_responses(
            [NetworkResponse(URL, 403, "text/html", body, "id", "Document")],
            URL,
        )

    assert type(error.value) is RedditScrapeError


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize(
    "error_type", [RedditListingUnavailableError],
)
async def test_transient_initial_page_recovers_without_restarting_saved_posts(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[RedditScrapeError],
    *,
    partial: bool,
):
    attempts = 0
    post = RawPost(
        "reddit.com",
        "t3_post",
        URL,
        None,
        dt.datetime.now(dt.UTC),
        "reddit",
        URL,
        "{}",
    )

    async def collect(
        *args: object, **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            if partial:
                yield ScrapeBatch([post], [])

            raise error_type("temporary")

        yield ScrapeBatch([post], [])

    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.reddit._scrape_community", collect)
    monkeypatch.setattr("dank.scrape.reddit.retry_sleep", sleep)
    batches = _retry_community(
        cast(Any, None),
        "LLMDevs",
        RedditSettings(),
        sort=RedditSort.TOP,
        time_filter=RedditTimeFilter.DAY,
    )
    saved: list[RawPost] = []

    if partial:
        with pytest.raises(error_type):
            async for batch in batches:
                saved.extend(batch.posts)
    else:
        async for batch in batches:
            saved.extend(batch.posts)

    assert saved == [post]
    assert attempts == (1 if partial else 2)
    assert sleep.await_count == int(
        not partial and error_type is RedditListingUnavailableError,
    )


async def test_persistent_empty_response_stops_after_one_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    attempts = 0

    async def collect(
        *args: object, **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        nonlocal attempts
        attempts += 1
        yield ScrapeBatch([], [])
        raise RedditListingUnavailableError("no usable listing")

    monkeypatch.setattr("dank.scrape.reddit._scrape_community", collect)
    monkeypatch.setattr("dank.scrape.reddit.retry_sleep", AsyncMock())

    with pytest.raises(RedditListingUnavailableError):
        _ = [
            batch
            async for batch in _retry_community(
                cast(Any, None),
                "LLMDevs",
                RedditSettings(),
                sort=RedditSort.TOP,
                time_filter=RedditTimeFilter.DAY,
            )
        ]

    assert attempts == 2


@pytest.mark.parametrize("status", [403, 429])
async def test_block_defers_remaining_communities_and_later_reddit_sources(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    status: int,
):
    page = MagicMock(close=AsyncMock())
    session = MagicMock(get_browser=AsyncMock(return_value=MagicMock(
        get=AsyncMock(return_value=page),
    )))
    requests = RedditRequests(0)
    limiter = RequestLimiter(ScrapeSettings())
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    visited: list[str] = []
    post = RawPost(
        "reddit.com", "t3_saved", URL, None, dt.datetime.now(dt.UTC),
        "reddit", URL, "{}",
    )

    async def collect(
        _page: object, community: str, *_args: object, **_kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        async with request_slot(URL):
            visited.append(community)

        yield ScrapeBatch([post], [])
        check_page_responses([
            NetworkResponse(URL, status, "text/html", BLOCK, "id", "Document"),
        ], URL)

    async def details(batch: ScrapeBatch, *_args: object, **_kwargs: object):
        yield batch

    monkeypatch.setattr("dank.scrape.reddit._scrape_community", collect)
    monkeypatch.setattr("dank.scrape.reddit._capture_batch_details", details)
    sleep = AsyncMock()
    monkeypatch.setattr("dank.scrape.http.retry_sleep", sleep)
    request_token = current_requests.set(requests)
    limiter_token = current_limiter.set(limiter)
    stats_token = current_source.set(stats)

    try:
        saved = [batch async for batch in scrape_reddit_communities(
            RedditSettings(), ("first", "second", "third"), session,
        )]
        later = [batch async for batch in scrape_reddit_communities(
            RedditSettings(), ("fourth", "fifth"), session,
        )]

        async with limiter.slot("https://other.test/feed"):
            pass
    finally:
        current_requests.reset(request_token)
        current_limiter.reset(limiter_token)
        current_source.reset(stats_token)

    assert [post for batch in saved for post in batch.posts] == [post]
    assert later == []
    assert visited == ["first"]
    assert stats.counts["fetch_failures"] == 2
    assert stats.counts["retries"] == 0
    assert stats.error_type == (
        "RedditAccessBlockedError" if status == 403 else "RedditRateLimitError"
    )
    assert "deferring remaining Reddit communities" in caplog.text
    assert RedditRequests(0).unavailable is None
    sleep.assert_not_awaited()
    assert page.close.await_count == 2


@pytest.mark.parametrize(
    "error_type", [RedditAccessBlockedError, RedditRateLimitError],
)
async def test_blocked_first_page_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, error_type: type[RedditScrapeError],
):
    attempts = 0

    async def collect(*_args: object, **_kwargs: object):
        nonlocal attempts
        attempts += 1
        yield ScrapeBatch([], [])
        raise error_type("blocked")

    monkeypatch.setattr("dank.scrape.reddit._scrape_community", collect)

    with pytest.raises(error_type):
        _ = [batch async for batch in _retry_community(
            cast(Any, None), "first", RedditSettings(),
            sort=RedditSort.TOP, time_filter=RedditTimeFilter.DAY,
        )]

    assert attempts == 1
