import datetime as dt
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
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
)
from dank.scrape.reddit.access import (
    RedditAccessBlockedError,
    RedditListingUnavailableError,
    RedditScrapeError,
    check_listing_dialogs,
    check_page_responses,
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


async def test_security_block_on_canonical_redirect_waits_and_is_not_a_429(
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

        async with request_slot(URL):
            assert now[0] == 480
    finally:
        current_limiter.reset(token)
        current_source.reset(source_token)

    assert delays == [480]
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
    "error_type", [RedditListingUnavailableError, RedditAccessBlockedError],
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
