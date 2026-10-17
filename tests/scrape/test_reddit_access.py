from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from zendriver import cdp

from dank.config import ScrapeSettings, SourceConfig
from dank.scrape.http import HostCooldownError, RequestLimiter, current_limiter
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.reddit.access import RedditScrapeError, check_page_responses
from dank.scrape.zendriver import NetworkCapture, NetworkResponse

URL = "https://www.reddit.com/r/python/top/?t=day"


@pytest.mark.parametrize("body_failed", [False, True])
async def test_browser_rate_limit_keeps_retry_after_even_without_body(
    *, body_failed: bool,
) -> None:
    page = MagicMock()
    page.send = AsyncMock(side_effect=[
        None, RuntimeError("body unavailable") if body_failed else ("", False),
    ])
    capture = NetworkCapture(
        page, ("https://www.reddit.com/",),
        (cdp.network.ResourceType.DOCUMENT,),
    )
    await capture.start()
    handlers = {
        call.args[0]: call.args[1] for call in page.add_handler.call_args_list
    }
    await handlers[cdp.network.ResponseReceived](SimpleNamespace(
        type_=cdp.network.ResourceType.DOCUMENT, request_id="limited",
        response=SimpleNamespace(
            url=URL, status=429, mime_type="text/html",
            headers={"rEtRy-AfTeR": "120", "Set-Cookie": "fixture-secret"},
        ),
    ))
    await handlers[cdp.network.LoadingFinished](SimpleNamespace(
        request_id="limited",
    ))
    responses = await capture.drain(0.001)
    await capture.stop()
    assert len(responses) == 1
    assert responses[0].status == 429
    assert responses[0].retry_after == "120"
    assert "fixture-secret" not in repr(responses)


@pytest.mark.parametrize("url,resource", [
    (URL, "Document"),
    ("https://www.reddit.com/svc/shreddit/more-comments/python/t3_post",
     "Fetch"),
    ("https://www.reddit.com/svc/shreddit/community-more-posts/top/?after=id",
     "Fetch"),
])
async def test_listing_rate_limit_defers_same_host_without_retrying_early(
    url: str, resource: str,
) -> None:
    limiter = RequestLimiter(ScrapeSettings())
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    limit_token = current_limiter.set(limiter)
    stats_token = current_source.set(stats)

    try:
        with pytest.raises(RedditScrapeError, match="HTTP 429"):
            check_page_responses([
                NetworkResponse(url, 429, "text/html", "", "id", resource,
                                "120"),
            ], URL)

        with pytest.raises(HostCooldownError):
            async with limiter.slot("https://www.reddit.com/r/next/top/"):
                pytest.fail("Reddit was requested before Retry-After")

        async with limiter.slot("https://example.test/feed"):
            pass
    finally:
        current_limiter.reset(limit_token)
        current_source.reset(stats_token)

    assert stats.counts["http_429"] == 1


def test_telemetry_and_other_hosts_do_not_fail_the_listing() -> None:
    check_page_responses([
        NetworkResponse("https://www.reddit.com/svc/shreddit/events", 429,
                        "text/plain", "", "telemetry", "Fetch"),
        NetworkResponse("https://other.test/r/python/top/", 403,
                        "text/html", "", "other", "Document"),
    ], URL)


def test_access_denial_is_not_reported_as_an_empty_listing() -> None:
    with pytest.raises(RedditScrapeError, match="HTTP 403"):
        check_page_responses([
            NetworkResponse(URL, 403, "text/html", "", "denied", "Document"),
        ], URL)


async def test_reddit_cooldown_wait_releases_other_http_slots(
    monkeypatch: pytest.MonkeyPatch,
):
    from dank.scrape.reddit.access import request_slot

    now = [0.0]
    limiter = RequestLimiter(ScrapeSettings(http_concurrency=1))
    monkeypatch.setattr(
        'dank.scrape.http.time', SimpleNamespace(monotonic=lambda: now[0]),
    )
    delays: list[float] = []

    async def wait(delay: float):
        delays.append(delay)

        async with limiter.slot('https://other.test/feed'):
            pass

        now[0] += delay

    monkeypatch.setattr('dank.scrape.http.retry_sleep', wait)
    limiter.cool_down(URL, 300)
    token = current_limiter.set(limiter)

    try:
        async with request_slot(URL):
            assert now[0] == 300
    finally:
        current_limiter.reset(token)

    assert delays == [300]
