from __future__ import annotations

import asyncio
import pathlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from zendriver import cdp

from dank.config import BrowserScrapeSettings, SourceConfig, XSettings
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.x import (
    X_GRAPHQL_PATTERNS,
    extract_posts_and_assets,
    scrape_x_accounts,
)
from dank.scrape.zendriver import NetworkCapture, NetworkResponse

TIMELINE_URL = (
    "https://x.com/i/api/graphql/query-id/UserOriginalsTimeline?variables={}"
)


@pytest.fixture
def timeline_body() -> str:
    path = (
        pathlib.Path(__file__).parents[1]
        / "fixtures/UserTweetsWactherGuruExample.json"
    )

    return path.read_text()


@pytest.mark.parametrize(("url", "expected"), [
    (TIMELINE_URL, True),
    ("https://x.com/i/api/graphql/id/UserTweets?variables={}", True),
    ("https://x.com/i/api/graphql/id/UserTweetsAndReplies", True),
    ("https://x.com/i/api/graphql/id/TweetDetail", True),
    ("https://x.com/i/api/graphql/id/UserMedia", True),
    ("https://x.com/i/api/graphql/id/UserTweetsCount", False),
    ("https://x.com/i/api/graphql/id/HomeTimeline", False),
    ("https://other.test/i/api/graphql/id/UserOriginalsTimeline", False),
])
async def test_account_network_responses_reach_post_extraction(
    timeline_body: str, url: str, *, expected: bool,
) -> None:
    page = MagicMock()
    page.send = AsyncMock(side_effect=[None, (timeline_body, False)])
    capture = NetworkCapture(page, X_GRAPHQL_PATTERNS)
    await capture.start()
    handlers = {
        call.args[0]: call.args[1] for call in page.add_handler.call_args_list
    }
    await handlers[cdp.network.ResponseReceived](SimpleNamespace(
        type_=cdp.network.ResourceType.FETCH,
        request_id="fixture-request",
        response=SimpleNamespace(
            url=url, status=200, mime_type="application/json", headers={},
        ),
    ))
    await handlers[cdp.network.LoadingFinished](SimpleNamespace(
        request_id="fixture-request",
    ))
    responses = await capture.drain(0.001)
    await capture.stop()
    posts, _ = extract_posts_and_assets(responses, set(), set())

    assert bool(posts) == expected
    assert page.send.await_count == (2 if expected else 1)

    if expected:
        assert posts[0].post_id == "2019987790369296512"
        assert posts[0].source == "x"
        assert posts[0].request_url == url


@pytest.mark.parametrize("stage", [
    "startup", "home", "capture", "account", "inspection", "scroll", "drain",
])
async def test_stalled_browser_retries_each_account_and_preserves_batches(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    timeline_body: str, stage: str,
) -> None:
    caplog.set_level("INFO", logger="dank.scrape.x")
    page = MagicMock()
    page.get = AsyncMock(return_value=page)
    page.wait_for_ready_state = AsyncMock()
    page.evaluate = AsyncMock(return_value="https://x.com/one")
    page.select = AsyncMock(side_effect=TimeoutError())
    browser = MagicMock(get=AsyncMock(return_value=page))
    session = MagicMock(
        get_browser=AsyncMock(return_value=browser), close=AsyncMock(),
    )
    response = NetworkResponse(
        TIMELINE_URL, 200, "application/json", timeline_body, "id", "Fetch",
    )
    capture = MagicMock(
        start=AsyncMock(), stop=AsyncMock(),
        drain=AsyncMock(return_value=[response] if stage == "scroll" else []),
    )
    monkeypatch.setattr(
        "dank.scrape.x.NetworkCapture", MagicMock(return_value=capture),
    )
    monkeypatch.setattr("dank.scrape.x.wait_for_x_session", AsyncMock())
    monkeypatch.setattr("dank.scrape.x.BROWSER_ACTION_TIMEOUT_SECONDS", 0.01)
    inspect_page = AsyncMock()
    scroll = AsyncMock()
    monkeypatch.setattr("dank.scrape.x._check_login_required", inspect_page)
    monkeypatch.setattr("dank.scrape.x._scroll", scroll)

    async def stalled(*args: Any, **kwargs: Any) -> None:
        await asyncio.Event().wait()

    commands = {
        "startup": session.get_browser, "home": browser.get,
        "capture": capture.start, "account": page.get,
        "inspection": inspect_page, "scroll": scroll, "drain": capture.drain,
    }
    commands[stage].side_effect = stalled
    stats = SourceMetrics(1, SourceConfig("x.com", ("one", "two")))
    token = current_source.set(stats)

    try:
        async with asyncio.timeout(1):
            batches = [batch async for batch in scrape_x_accounts(
                XSettings(BrowserScrapeSettings(max_posts=1000)),
                stats.source.accounts, session,
            )]
    finally:
        current_source.reset(token)

    assert bool(batches) == (stage == "scroll")
    assert stats.counts["fetch_failures"] == 2
    assert stats.counts["retries"] == 2
    assert stats.error_type == "TimeoutError"
    assert "browser failure=TimeoutError" in caplog.text
    assert "X timeline [2/2] account=two" in caplog.text
    assert session.close.await_count == 4
    assert page.get.await_count <= 4

    if stage not in {"startup", "home"}:
        assert capture.stop.await_count == 4
