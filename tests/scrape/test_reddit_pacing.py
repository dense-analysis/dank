import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dank.config import RedditSettings
from dank.scrape.reddit import access
from dank.scrape.reddit.access import RedditRequests, current_requests
from dank.scrape.reddit.details import (
    _fetch_thread,  # pyright: ignore[reportPrivateUsage]
)


async def test_shared_request_clock_spaces_concurrent_actions_and_no_catchup(
    monkeypatch: pytest.MonkeyPatch,
):
    now = 100.0
    starts: list[float] = []

    async def sleep(delay: float):
        nonlocal now
        now += delay

    monkeypatch.setattr(access, 'time', SimpleNamespace(monotonic=lambda: now))
    monkeypatch.setattr(access, 'asyncio', SimpleNamespace(
        sleep=sleep, Lock=asyncio.Lock,
    ))
    requests = RedditRequests(10)

    async def action():
        await requests.wait()
        starts.append(now)

    await asyncio.gather(action(), action(), action())
    now += 50
    await action()
    await action()
    assert starts == [100, 110, 120, 170, 180]


async def test_denied_json_is_not_retried_for_each_post_in_same_run():
    page = AsyncMock()
    page.evaluate.return_value = json.dumps({'status': 403})
    requests = RedditRequests(0)
    token = current_requests.set(requests)

    try:
        for identifier in ('abc', 'def'):
            assert await _fetch_thread(page, {
                'id': f't3_{identifier}', 'subreddit': 'python',
                'permalink': f'https://www.reddit.com/r/python/comments/{identifier}/',
            }, RedditSettings()) is None
    finally:
        current_requests.reset(token)

    page.evaluate.assert_awaited_once()
    assert requests.requests == 1
    assert RedditRequests(0).json_available is True
