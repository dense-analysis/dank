from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import (
    BrowserScrapeSettings,
    ConfigError,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.runner import (
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.x.home import HOME_PATTERNS, HomeCapture, scrape_x_for_you
from dank.scrape.x.session import LoginRequiredError
from dank.scrape.zendriver import BrowserProfileBusyError, NetworkResponse


def tweet(identifier: str, **extra: Any) -> dict[str, Any]:
    return {
        "__typename": "Tweet",
        "rest_id": identifier,
        "core": {
            "user_results": {
                "result": {
                    "legacy": {
                        "screen_name": "example",
                        "name": "Example",
                    },
                },
            },
        },
        "legacy": {"full_text": f"Post {identifier}", "id_str": identifier},
        **extra,
    }


def item(post: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "itemType": "TimelineTweet",
        "tweet_results": {"result": post},
        **extra,
    }


def response(*items: dict[str, Any], status: int = 200) -> NetworkResponse:
    return NetworkResponse(
        "https://x.com/i/api/graphql/id/HomeTimeline?variables={}",
        status,
        "application/json",
        json.dumps(
            {
                "data": {
                    "home": {
                        "home_timeline_urt": {
                            "instructions": [
                                {
                                    "type": "TimelineAddEntries",
                                    "entries": [
                                        {
                                            "content": {
                                                "entryType": (
                                                "TimelineTimelineItem"
                                            ),
                                                "itemContent": entry,
                                            },
                                        }
                                        for entry in items
                                    ],
                                },
                            ],
                        },
                    },
                },
            },
        ),
        "request",
        "Fetch",
    )


def recommended(batch: Any) -> list[str]:
    return [
        p.post_id
        for p in batch.posts
        if "dank_x_home" in json.loads(p.payload)
    ]


def test_excludes_ads_and_nested_quotes_but_saves_quote_content():
    capture = HomeCapture(10)
    batch = capture.extract(
        response(
            item(tweet("10", quoted_status_result={"result": tweet("11")})),
            item(tweet("12"), promotedMetadata={"advertiser": "ad"}),
            item(tweet("13")),
        ),
    )
    assert recommended(batch) == ["10", "13"]
    assert {p.post_id for p in batch.posts} == {"10", "11", "13"}
    assert [
        json.loads(p.payload)["dank_x_home"]["position"]
        for p in batch.posts
        if p.post_id in {"10", "13"}
    ] == [1, 2]
    first = json.loads(batch.posts[0].payload)["dank_x_home"]
    assert first["item"]["tweet_results"]["result"]["rest_id"] == "10"
    assert {p.scraped_at for p in batch.posts} == {capture.observed_at}


def test_seen_quote_can_later_be_a_recommendation_and_limit_is_exact():
    capture = HomeCapture(2)
    capture.extract(
        response(
            item(tweet("10", quoted_status_result={"result": tweet("11")})),
        ),
    )
    batch = capture.extract(
        response(item(tweet("10")), item(tweet("11")), item(tweet("12"))),
    )
    assert recommended(batch) == ["11"]
    assert capture.recommended == {"10", "11"}


def test_reference_does_not_suppress_later_content():
    capture = HomeCapture(2)
    assert not capture.extract(
        response(item({"__typename": "Tweet", "rest_id": "10"})),
    ).posts
    assert recommended(capture.extract(response(item(tweet("10"))))) == ["10"]


def test_modules_preserve_order_and_exclude_unrelated_results():
    payload = json.loads(response().body)
    payload["sidebar"] = {"tweet_results": {"result": tweet("99")}}
    instructions = payload["data"]["home"]["home_timeline_urt"]["instructions"]
    instructions[0]["entries"] = [
        {
            "content": {
                "entryType": "TimelineTimelineModule",
                "items": [
                    {"item": {"itemContent": item(tweet("10"))}},
                    {"item": {"itemContent": item(tweet("11"))}},
                ],
            },
        },
    ]
    instructions.append(
        {
            "type": "TimelineAddToModule",
            "moduleItems": [
                {"item": {"itemContent": item(tweet("12"))}},
            ],
        },
    )
    batch = HomeCapture(10).extract(
        response()._replace(body=json.dumps(payload)),
    )
    assert recommended(batch) == ["10", "11", "12"]


def test_only_for_you_network_operation_is_accepted():
    pattern = re.compile(HOME_PATTERNS[0])
    assert pattern.match(response().url)
    assert not pattern.match(
        response().url.replace("HomeTimeline", "HomeLatestTimeline"),
    )
    assert not pattern.match(
        response().url.replace("HomeTimeline", "TweetDetail"),
    )


@pytest.mark.parametrize(
    "invalid",
    [
        response(status=429),
        response()._replace(body='{"errors":[{}]}'),
        response()._replace(body='{"data":{"user":{}}}'),
    ],
)
def test_failed_response_is_not_an_empty_success(invalid: NetworkResponse):
    with pytest.raises((RuntimeError, ValueError)):
        HomeCapture(10).extract(invalid)


@pytest.mark.parametrize("failure", [True, False])
async def test_capture_is_attached_before_navigation_and_partial_posts_survive(
    monkeypatch: pytest.MonkeyPatch,
    *,
    failure: bool,
):
    events: list[str] = []

    async def navigate(url: str):
        events.append(url)

    async def start():
        events.append("capture")

    page = MagicMock(get=AsyncMock(side_effect=navigate))
    browser = MagicMock(get=AsyncMock(return_value=page))
    session = MagicMock(get_browser=AsyncMock(return_value=browser))
    capture = MagicMock(
        start=AsyncMock(side_effect=start),
        stop=AsyncMock(),
        drain=AsyncMock(
            side_effect=[
                [response(item(tweet("10")))],
                [response(status=429)] if failure else [],
            ],
        ),
    )
    monkeypatch.setattr(
        "dank.scrape.x.home.NetworkCapture", MagicMock(return_value=capture),
    )
    monkeypatch.setattr("dank.scrape.x.home.wait_for_x_session", AsyncMock())
    monkeypatch.setattr("dank.scrape.x.home.select_for_you", AsyncMock())
    monkeypatch.setattr("dank.scrape.x.home.check_login_required", AsyncMock())
    monkeypatch.setattr("dank.scrape.x.home.scroll", AsyncMock())
    stats = SourceMetrics(1, SourceConfig("x.com", ()))
    token = current_source.set(stats)

    try:
        batches = [
            batch
            async for batch in scrape_x_for_you(
                XSettings(scrape=BrowserScrapeSettings(max_scrolls=1)),
                session,
            )
        ]
    finally:
        current_source.reset(token)

    assert events[:2] == ["capture", "https://x.com/home"]
    assert recommended(batches[0]) == ["10"]
    assert stats.counts["fetch_failures"] == int(failure)
    capture.stop.assert_awaited_once()


@pytest.mark.parametrize("error", [
    LoginRequiredError("Sign in required"),
    BrowserProfileBusyError("Profile already in use"),
])
async def test_session_failure_stops_x_without_attempting_accounts(
    monkeypatch: pytest.MonkeyPatch, error: Exception,
):
    settings = load_settings("config.example.toml")
    source = SourceConfig("x.com", ("example",))
    accounts = MagicMock()
    monkeypatch.setattr("dank.scrape.runner.scrape_x_accounts", accounts)
    session = MagicMock(get_browser=AsyncMock(side_effect=error))
    stats = SourceMetrics(1, source)
    token = current_source.set(stats)

    try:
        batches = [batch async for batch in _discover_source_batches(
            settings, source, MagicMock(), MagicMock(), session,
            feed_staleness=dt.timedelta(days=1), batch_size=50,
        )]
    finally:
        current_source.reset(token)

    assert batches == []
    assert stats.counts["fetch_failures"] == 1
    accounts.assert_not_called()


@pytest.mark.parametrize(("value", "expected"), [
    ("", True), ("[x]\nhomepage = false", False),
])
def test_homepage_only_source_and_opt_out(
    tmp_path: pathlib.Path, value: str, *, expected: bool,
):
    path = tmp_path / "test-settings.toml"
    path.write_text('sources = ["x.com"]\n' + value)
    settings = load_settings(path)
    assert settings.x.homepage is expected
    assert settings.sources[0].accounts == ()


def test_homepage_setting_requires_boolean(tmp_path: pathlib.Path):
    path = tmp_path / "test-settings.toml"
    path.write_text('[x]\nhomepage = "false"')

    with pytest.raises(ConfigError, match=r"x\.homepage"):
        load_settings(path)
