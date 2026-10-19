import datetime as dt
import json
from collections.abc import AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import (
    BrowserScrapeSettings,
    RedditSettings,
    RedditSort,
    RedditTimeFilter,
    SourceConfig,
)
from dank.model import RawPost
from dank.scrape.__main__ import main
from dank.scrape.reddit import (
    _retry_community,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.reddit.access import RedditRateLimitError
from dank.scrape.runner import (
    _load_pending_reddit_posts,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch


def raw() -> RawPost:
    return RawPost(
        "reddit.com",
        "t3_post",
        "https://www.reddit.com/r/python/comments/post/",
        None,
        dt.datetime.now(dt.UTC),
        "reddit",
        "",
        json.dumps({"id": "t3_post"}),
    )


@pytest.mark.parametrize("partial", [False, True])
async def test_rate_limits_defer_without_duplicating_saved_posts(
    monkeypatch: pytest.MonkeyPatch,
    *,
    partial: bool,
):
    calls = 0

    async def collect(
        *args: object, **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        nonlocal calls
        calls += 1

        if calls == 1:
            if partial:
                yield ScrapeBatch([raw()], [])

            raise RedditRateLimitError("limited")

        yield ScrapeBatch([raw()], [])

    monkeypatch.setattr("dank.scrape.reddit._scrape_community", collect)
    batches = _retry_community(
        AsyncMock(),
        "python",
        RedditSettings(),
        sort=RedditSort.TOP,
        time_filter=RedditTimeFilter.DAY,
    )
    saved: list[RawPost] = []

    with pytest.raises(RedditRateLimitError):
        async for batch in batches:
            saved.extend(batch.posts)

    assert len(saved) == int(partial)
    assert calls == 1


async def test_refresh_query_is_recent_bounded_and_uses_latest_post_state():
    client = AsyncMock()
    client.fetch_json.return_value = SimpleNamespace(rows=[raw()._asdict()])
    settings = RedditSettings(scrape=BrowserScrapeSettings(max_posts=5))
    posts = await _load_pending_reddit_posts(
        client,
        SourceConfig(
            "reddit.com",
            (),
            ("https://www.reddit.com/r/python/",),
        ),
        settings,
    )
    assert [post.post_id for post in posts] == ["t3_post"]
    sql, parameters = client.fetch_json.await_args.args
    assert sql.index("LIMIT 1 BY post_id") < sql.index(
        "WHERE JSONExtractString",
    )
    assert parameters["communities"] == ["python"]
    assert parameters["limit"] == 5
    assert parameters["cutoff"].tzinfo == dt.UTC
    assert (
        dt.timedelta(hours=23)
        < dt.datetime.now(dt.UTC) - parameters["cutoff"]
        < dt.timedelta(hours=25)
    )


def test_refresh_command_is_explicit_and_forwards_selection(
    monkeypatch: pytest.MonkeyPatch,
):
    run = MagicMock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr(
        "sys.argv", ["scrape", "--reddit-details", "--headless"],
    )
    main()
    assert run.call_args.kwargs["reddit_details"] is True
    assert run.call_args.kwargs["headless"] is True


@pytest.mark.parametrize(
    "extra",
    [
        ["--x-post", "https://x.com/person/status/123"],
        ["--retry-failed-pages"],
    ],
)
def test_refresh_rejects_conflicting_retry_modes(
    monkeypatch: pytest.MonkeyPatch, extra: list[str],
):
    run = MagicMock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr("sys.argv", ["scrape", "--reddit-details", *extra])

    with pytest.raises(SystemExit):
        main()

    run.assert_not_called()
