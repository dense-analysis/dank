from __future__ import annotations

import datetime as dt
import json
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from dank.config import (
    AccountScheduleSettings,
    ConfigError,
    SourceAdapter,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.scrape.account_schedule import (
    AccountCheck,
    PostingActivity,
    check_interval,
)
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.runner import (
    _collect_with_history,  # pyright: ignore[reportPrivateUsage]
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    _load_account_checks,  # pyright: ignore[reportPrivateUsage]
    _plan_account_checks,  # pyright: ignore[reportPrivateUsage]
    _save_account_checks,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult

START = dt.datetime(2026, 11, 21, 12, tzinfo=dt.UTC)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)


@pytest.mark.parametrize(("activity", "interval"), [
    (None, HOUR),
    (PostingActivity(200, START - 9 * DAY, START - 3 * HOUR), HOUR),
    (PostingActivity(28, START - 9 * DAY, START - 3 * HOUR), 6 * HOUR),
    (PostingActivity(1, START - 9 * DAY, START - 3 * HOUR), DAY),
    (PostingActivity(0, START - 9 * DAY, START - 8 * DAY), DAY),
    (PostingActivity(1, START - 9 * DAY, START - HOUR), HOUR),
])
def test_estimate_is_bounded_and_responds_to_recent_activity(
    activity: PostingActivity | None, interval: dt.timedelta,
) -> None:
    previous = AccountCheck(START - 3 * DAY, START - HOUR)
    assert check_interval(
        AccountScheduleSettings(), previous, activity, START,
    ) == interval


def test_recently_added_account_uses_its_shorter_observation_span() -> None:
    previous = AccountCheck(START - 3 * HOUR, START - 2 * HOUR)
    activity = PostingActivity(1, START - 4 * HOUR, START - 4 * HOUR)
    assert check_interval(
        AccountScheduleSettings(), previous, activity, START,
    ) == 4 * HOUR


@pytest.mark.parametrize(("config", "key"), [
    ("[scrape]\naccount_schedule = false", "schedule"),
    ('[scrape.account_schedule]\nenabled = "false"', "enabled"),
    ('[scrape.account_schedule]\nmin_interval = "25h"', "min_interval"),
    ("[scrape.account_schedule]\nmax_interval = 24", "max_interval"),
    ('[scrape.account_schedule]\nlookback = "0d"', "lookback"),
])
def test_invalid_schedule_config(
    memory_path: Path, config: str, key: str,
) -> None:
    path = memory_path / "test-config.toml"
    path.write_text(config)

    with pytest.raises(ConfigError, match=key):
        load_settings(path)


def test_schedule_defaults_and_overrides(memory_path: Path) -> None:
    path = memory_path / "test-config.toml"
    path.write_text("")
    settings = load_settings(path)
    assert settings.account_schedule == AccountScheduleSettings(
        enabled=True, min_interval=HOUR, max_interval=DAY, lookback=7 * DAY,
    )
    assert settings.x.schedule is None
    path.write_text(
        '[scrape.account_schedule]\nenabled = false\nmin_interval = "30m"\n'
        'max_interval = "12h"\nlookback = "14d"\n'
        '[x.schedule]\nenabled = true\nmax_interval = "2h"\n',
    )
    settings = load_settings(path)
    assert settings.account_schedule == AccountScheduleSettings(
        enabled=False, min_interval=HOUR / 2, max_interval=12 * HOUR,
        lookback=14 * DAY,
    )
    assert settings.x.schedule == AccountScheduleSettings(
        enabled=True, min_interval=HOUR / 2, max_interval=2 * HOUR,
        lookback=14 * DAY,
    )


@pytest.mark.parametrize(("max_interval", "due"), [
    ("6h", True), ("12h", False),
])
async def test_quiet_accounts_use_configured_max_interval(
    memory_path: Path, max_interval: str, *, due: bool,
) -> None:
    path = memory_path / "test-config.toml"
    path.write_text(
        f'[scrape.account_schedule]\nmax_interval = "{max_interval}"\n',
    )
    channel = "https://www.youtube.com/@quiet/videos"
    settings = load_settings(path)._replace(data_dir=memory_path, sources=(
        SourceConfig("x.com", ("quiet",)),
        SourceConfig("youtube.com", ("@quiet",)),
    ))
    previous = AccountCheck(START - 12 * DAY, START - 8 * HOUR)
    _save_account_checks(
        memory_path / "x-account-checks.json", {"quiet": previous},
    )
    _save_account_checks(
        memory_path / "youtube-account-checks.json", {channel: previous},
    )
    client = MagicMock(fetch_json=AsyncMock(return_value=QueryResult([
        {"adapter": kind, "account": account, "recent_posts": 0,
         "oldest_at": START - 12 * DAY, "latest_at": START - 8 * DAY}
        for kind, account in (("x", "quiet"), ("youtube", channel))
    ])))

    plans = await _plan_account_checks(settings, client, START)

    assert plans[SourceAdapter.X].due == ({"quiet"} if due else set())
    assert plans[SourceAdapter.YOUTUBE].due == ({channel} if due else set())
    assert client.fetch_json.await_count == int(not due)


@pytest.mark.parametrize("bypass", ["", "scoped", "disabled", "backfill"])
async def test_select_accounts_for_regular_and_forced_runs(
    memory_path: Path, monkeypatch: pytest.MonkeyPatch, bypass: str,
) -> None:
    path = memory_path / "test-config.toml"
    path.write_text("[x]\nhomepage = false\n")
    settings = load_settings(path)._replace(data_dir=memory_path)

    if bypass == "disabled":
        settings = settings._replace(x=settings.x._replace(
            schedule=AccountScheduleSettings(enabled=False),
        ))
    elif bypass == "backfill":
        settings = settings._replace(x=settings.x._replace(known_overlap=0))

    state = memory_path / "x-account-checks.json"
    # The hourly timer can start a few seconds earlier on the following run.
    timer_offset = dt.timedelta(seconds=20)
    recent = AccountCheck(START - 10 * DAY, START - HOUR + timer_offset)
    original = {
        "active": recent,
        "quiet": recent,
        "just_checked": recent._replace(last_checked_at=START - HOUR / 2),
        "overdue": recent._replace(last_checked_at=START - DAY + timer_offset),
        "retry": recent._replace(retry_due=True),
    }
    _save_account_checks(state, original)
    source = SourceConfig("x.com", (
        "@Active", "quiet", "just_checked", "overdue", "retry", "new_account",
    ))
    attempted: list[str] = []

    async def scrape(
        _settings: XSettings, accounts: tuple[str, ...], *args: object,
        on_checked: Callable[[str], None] | None, **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        attempted.extend(accounts)

        if on_checked is not None:
            # Overlap on active accounts can succeed with no new posts.
            on_checked("active")
            on_checked("new_account")

        for batch in ():
            yield batch

    monkeypatch.setattr("dank.scrape.runner.scrape_x_accounts", scrape)
    activity_result = QueryResult([
        {"adapter": "x", "account": "active", "recent_posts": 200,
         "oldest_at": START - 10 * DAY, "latest_at": START - 3 * HOUR},
        {"adapter": "x", "account": "quiet", "recent_posts": 0,
         "oldest_at": START - 10 * DAY, "latest_at": START - 8 * DAY},
    ])
    no_overlap = QueryResult([])
    results = [no_overlap] if bypass else [activity_result, no_overlap]
    client = MagicMock(fetch_json=AsyncMock(side_effect=results))
    plans = {} if bypass == "scoped" else await _plan_account_checks(
        settings._replace(sources=(source,)), client, START,
    )

    assert [batch async for batch in _discover_source_batches(
        settings, source, client, MagicMock(), MagicMock(),
        feed_staleness=DAY, batch_size=50, scrape_started_at=START,
        account_plans=plans,
    )] == []

    saved = _load_account_checks(state)

    if bypass:
        assert attempted == list(source.accounts)
        assert saved == original
        assert client.fetch_json.await_count == int(bypass != "backfill")

        return

    assert attempted == ["@Active", "overdue", "retry", "new_account"]
    activity, overlap = client.fetch_json.await_args_list
    activity_sql, activity_params = activity.args
    assert "FROM raw_posts" in activity_sql and "FROM posts" in activity_sql
    assert "NOT is_reference" in activity_sql
    assert "GROUP BY adapter, account, item_id" in activity_sql
    assert "min(published_at)" in activity_sql
    assert activity_params == {
        "accounts": [("x", "active", START - 7 * DAY),
                     ("x", "quiet", START - 7 * DAY)],
        "cutoff": START,
    }
    assert overlap.args[1]["accounts"] == [
        "active", "new_account", "overdue", "retry",
    ]
    assert saved == {
        **original, "active": recent._replace(last_checked_at=START),
        "overdue": original["overdue"]._replace(retry_due=True),
        "new_account": AccountCheck(START, START),
    }


async def test_all_deferred_accounts_delay_browser_start(
    memory_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    config = memory_path / "test-config.toml"
    config.write_text("[x]\nhomepage = false\n")
    settings = load_settings(config)._replace(data_dir=memory_path)
    source = SourceConfig("x.com", ("quiet",))
    seen_at = START - HOUR / 2
    _save_account_checks(
        memory_path / "x-account-checks.json",
        {"quiet": AccountCheck(seen_at, seen_at)},
    )
    session = MagicMock(get_browser=AsyncMock())
    client = MagicMock(fetch_json=AsyncMock())
    caplog.set_level("INFO", logger="dank.scrape.runner")
    plans = await _plan_account_checks(
        settings._replace(sources=(source,)), client, START,
    )
    stats = SourceMetrics(1, source)
    token = current_source.set(stats)

    try:
        assert [batch async for batch in _discover_source_batches(
            settings, source, client, MagicMock(), session,
            feed_staleness=DAY, batch_size=50, scrape_started_at=START,
            account_plans=plans,
        )] == []
    finally:
        current_source.reset(token)

    assert stats.skipped
    assert "due=0 deferred=1" in caplog.text
    session.get_browser.assert_not_awaited()
    client.fetch_json.assert_not_awaited()


@pytest.mark.parametrize("mode", ["regular", "scoped", "unavailable"])
async def test_run_shares_one_activity_query_across_account_sources(
    memory_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    path = memory_path / "test-config.toml"
    path.write_text('[x.schedule]\nlookback = "14d"\n')
    channel = "https://www.youtube.com/@writer/videos"
    sources = (
        SourceConfig("x.com", ("writer",)),
        SourceConfig("youtube.com", ("@writer",)),
        SourceConfig("x.com", ("@WRITER",)),
    )
    settings = load_settings(path)._replace(
        data_dir=memory_path, sources=sources,
    )
    previous = AccountCheck(START - 15 * DAY, START - HOUR)
    _save_account_checks(
        memory_path / "x-account-checks.json", {"writer": previous},
    )
    _save_account_checks(
        memory_path / "youtube-account-checks.json", {channel: previous},
    )
    result = QueryResult([
        {"adapter": "x", "account": "writer", "recent_posts": 0,
         "oldest_at": START - 19 * DAY, "latest_at": START - 18 * DAY},
        {"adapter": "youtube", "account": channel, "recent_posts": 200,
         "oldest_at": START - 8 * DAY, "latest_at": START - 3 * HOUR},
    ])
    client = MagicMock(fetch_json=AsyncMock(return_value=result))

    if mode == "unavailable":
        client.fetch_json.side_effect = OperationalError("fixture timeout")

    history = MagicMock(
        started_at=START, report_progress=AsyncMock(), source_concurrency=2,
        linked_video_ids=set(),
    )
    collect_source = AsyncMock()
    monkeypatch.setattr("dank.scrape.runner._collect_source", collect_source)
    monkeypatch.setattr(
        "dank.scrape.runner.collect_linked_youtube_metadata", AsyncMock(),
    )
    monkeypatch.setattr(
        "dank.scrape.runner.collect_run_youtube_transcripts", AsyncMock(),
    )

    await _collect_with_history(
        settings, client, MagicMock(), MagicMock(), MagicMock(), MagicMock(),
        history, memory_path, youtube_profile=None, scoped=mode == "scoped",
    )

    plans = [
        call.kwargs["account_plans"] for call in collect_source.await_args_list
    ]
    assert len(plans) == len(sources)
    assert all(plan is plans[0] for plan in plans)

    if mode == "scoped":
        client.fetch_json.assert_not_awaited()
        assert plans[0] == {}

        return

    client.fetch_json.assert_awaited_once()
    sql, params = client.fetch_json.await_args.args
    assert "FROM raw_posts" in sql and "FROM youtube_discoveries" in sql
    assert params == {"accounts": [
        ("x", "writer", START - 14 * DAY),
        ("youtube", channel, START - 7 * DAY),
    ], "cutoff": START}
    assert plans[0][SourceAdapter.X].due == (
        {"writer"} if mode == "unavailable" else set()
    )
    assert plans[0][SourceAdapter.YOUTUBE].due == {channel}


def test_bad_local_check_file_does_not_defer_accounts(
    memory_path: Path,
) -> None:
    path = memory_path / "x-account-checks.json"
    path.write_text("{")
    assert _load_account_checks(path) == {}
    invalid = {"version": 1, "accounts": {"quiet": "invalid"}}
    path.write_text(json.dumps(invalid))
    assert _load_account_checks(path) == {}
