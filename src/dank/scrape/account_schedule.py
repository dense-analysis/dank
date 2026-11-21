from __future__ import annotations

import datetime
from typing import NamedTuple

from dank.config import AccountScheduleSettings

MAX_TIMER_SLACK = datetime.timedelta(minutes=1)


class AccountCheck(NamedTuple):
    first_checked_at: datetime.datetime
    last_checked_at: datetime.datetime
    retry_due: bool = False


class PostingActivity(NamedTuple):
    recent_posts: int
    oldest_at: datetime.datetime
    latest_at: datetime.datetime


def recent_check_age(
    settings: AccountScheduleSettings, check: AccountCheck,
    started_at: datetime.datetime,
) -> datetime.timedelta | None:
    if check.retry_due or check.last_checked_at > started_at:
        return None

    # An hourly timer can start a few seconds earlier on the following run.
    elapsed = started_at - check.last_checked_at + min(
        MAX_TIMER_SLACK, settings.min_interval / 60,
    )

    return elapsed if elapsed < settings.max_interval else None


def check_interval(
    settings: AccountScheduleSettings, check: AccountCheck,
    activity: PostingActivity | None, started_at: datetime.datetime,
) -> datetime.timedelta:
    if (activity is None or check.last_checked_at - activity.latest_at
            < settings.min_interval):
        return settings.min_interval

    if not activity.recent_posts:
        return settings.max_interval

    # Use the observed span for new accounts. Aim for one new post per check.
    observed_since = min(check.first_checked_at, activity.oldest_at)
    observed_span = min(settings.lookback, started_at - observed_since)
    interval = observed_span / activity.recent_posts

    return max(settings.min_interval, min(interval, settings.max_interval))
