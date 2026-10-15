import datetime as dt

import pytest

from dank.storage.clickhouse import parse_datetime


@pytest.mark.parametrize("value", [
    dt.datetime(2026, 10, 15, 12, 5, 9, 823000),
    "2026-10-15 12:05:09.823",
    "2026-10-15T12:05:09.823+00:00",
])
def test_clickhouse_utc_timestamps_do_not_shift_to_the_host_timezone(
    value: dt.datetime | str,
) -> None:
    result = parse_datetime(value)
    assert result == dt.datetime(2026, 10, 15, 12, 5, 9, 823000, dt.UTC)
    assert result.timestamp() == 1792065909.823


def test_aware_timestamp_retains_its_instant():
    assert parse_datetime("2026-10-15T13:05:09.823+01:00") == dt.datetime(
        2026, 10, 15, 12, 5, 9, 823000, dt.UTC,
    )
