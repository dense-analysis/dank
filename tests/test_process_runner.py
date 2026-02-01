import datetime

import pytest

from dank.process.runner import parse_age_window


def test_parse_age_window_seconds() -> None:
    assert parse_age_window("30s") == datetime.timedelta(seconds=30)
    assert parse_age_window("15") == datetime.timedelta(seconds=15)


def test_parse_age_window_minutes() -> None:
    assert parse_age_window("10m") == datetime.timedelta(minutes=10)
    assert parse_age_window("5min") == datetime.timedelta(minutes=5)


def test_parse_age_window_hours() -> None:
    assert parse_age_window("2h") == datetime.timedelta(hours=2)
    assert parse_age_window("1hour") == datetime.timedelta(hours=1)


@pytest.mark.parametrize("value", ["", "0", "-5m", "3d", "abc"])
def test_parse_age_window_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_age_window(value)
