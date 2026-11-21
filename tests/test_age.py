import datetime as dt

import pytest

from dank.age import parse_age_window


@pytest.mark.parametrize("value", ["30d", "30 day", "30days", " 30D "])
def test_retention_age_accepts_days(value: str) -> None:
    assert parse_age_window(value) == dt.timedelta(days=30)


@pytest.mark.parametrize("value", ["all", "0d", "-1d", "1.5d"])
def test_retention_age_requires_a_positive_finite_window(value: str) -> None:
    with pytest.raises(ValueError):
        parse_age_window(value)
