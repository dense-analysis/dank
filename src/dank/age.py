from __future__ import annotations

import datetime
import re


def parse_age_window(value: str) -> datetime.timedelta:
    match = re.fullmatch(r"(\d+)\s*([a-z]*)", value.strip().lower())

    if not match:
        raise ValueError("Age must look like 30s, 10m, or 2h")

    amount = int(match.group(1))

    if amount <= 0:
        raise ValueError("Age must be greater than zero")

    # Match the unit.
    match match.group(2):
        case "" | "s" | "sec" | "secs" | "second" | "seconds":
            return datetime.timedelta(seconds=amount)
        case "m" | "min" | "mins" | "minute" | "minutes":
            return datetime.timedelta(minutes=amount)
        case "h" | "hr" | "hrs" | "hour" | "hours":
            return datetime.timedelta(hours=amount)
        case _:
            raise ValueError("Age must be in seconds, minutes, or hours")
