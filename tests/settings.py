"""Read immutable settings from the public native example once per worker."""

from functools import cache
from pathlib import Path

from dank.config import Settings, load_settings

_NATIVE_EXAMPLE = Path(__file__).parents[1] / "config.native.example.toml"


@cache
def native_settings() -> Settings:
    return load_settings(_NATIVE_EXAMPLE)
