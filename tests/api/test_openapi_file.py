"""The reader's checked-in schema matches what the app describes."""

from dank.api.openapi import SCHEMA_FILE, schema_text


def test_the_checked_in_schema_is_current() -> None:
    # On failure, run `make openapi` and commit both generated files.
    assert SCHEMA_FILE.read_text(encoding="utf-8") == schema_text()
