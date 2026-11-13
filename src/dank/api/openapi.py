"""`dank-openapi`: write the API's OpenAPI schema without starting a server.

The reader generates its TypeScript types from this file, and a test fails
while the checked-in copy differs from what the app describes.
"""

import argparse
import json
import pathlib
import tempfile
from typing import Any

from dank.api.app import create_app
from dank.config import load_settings

ROOT = pathlib.Path(__file__).resolve().parents[3]
# The schema doesn't depend on configuration, so the checked-in example
# stands in for config.toml, which may not exist and is never read here.
EXAMPLE_CONFIG = ROOT / "config.example.toml"
SCHEMA_FILE = ROOT / "reader" / "openapi.json"


def schema() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as data:
        settings = load_settings(EXAMPLE_CONFIG)._replace(
            data_dir=pathlib.Path(data),
        )

        return create_app(settings).openapi()


def schema_text() -> str:
    return json.dumps(schema(), indent=2, ensure_ascii=False) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dank-openapi",
        description="Write the API's OpenAPI schema for the reader.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=str(SCHEMA_FILE),
        help="Where to write the schema (default: reader/openapi.json)",
    )
    args = parser.parse_args()
    pathlib.Path(args.output).write_text(schema_text(), encoding="utf-8")
