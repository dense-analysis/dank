"""Command-line entry point for raw-capture and downloaded-media retention."""

from __future__ import annotations

import argparse
import asyncio

from dank.age import parse_age_window
from dank.collection_lock import CollectionBusyError
from dank.config import load_settings
from dank.logging_setup import configure_logging
from dank.scrub.runner import run_scrub


def main() -> None:
    parser = argparse.ArgumentParser(prog="scrub")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument(
        "--age", required=True,
        help="Remove raw captures/media older than this age (30d, 24h)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report the selected rows and files without deleting them",
    )
    args = parser.parse_args()

    try:
        age = parse_age_window(args.age)
    except (ValueError, OverflowError) as error:
        parser.error(str(error))

    try:
        settings = load_settings(args.config)
        configure_logging(settings.logging, component="scrub")
        asyncio.run(run_scrub(settings, age=age, dry_run=args.dry_run))
    except (CollectionBusyError, OSError, OverflowError) as error:
        parser.exit(1, f"scrub: {error}\n")


if __name__ == "__main__":
    main()
