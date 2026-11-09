"""CLI for labelling saved post embeddings; database work stays in runner."""

from __future__ import annotations

import argparse
import asyncio
import re

from dank.config import load_settings
from dank.db.sources import SourcesError, sources_in_use
from dank.logging_setup import configure_logging
from dank.training.runner import run_train


def main() -> None:
    parser = argparse.ArgumentParser(prog="train")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--age", default="24h")
    parser.add_argument(
        "--limit",
        type=int,
        default=10_000,
        help="Maximum posts to label (default: 10,000)",
    )
    parser.add_argument(
        "--domains",
        help="Regex over configured source domains",
    )
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be positive")

    try:
        domain_regex = re.compile(args.domains) if args.domains else None
        settings = load_settings(args.config)
        configure_logging(settings.logging, component="train")
        # Sources turned off in the app still have posts to label.
        known = asyncio.run(sources_in_use(settings)).known
        asyncio.run(
            run_train(
                settings._replace(sources=known),
                age=args.age,
                limit=args.limit,
                domain_regex=domain_regex,
            ),
        )
    except (ValueError, RuntimeError, SourcesError) as error:
        parser.exit(1, f"train: {error}\n")


if __name__ == "__main__":
    main()
