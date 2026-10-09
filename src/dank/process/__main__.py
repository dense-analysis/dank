from __future__ import annotations

import argparse
import re

from dank.process.runner import run_process_from_config


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.process")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--age",
        default="24h",
        help="How far back to process (30s, 10m, 2h, or all saved captures)",
    )
    parser.add_argument(
        "--domains",
        help="Regex pattern to select source domains from config",
    )
    parser.add_argument(
        "--reprocess", action="store_true",
        help="Rebuild posts in the age window, including processed records",
    )
    args = parser.parse_args()
    domain_regex = None

    if args.domains is not None:
        try:
            domain_regex = re.compile(args.domains)
        except re.error as error:
            parser.error(f"Invalid --domains value: {error}")

    run_process_from_config(
        args.config, age=args.age, domain_regex=domain_regex,
        reprocess=args.reprocess,
    )


if __name__ == "__main__":
    main()
