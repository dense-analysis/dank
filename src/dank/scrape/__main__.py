from __future__ import annotations

import argparse
import re

from dank.scrape.runner import run_scrape_from_config


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.scrape")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run the browser in headless mode",
    )
    parser.add_argument(
        "--domains",
        help=(
            "Regex pattern to select source domains from config "
            "(for example, '^x\\.com$')"
        ),
    )

    args = parser.parse_args()
    domain_regex = None

    if args.domains:
        try:
            domain_regex = re.compile(args.domains)
        except re.error as error:
            parser.error(f"Invalid --domains value: {error}")

    run_scrape_from_config(
        args.config,
        headless=args.headless,
        domain_regex=domain_regex,
    )


if __name__ == "__main__":
    main()
