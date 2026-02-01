from __future__ import annotations

import argparse

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
    args = parser.parse_args()
    run_scrape_from_config(args.config, headless=args.headless)


if __name__ == "__main__":
    main()
