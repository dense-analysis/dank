from __future__ import annotations

import argparse
import re

from dank.scrape.runner import run_scrape_from_config
from dank.scrape.x.conversations import canonical_post_url


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
    parser.add_argument(
        "--retry-failed-pages",
        action="store_true",
        help="Retry saved RSS page failures instead of collecting feeds",
    )

    parser.add_argument(
        "--x-post", action="append", default=[], type=canonical_post_url,
        help="Capture a saved X conversation; repeat for multiple posts",
    )

    parser.add_argument(
        "--reddit-details", action="store_true",
        help="Refresh pending Reddit details saved in the last 24 hours",
    )
    parser.add_argument(
        "--x-details", action="store_true",
        help="Refresh pending X discussions saved in the last 24 hours",
    )
    args = parser.parse_args()

    if sum((bool(args.x_post), args.x_details, args.reddit_details,
            args.retry_failed_pages)) > 1:
        parser.error(
            "Discussion/detail refresh and retry modes cannot be combined",
        )

    if args.x_post and (args.domains or args.retry_failed_pages):
        parser.error(
            "--x-post cannot be combined with --domains "
            "or --retry-failed-pages",
        )

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
        retry_failed_pages=args.retry_failed_pages,
        x_posts=tuple(args.x_post),
        reddit_details=args.reddit_details,
        x_details=args.x_details,
    )


if __name__ == "__main__":
    main()
