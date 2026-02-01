from __future__ import annotations

import argparse

from dank.process.runner import run_process_from_config


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.process")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Limit raw posts to process",
    )
    args = parser.parse_args()
    run_process_from_config(args.config, limit=args.limit)


if __name__ == "__main__":
    main()
