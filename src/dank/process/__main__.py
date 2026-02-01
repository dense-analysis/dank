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
        "--age",
        default="24h",
        help="How far back to process (e.g. 30s, 10m, 2h)",
    )
    args = parser.parse_args()
    run_process_from_config(args.config, age=args.age)


if __name__ == "__main__":
    main()
