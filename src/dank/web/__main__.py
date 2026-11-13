from __future__ import annotations

import argparse
import re

from dank.api.serve import ServeOptions, serve

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.web")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="Host to bind (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port to bind",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable hot code reloading",
    )
    args = parser.parse_args()

    _terminal_print(f"API docs: http://{args.host}:{args.port}/api/docs")
    serve(
        ServeOptions(args.config, args.host, args.port),
        reload=not args.no_reload,
    )


def _terminal_print(message: str) -> None:
    # OSC is the ANSI Operating System Command sequence used for
    # terminal hyperlinks.
    osc = "\x1b]8;;"
    st = "\x1b\\"

    linked = re.sub(
        r"https?://[^\s]+",
        lambda match: (
            f"{osc}{match.group(0)}{st}"
            f"{match.group(0)}"
            f"{osc}{st}"
        ),
        message,
    )
    print(linked)


if __name__ == "__main__":
    main()
