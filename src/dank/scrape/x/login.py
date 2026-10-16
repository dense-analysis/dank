from __future__ import annotations

import argparse
import asyncio
import math
import signal
import sys

from dank.config import ConfigError, Settings, load_settings
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
)

from .session import X_HOME_URL, LoginRequiredError, wait_for_x_session


async def sign_in(settings: Settings, *, timeout_seconds: float) -> None:
    task = asyncio.current_task()

    if task is not None:
        asyncio.get_running_loop().add_signal_handler(
            signal.SIGTERM, task.cancel,
        )

    config = BrowserConfig.from_settings(settings)

    async with BrowserSession(config) as session:
        browser = await session.get_browser()
        page = await browser.get(X_HOME_URL)
        print(
            "Sign in to X in the browser and complete any verification. "
            "Waiting for your signed-in home page...",
            flush=True,
        )
        await wait_for_x_session(page, timeout_seconds=timeout_seconds)

    # Closing the browser flushes the profile before reporting success.
    print("X session saved. You can now run make scrape.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="x-login",
        description="Sign in to X and save the browser session.",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument(
        "--timeout", type=float, default=600,
        help="Seconds to wait for sign-in (default: 600)",
    )
    args = parser.parse_args()

    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive, finite number")

    try:
        settings = load_settings(args.config)
        asyncio.run(sign_in(settings, timeout_seconds=args.timeout))
    except (LoginRequiredError, BrowserProfileBusyError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("X sign-in cancelled.", file=sys.stderr)
        raise SystemExit(130) from None
    except (ConfigError, OSError):
        print("Could not open the configuration or browser profile.",
              file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
