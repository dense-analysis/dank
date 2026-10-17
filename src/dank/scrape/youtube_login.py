"""Interactive YouTube sign-in, isolated from every other collector."""

from __future__ import annotations

import argparse
import asyncio
import math
import signal
import sys

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import ConfigError, Settings, load_settings
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
)

SESSION_POLL_SECONDS = 1.0


async def signed_in(page: zendriver.Tab) -> bool:
    try:
        return await page.evaluate(
            "Boolean(location.hostname === 'www.youtube.com' "
            "&& window.ytcfg?.get('LOGGED_IN'))",
        ) is True
    except (ProtocolException, ConnectionError):
        # Google sign-in navigations replace the page's execution context.
        return False


async def sign_in(
    settings: Settings, *, timeout_seconds: float,
    check_only: bool = False, remote_desktop: bool = False,
) -> bool:
    task = asyncio.current_task()

    if task is not None:
        asyncio.get_running_loop().add_signal_handler(
            signal.SIGTERM, task.cancel,
        )

    config = BrowserConfig.from_settings(
        settings, headless=check_only,
    )._replace(
        profile_dir=(
            settings.youtube.profile_dir
            or settings.data_dir / "youtube-browser-profile"
        ),
    )

    async with BrowserSession(config) as session:
        browser = await session.get_browser()
        page = await browser.get("https://www.youtube.com/")

        if not check_only:
            destination = (
                "http://127.0.0.1:6081/vnc.html?autoconnect=1&resize=scale"
                if remote_desktop else "the opened browser window"
            )
            print(
                f"Sign in to YouTube in {destination}. "
                "Complete any verification yourself.", flush=True,
            )

        try:
            async with asyncio.timeout(timeout_seconds):
                while True:
                    if await signed_in(page):
                        break

                    await asyncio.sleep(SESSION_POLL_SECONDS)
        except TimeoutError:
            return False

    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="youtube-login",
        description="Save a separate YouTube browser session by signing in.",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--check-session", action="store_true")
    parser.add_argument("--remote-desktop", action="store_true")
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()

    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive, finite number")

    try:
        valid = asyncio.run(sign_in(
            load_settings(args.config),
            timeout_seconds=(min(args.timeout, 15)
                             if args.check_session else args.timeout),
            check_only=args.check_session,
            remote_desktop=args.remote_desktop,
        ))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("YouTube sign-in cancelled.", file=sys.stderr)
        raise SystemExit(130) from None
    except (ConfigError, OSError, BrowserProfileBusyError) as error:
        print(f"Could not open the YouTube session: {type(error).__name__}",
              file=sys.stderr)
        raise SystemExit(1) from None

    if not valid:
        print("YouTube sign-in is required or timed out.", file=sys.stderr)
        raise SystemExit(3) from None

    print(
        "YouTube session saved. Set [youtube] authenticated = true to use it. "
        "Set homepage = true to also collect your recommendations.",
    )


if __name__ == "__main__":
    main()
