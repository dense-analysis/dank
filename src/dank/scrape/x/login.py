from __future__ import annotations

import argparse
import asyncio
import math
import signal
import sys

import zendriver

from dank.config import ConfigError, Settings, load_settings
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
)

from .onepassword import (
    LoginCredentials,
    OnePasswordError,
    load_credentials,
    read_credentials,
)
from .session import (
    SESSION_CHECK_TIMEOUT_SECONDS,
    X_HOME_URL,
    LoginRequiredError,
    check_x_session,
    wait_for_x_session,
)

SESSION_REUSED_MESSAGE = (
    "Already signed in to X. Reusing the saved session; "
    "no sign-in action needed. You can run make scrape."
)
SESSION_REQUIRED_EXIT_CODE = 3


def cancel_on_termination() -> None:
    task = asyncio.current_task()

    if task is not None:
        asyncio.get_running_loop().add_signal_handler(
            signal.SIGTERM, task.cancel,
        )


async def check_saved_session(
    settings: Settings, *, timeout_seconds: float,
) -> bool:
    cancel_on_termination()
    print("Checking saved X session...", flush=True)

    async with BrowserSession(
        BrowserConfig.from_settings(settings, headless=True),
    ) as session:
        browser = await session.get_browser()
        page = await browser.get(X_HOME_URL)
        valid = await check_x_session(page, timeout_seconds=timeout_seconds)

    if valid:
        print(SESSION_REUSED_MESSAGE, flush=True)
    else:
        print("X sign-in is required.", flush=True)

    return valid


async def fit_login_window(page: zendriver.Tab) -> None:
    size = await page.evaluate(
        "[screen.availWidth, screen.availHeight]", return_by_value=True,
    )

    match size:
        case [int() as width, int() as height] if width > 0 and height > 0:
            await page.set_window_size(width=width, height=height)
        case _:
            pass


async def sign_in(
    settings: Settings, *, timeout_seconds: float,
    credentials: LoginCredentials | None = None, manual: bool = False,
    remote_desktop: bool = False,
) -> None:
    cancel_on_termination()
    config = BrowserConfig.from_settings(settings)

    async with BrowserSession(config) as session:
        if not remote_desktop:
            print("Checking saved X session...", flush=True)

        browser = await session.get_browser()
        page = await browser.get("about:blank")
        await fit_login_window(page)
        await page.get(X_HOME_URL)
        reused = await check_x_session(
            page,
            timeout_seconds=min(
                timeout_seconds, SESSION_CHECK_TIMEOUT_SECONDS,
            ),
        )

        if not reused:
            await complete_sign_in(
                page, settings, timeout_seconds=timeout_seconds,
                credentials=credentials, manual=manual,
                remote_desktop=remote_desktop,
            )

    # Closing the browser flushes the profile before reporting success.
    print(
        SESSION_REUSED_MESSAGE if reused else
        "X sign-in complete. Session saved. You can now run make scrape.",
        flush=True,
    )


async def complete_sign_in(
    page: zendriver.Tab, settings: Settings, *, timeout_seconds: float,
    credentials: LoginCredentials | None, manual: bool,
    remote_desktop: bool,
) -> None:
    if remote_desktop:
        print(
            "X sign-in is required. Open "
            "http://127.0.0.1:6080/vnc.html?autoconnect=1&resize=scale "
            "and complete sign-in there.",
            flush=True,
        )
    else:
        print(
            "X sign-in is required. Complete it in the browser window.",
            flush=True,
        )

    if credentials is None and not manual:
        credentials = await load_credentials(settings.x.onepassword)

    if credentials is not None:
        print(
            "1Password will fill visible, empty X login fields. "
            "Click Continue yourself and complete any verification.",
            flush=True,
        )

    await wait_for_x_session(
        page, timeout_seconds=timeout_seconds, credentials=credentials,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="x-login",
        description="Sign in to X and save the browser session.",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument(
        "--manual", action="store_true",
        help="Skip optional 1Password autofill",
    )
    parser.add_argument(
        "--onepassword-references", action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--credentials-stdin", action="store_true", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--check-session", action="store_true", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--remote-desktop", action="store_true", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--timeout", type=float, default=600,
        help="Seconds to wait for sign-in (default: 600)",
    )
    args = parser.parse_args()

    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive, finite number")

    try:
        settings = load_settings(args.config)

        if args.onepassword_references:
            if settings.x.onepassword is not None and not args.manual:
                print(settings.x.onepassword.username)
                print(settings.x.onepassword.password)

            return

        if args.check_session:
            valid = asyncio.run(check_saved_session(
                settings,
                timeout_seconds=min(
                    args.timeout, SESSION_CHECK_TIMEOUT_SECONDS,
                ),
            ))

            if not valid:
                raise SystemExit(SESSION_REQUIRED_EXIT_CODE)

            return

        credentials = (
            read_credentials(sys.stdin.buffer)
            if args.credentials_stdin else None
        )
        asyncio.run(sign_in(
            settings, timeout_seconds=args.timeout,
            credentials=credentials, manual=args.manual,
            remote_desktop=args.remote_desktop,
        ))
    except (
        LoginRequiredError, BrowserProfileBusyError, OnePasswordError,
    ) as error:
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
