"""Save an isolated YouTube session through Google or manual sign-in."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import signal
import sys

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import ConfigError, Settings, load_settings
from dank.scrape.youtube_google_login import (
    GoogleCredentialProvider,
    GoogleLoginError,
    google_sign_in,
)
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
)
from dank.youtube_session import save_youtube_session_from_page

SESSION_POLL_SECONDS = 1.0
SESSION_CHECK_SECONDS = 15.0
# Settling is a bounded heuristic; reopening proves disk persistence.
SESSION_SETTLE_SECONDS = 2.0
SESSION_FINALIZE_SECONDS = 15.0


async def _session_state(page: zendriver.Tab) -> bool | str:
    try:
        state = await page.evaluate(
            "(() => {"
            "if (location.origin !== 'https://www.youtube.com') return false;"
            "const loggedIn = window.ytcfg?.get('LOGGED_IN');"
            "return loggedIn === true ? true : "
            "(loggedIn === false ? 'signed_out' : false);"
            "})()",
        )
    except (ProtocolException, ConnectionError):
        # Google sign-in navigations replace the page's execution context.
        return False

    return "signed_out" if state == "signed_out" else state is True


async def signed_in(page: zendriver.Tab) -> bool:
    return await _session_state(page) is True


async def _wait_for_session(
    page: zendriver.Tab, timeout_seconds: float, *,
    stop_signed_out: bool = False,
) -> bool:
    try:
        async with asyncio.timeout(timeout_seconds):
            while True:
                state = await _session_state(page)

                if state is True:
                    return True

                if stop_signed_out and state == "signed_out":
                    return False

                await asyncio.sleep(SESSION_POLL_SECONDS)
    except TimeoutError:
        return False


async def _ready_document(page: zendriver.Tab) -> float | None:
    try:
        marker = await page.evaluate(
            "location.origin === 'https://www.youtube.com' && "
            "document.readyState === 'complete' && "
            "window.ytcfg?.get('LOGGED_IN') === true "
            "? performance.timeOrigin : null",
        )
    except (ProtocolException, ConnectionError):
        return None

    if (isinstance(marker, int | float) and not isinstance(marker, bool)
            and math.isfinite(marker) and marker > 0):
        return float(marker)

    return None


async def _settled_document(
    page: zendriver.Tab, previous: float | None = None,
) -> float:
    loop = asyncio.get_running_loop()
    stable_since = loop.time()
    last: float | None = None

    while True:
        marker = await _ready_document(page)

        if marker is None or marker == previous or marker != last:
            stable_since = loop.time()

        last = marker

        if (marker is not None and marker != previous
                and loop.time() - stable_since >= SESSION_SETTLE_SECONDS):
            return marker

        await asyncio.sleep(SESSION_POLL_SECONDS)


async def _finalize_session(page: zendriver.Tab) -> bool:
    # A fresh document must authenticate using cookies before we close Chrome.
    # Reopening the profile still independently verifies persistence on disk.
    try:
        async with asyncio.timeout(SESSION_FINALIZE_SECONDS):
            previous = await _settled_document(page)
            await page.get("https://www.youtube.com/?hl=en-GB")
            await _settled_document(page, previous)

        return True
    except (TimeoutError, ProtocolException, ConnectionError):
        return False


async def _open_session(
    settings: Settings, config: BrowserConfig, *, timeout_seconds: float,
    check_only: bool, remote_desktop: bool, manual: bool,
    credentials_stdio: bool,
) -> bool:
    session_path = (
        config.profile_dir or settings.data_dir / "youtube-browser-profile"
    ) / "youtube-session.json"

    async with BrowserSession(config) as session:
        browser = await session.get_browser()
        page = await browser.get("https://www.youtube.com/")
        existing = await _wait_for_session(
            page, min(timeout_seconds, SESSION_CHECK_SECONDS),
            stop_signed_out=not check_only,
        )

        if check_only or existing:
            if existing:
                await save_youtube_session_from_page(
                    browser, page, session_path,
                )

            return existing

        if settings.youtube.onepassword is not None and not manual:
            provider = GoogleCredentialProvider(
                settings.youtube.onepassword, stdio=credentials_stdio,
            )
            print("Signing in to YouTube with 1Password.", flush=True)
            await google_sign_in(
                page, provider, timeout_seconds=timeout_seconds,
            )

            if not await _finalize_session(page):
                raise GoogleLoginError(
                    "Google sign-in did not authenticate "
                    "a clean YouTube home.",
                )

            saved = await save_youtube_session_from_page(
                browser, page, session_path,
            )

            if not saved:
                raise GoogleLoginError(
                    "Could not save the authenticated YouTube session.",
                )

            return True

        destination = (
            "http://127.0.0.1:6081/vnc.html?autoconnect=1&resize=scale"
            if remote_desktop else "the opened browser window"
        )
        print(
            f"Sign in to YouTube in {destination}. "
            "Complete any verification yourself.", flush=True,
        )

        if not await _wait_for_session(page, timeout_seconds):
            return False

        if not await _finalize_session(page):
            return False

        return await save_youtube_session_from_page(
            browser, page, session_path,
        )


async def sign_in(
    settings: Settings, *, timeout_seconds: float,
    check_only: bool = False, remote_desktop: bool = False,
    manual: bool = False, credentials_stdio: bool = False,
) -> bool:
    task = asyncio.current_task()

    if task is not None:
        asyncio.get_running_loop().add_signal_handler(
            signal.SIGTERM, task.cancel,
        )

    automated = settings.youtube.onepassword is not None and not manual
    config = BrowserConfig.from_settings(
        settings, headless=check_only or automated,
    )._replace(
        profile_dir=(
            settings.youtube.profile_dir
            or settings.data_dir / "youtube-browser-profile"
        ),
    )
    config = config._replace(
        session_file=(config.profile_dir or settings.data_dir
                      / "youtube-browser-profile") / "youtube-session.json",
    )
    valid = await _open_session(
        settings, config, timeout_seconds=timeout_seconds,
        check_only=check_only, remote_desktop=remote_desktop,
        manual=manual, credentials_stdio=credentials_stdio,
    )

    if not valid or check_only:
        return valid

    # Chromium must flush the profile before a new browser proves persistence.
    print("Verifying the saved YouTube session after reopening.", flush=True)
    return await _open_session(
        settings, config._replace(headless=True),
        timeout_seconds=min(timeout_seconds, SESSION_CHECK_SECONDS),
        check_only=True, remote_desktop=False, manual=True,
        credentials_stdio=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="youtube-login",
        description="Save a separate YouTube browser session by signing in.",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--check-session", action="store_true")
    parser.add_argument("--remote-desktop", action="store_true")
    parser.add_argument("--manual", action="store_true")
    parser.add_argument("--credentials-stdio", action="store_true")
    parser.add_argument("--onepassword-references", action="store_true")
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()

    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive, finite number")

    try:
        settings = load_settings(args.config)

        if args.onepassword_references:
            refs = settings.youtube.onepassword
            print(json.dumps(
                refs._asdict() if refs and not args.manual else {},
            ))

            return

        valid = asyncio.run(sign_in(
            settings,
            timeout_seconds=(min(args.timeout, SESSION_CHECK_SECONDS)
                             if args.check_session else args.timeout),
            check_only=args.check_session, remote_desktop=args.remote_desktop,
            manual=args.manual, credentials_stdio=args.credentials_stdio,
        ))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("YouTube sign-in cancelled.", file=sys.stderr)
        raise SystemExit(130) from None
    except GoogleLoginError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(3) from None
    except (ConfigError, OSError, BrowserProfileBusyError) as error:
        print(f"Could not open the YouTube session: {type(error).__name__}",
              file=sys.stderr)
        raise SystemExit(1) from None

    if not valid:
        print("YouTube sign-in is required or was not saved. "
              "Retry youtube-login or use --manual.", file=sys.stderr)
        raise SystemExit(3) from None

    print(
        "YouTube session verified. Set [youtube] authenticated = true to use "
        "it. Set homepage = true to also collect your recommendations.",
    )


if __name__ == "__main__":
    main()
