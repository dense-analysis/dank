from __future__ import annotations

import asyncio

import zendriver
from zendriver.core.connection import ProtocolException

X_HOME_URL = "https://x.com/home"
SESSION_POLL_SECONDS = 2
LOGIN_LIMITED_MESSAGE = (
    "X has temporarily limited sign-in. Stop retrying and wait for X's "
    "restriction to clear before running x-login again."
)
LOGIN_REQUIRED_MESSAGE = (
    "X sign-in required. Run make x-login (Docker) or "
    "make x-login MODE=native, then retry collection."
)


class LoginRequiredError(RuntimeError):
    pass


async def is_signed_in(page: zendriver.Tab) -> bool:
    """Require authenticated navigation on X, not merely a loaded page."""
    try:
        state = await page.evaluate(
            "(() => {"
            "if (location.origin !== 'https://x.com') return false;"
            "if (document.body?.innerText.toLowerCase().includes("
            "'temporarily limited your login')) return 'limited';"
            "return !!document.querySelector("
            "'[data-testid=SideNav_AccountSwitcher_Button]') && "
            "!!document.querySelector('[data-testid=AppTabBar_Home_Link]');"
            "})()",
        )
    except (ProtocolException, ConnectionError):
        # Navigation can briefly destroy the execution context.
        return False

    if state == "limited":
        raise LoginRequiredError(LOGIN_LIMITED_MESSAGE)

    return state is True


async def wait_for_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
) -> None:
    try:
        async with asyncio.timeout(timeout_seconds):
            while not await is_signed_in(page):
                if page.browser and page.browser.stopped:
                    raise LoginRequiredError("Browser closed before sign-in.")

                await asyncio.sleep(SESSION_POLL_SECONDS)
    except TimeoutError:
        raise LoginRequiredError(LOGIN_REQUIRED_MESSAGE) from None
