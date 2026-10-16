from __future__ import annotations

import asyncio

import zendriver
from zendriver.core.connection import ProtocolException

X_HOME_URL = "https://x.com/home"
LOGIN_REQUIRED_MESSAGE = (
    "X sign-in required. Run make x-login (Docker) or "
    "make x-login MODE=native, then retry collection."
)


class LoginRequiredError(RuntimeError):
    pass


async def is_signed_in(page: zendriver.Tab) -> bool:
    """Require authenticated navigation on X, not merely a loaded page."""
    try:
        return await page.evaluate(
            "location.origin === 'https://x.com' && "
            "!!document.querySelector("
            "'[data-testid=SideNav_AccountSwitcher_Button]') && "
            "!!document.querySelector('[data-testid=AppTabBar_Home_Link]')",
        ) is True
    except (ProtocolException, ConnectionError):
        # Navigation can briefly destroy the execution context.
        return False


async def wait_for_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
) -> None:
    try:
        async with asyncio.timeout(timeout_seconds):
            while not await is_signed_in(page):
                if page.browser and page.browser.stopped:
                    raise LoginRequiredError("Browser closed before sign-in.")

                await asyncio.sleep(0.5)
    except TimeoutError:
        raise LoginRequiredError(LOGIN_REQUIRED_MESSAGE) from None
