from __future__ import annotations

import asyncio

import zendriver
from zendriver.core.connection import ProtocolException

from .onepassword import (
    PASSWORD_SELECTOR,
    USERNAME_SELECTOR,
    LoginCredentials,
    fill_field,
)

X_HOME_URL = "https://x.com/home"
SESSION_POLL_SECONDS = 2
SESSION_CHECK_TIMEOUT_SECONDS = 20
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


async def read_session_state(page: zendriver.Tab) -> bool | str:
    """Distinguish a signed-in page, a login page and a page still loading."""
    try:
        state = await page.evaluate(
            "(() => {"
            "if (location.origin !== 'https://x.com') return false;"
            "if (document.body?.innerText.toLowerCase().includes("
            "'temporarily limited your login')) return 'limited';"
            "if (document.querySelector("
            "'[data-testid=SideNav_AccountSwitcher_Button]') && "
            "document.querySelector('[data-testid=AppTabBar_Home_Link]')) "
            "return true;"
            "if (['/i/flow/', '/i/jf/onboarding/', "
            "'/login', '/account/access']"
            ".some(path => location.pathname.startsWith(path))) "
            "return 'login';"
            "return false;"
            "})()",
        )
    except (ProtocolException, ConnectionError):
        # Navigation can briefly destroy the execution context.
        return False

    if state == "limited":
        raise LoginRequiredError(LOGIN_LIMITED_MESSAGE)

    if state == "login":
        return "login"

    return state is True


async def is_signed_in(page: zendriver.Tab) -> bool:
    return await read_session_state(page) is True


async def check_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
) -> bool:
    try:
        async with asyncio.timeout(timeout_seconds):
            while True:
                state = await read_session_state(page)

                if state is True:
                    return True

                if state == "login":
                    return False

                if page.browser and page.browser.stopped:
                    raise LoginRequiredError(
                        "Browser closed during session check.",
                    )

                await asyncio.sleep(SESSION_POLL_SECONDS)
    except TimeoutError:
        raise LoginRequiredError(
            "Could not confirm the saved X session. "
            "Check the connection and try again.",
        ) from None


async def wait_for_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
    credentials: LoginCredentials | None = None,
) -> None:
    fields = (
        [(USERNAME_SELECTOR, credentials.username),
         (PASSWORD_SELECTOR, credentials.password)] if credentials else []
    )

    try:
        async with asyncio.timeout(timeout_seconds):
            while not await is_signed_in(page):
                if page.browser and page.browser.stopped:
                    raise LoginRequiredError("Browser closed before sign-in.")

                pending: list[tuple[str, str]] = []

                for selector, value in fields:
                    if not await fill_field(page, selector, value):
                        pending.append((selector, value))

                fields = pending

                await asyncio.sleep(SESSION_POLL_SECONDS)
    except TimeoutError:
        raise LoginRequiredError(LOGIN_REQUIRED_MESSAGE) from None
