from __future__ import annotations

import zendriver
from zendriver import cdp

from .session import LOGIN_REQUIRED_MESSAGE, LoginRequiredError

READY_STATE_TIMEOUT_SECONDS = 2
LOGIN_PROMPT_TIMEOUT_SECONDS = 0.25


async def check_login_required(page: zendriver.Tab) -> None:
    if await is_login_page(page):
        raise LoginRequiredError(LOGIN_REQUIRED_MESSAGE)


async def is_login_page(page: zendriver.Tab) -> bool:
    location = await get_location(page)

    if not location:
        return await has_login_prompt(page)

    if any(path in location for path in (
        "/i/flow/", "/login", "/i/jf/onboarding/", "/account/access",
    )):
        return True

    return await has_login_prompt(page)


async def ensure_navigation(page: zendriver.Tab, url: str) -> None:
    await wait_for_ready_state(page)

    location = await get_location(page)

    if location and location != "about:blank":
        return

    try:
        await page.send(cdp.page.navigate(url))
    except Exception:
        return

    await wait_for_ready_state(page)


async def wait_for_ready_state(page: zendriver.Tab) -> None:
    try:
        # Bound ready-state waits because X keeps active requests open.
        await page.wait_for_ready_state(timeout=READY_STATE_TIMEOUT_SECONDS)
    except TimeoutError:
        pass


async def get_location(page: zendriver.Tab) -> str | None:
    try:
        location = await page.evaluate("location.href")
    except Exception:
        return None

    if not isinstance(location, str):
        return None

    return location


async def has_login_prompt(page: zendriver.Tab) -> bool:
    # Only wait briefly for a login selector to appear.
    try:
        await page.select(
            'a[href="/i/flow/signup"]',
            timeout=LOGIN_PROMPT_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        return False
    else:
        return True


async def scroll(page: zendriver.Tab) -> None:
    await page.evaluate(
        "(() => {"
        "const viewport = window.innerHeight || 800;"
        "const step = Math.max(500, Math.floor(viewport * 1.5));"
        "const root = document.scrollingElement || document.documentElement;"
        "const maxTop = Math.max(0, root.scrollHeight - viewport);"
        "const nextTop = Math.min(root.scrollTop + step, maxTop);"
        "window.scrollTo(0, nextTop);"
        "})()",
    )




async def scroll_conversation(page: zendriver.Tab) -> None:
    # Reach the loaded replies' cursor within the small conversation budget.
    await page.evaluate(
        "window.scrollTo(0, "
        "(document.scrollingElement || document.documentElement)"
        ".scrollHeight)",
    )
