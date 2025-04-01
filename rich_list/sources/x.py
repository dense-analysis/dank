from urllib.parse import quote
import zendriver
from typing import Any, AsyncIterator, TypedDict
from typing import cast
import asyncio


from .base import SourceSettings, Post, register_source


class XAccountSettings(TypedDict):
    name: str


class XSettings(SourceSettings):
    username: str
    password: str
    accounts: list[XAccountSettings]


async def check_is_authenticated(browser: zendriver.Browser) -> bool:
    cookies = await browser.cookies.get_all()

    for cookie in cookies:
        if cookie.name == "auth_token" and cookie.value:
            return True

    return False


async def login(
    browser: zendriver.Browser,
    username: str,
    password: str,
) -> list[dict[str, Any]]:
    page = await browser.get("https://x.com/login")

    try:
        await page.wait_for_ready_state()
    except asyncio.TimeoutError:
        # If we time out waiting for the page to be ready, proceed anyway.
        # We can timeout and still the page can actually be interacted with.
        pass

    username_input = await page.select('[autocomplete="username"]')

    await username_input.send_keys(username + '\n')

    password_input = await page.select(
        '[autocomplete="current-password"]'
    )

    await password_input.send_keys(password + '\n')

    try:
        await page.wait_for_ready_state()
    except asyncio.TimeoutError:
        # If we time out waiting for the page to be ready, proceed anyway.
        # We can timeout and still the page can actually be interacted with.
        pass

    # Store cookies after the page has loaded.
    return [
        cast(zendriver.cdp.network.Cookie, cookie_obj).to_json()
        for cookie_obj in
        await browser.cookies.get_all()
    ]



async def load_posts(
    settings: XSettings,
    source_persistent_data: dict[str, Any],
) -> AsyncIterator[Post]:
    browser = await zendriver.start(sandbox=False)

    cookies = source_persistent_data.get('cookies')

    if cookies:
        await browser.cookies.set_all([
            zendriver.cdp.network.CookieParam.from_json(cookie)
            for cookie in cookies
        ])

    async with browser:
        if not await check_is_authenticated(browser):
            source_persistent_data['cookies'] = await login(
                browser,
                settings['username'],
                settings['password'],
            )

    for account in settings['accounts']:
        quoted_name = quote(account['name'])
        page = await browser.get(f"https://x.com/{quoted_name}")

        try:
            await page.wait_for_ready_state()
        except asyncio.TimeoutError:
            # Proceed even if we timeout anyway.
            pass

        articles = await page.query_selector_all("article")

        for elem in articles:
            yield Post(title=elem.text)


register_source('x', XSettings, load_posts)
