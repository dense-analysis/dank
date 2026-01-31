import asyncio
import datetime
import threading
import time
from collections.abc import AsyncIterator
from typing import Any, NamedTuple, cast

import zendriver

from dank.model import Post


async def find(
    elem: zendriver.Tab | zendriver.Element,
    css: str,
) -> zendriver.Element | None:
    '''Find a single element from a page or element.'''
    return cast(
        zendriver.Element | None,
        await elem.query_selector(css),  # type: ignore
    )


async def find_all(
    elem: zendriver.Tab | zendriver.Element,
    css: str,
) -> list[zendriver.Element]:
    '''Find a multiple elements from a page or element.'''
    return cast(
        list[zendriver.Element],
        await elem.query_selector_all(css),  # type: ignore
    )


_thread_local = threading.local()
browser_lock = asyncio.Lock()


class XSettings(NamedTuple):
    username: str
    password: str
    max_posts: int


async def check_is_authenticated(browser: zendriver.Browser) -> bool:
    cookies = await browser.cookies.get_all()

    for cookie in cookies:
        if (
            (cookie.name == "auth_token"
            and cookie.domain == 'x.com'
            and cookie.value
            and cookie.expires) or 0.0 < time.time()
        ):
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
    except TimeoutError:
        # If we time out waiting for the page to be ready, proceed anyway.
        # We can timeout and still the page can actually be interacted with.
        pass

    username_input = await page.select('[autocomplete="username"]')

    await username_input.send_keys(username + '\n')

    password_input = await page.select(
        '[autocomplete="current-password"]',
    )

    await password_input.send_keys(password + '\n')

    try:
        await page.wait_for_ready_state()
    except TimeoutError:
        # If we time out waiting for the page to be ready, proceed anyway.
        # We can timeout and still the page can actually be interacted with.
        pass

    # Store cookies after the page has loaded.
    return [
        cast(zendriver.cdp.network.Cookie, cookie_obj).to_json()
        for cookie_obj in
        await browser.cookies.get_all()
    ]


async def get_browser(
    settings: XSettings,
    data_store: dict[str, Any],
) -> zendriver.Browser:
    async with browser_lock:
        browser: zendriver.Browser | None = getattr(
            _thread_local,
            'browser',
            None,
        )

        # If a browser isn't in thread-local storage then start one and set the
        # cookies we have already.
        if browser is None:
            browser = await zendriver.start(sandbox=False)

            cookies = data_store.get('cookies')

            if cookies:
                await browser.cookies.set_all([
                    zendriver.cdp.network.CookieParam.from_json(cookie)
                    for cookie in cookies
                ])

            _thread_local.browser = browser

        # If not logged in to x.com, then log in and keep the cookies.
        if not await check_is_authenticated(browser):
            data_store['cookies'] = await login(
                browser,
                settings.username,
                settings.password,
            )

    return browser


async def extract_post_url(post: zendriver.Element) -> str | None:
    link_elem = await find(post, 'a[href*="/status/"]')

    if link_elem is not None:
        return cast(str, link_elem.attrs['href'])

    return None


async def extract_post_created_at(
    post: zendriver.Element,
) -> datetime.datetime | None:
    time_elem = await find(post, 'time')

    if time_elem is not None:
        return datetime.datetime.fromisoformat(
            cast(str, time_elem.attrs['datetime']),
        )

    return None


async def handle_url(
    url: str,
    settings: XSettings,
    data_store: dict[str, Any],
) -> AsyncIterator[Post]:
    browser = await get_browser(settings, data_store)

    page = await browser.get(url)

    async with page:
        try:
            await page.wait_for_ready_state()
        except TimeoutError:
            # Proceed even if we timeout anyway.
            pass

        remaining_posts = settings.max_posts

        while remaining_posts >= 0:
            posts = await find_all(page, "article")
            remaining_posts -= len(posts)

            for post in posts:
                if '/status/' in url:
                    post_url = url
                else:
                    post_url = await extract_post_url(post)

                created_at = await extract_post_created_at(post)

                if post_url and created_at:
                    yield Post(
                        url=post_url,
                        title='',

                        created_at=created_at,
                        updated_at=created_at,
                    )

            if '/status/' in url:
                remaining_posts = 0
