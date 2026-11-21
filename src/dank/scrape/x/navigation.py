from __future__ import annotations

import asyncio
import json
import logging
from typing import cast
from urllib.parse import urlsplit

import zendriver
from zendriver import cdp
from zendriver.core.keys import KeyEvents, SpecialKeys

from .session import raise_for_login_state, read_session_state

logger = logging.getLogger(__name__)

READY_STATE_TIMEOUT_SECONDS = 2
ACCOUNT_NAVIGATION_TIMEOUT_SECONDS = 15
SEARCH_SHORTCUT_TIMEOUT_SECONDS = 2
TYPEAHEAD_POLL_SECONDS = 0.1
# Operator-observed X input races need pacing in addition to DOM readiness.
SEARCH_KEY_PAUSE_SECONDS = 0.25
SEARCH_INPUT_SELECTOR = '[data-testid="SearchBox_Search_Input"]'
ACCOUNT_SUGGESTIONS_SCRIPT = """(input) => {
    const popup = document.getElementById(
        input.getAttribute('aria-owns') || input.getAttribute('aria-controls')
    );
    return Array.from(popup?.querySelectorAll(
        '[role="option"] ' +
        '[data-testid^="UserAvatar-Container-"]'
    ) || [], avatar => avatar.getAttribute('data-testid')
        .slice('UserAvatar-Container-'.length));
}"""

# Read X's keyboard selection from the input's own popup. Display names are
# ambiguous; the avatar test ID contains the account's actual handle.
SELECTED_ACCOUNT_SCRIPT = """(input) => {
    if (document.activeElement !== input) return null;
    const popup = document.getElementById(
        input.getAttribute('aria-owns') || input.getAttribute('aria-controls')
    );
    const active = document.getElementById(
        input.getAttribute('aria-activedescendant')
    );
    const activeOption = active?.closest('[role="option"]');
    const selected = (active && popup?.contains(active) &&
        activeOption?.getAttribute('aria-selected') !== 'false'
        ? activeOption || active : null) || popup?.querySelector(
        '[role="option"][aria-selected="true"]'
    );
    const avatar = selected?.querySelector(
        '[data-testid^="UserAvatar-Container-"]'
    );
    if (!selected) return null;
    return avatar?.getAttribute('data-testid')
        .slice('UserAvatar-Container-'.length) ||
        '@query:' + Array.from(popup.querySelectorAll('[role="option"]'))
            .indexOf(selected);
}"""


# Confirm X has committed the same active option across browser render turns.
# A selected attribute can appear before the keyboard handler sees that state.
CONFIRM_SELECTION_SCRIPT = """async (input) => {
    const readSelection = (""" + SELECTED_ACCOUNT_SCRIPT + """);
    const selected = readSelection(input);
    const id = input.getAttribute('aria-activedescendant');
    if (!selected) return null;
    await new Promise(requestAnimationFrame);
    await new Promise(requestAnimationFrame);
    return input.getAttribute('aria-activedescendant') === id &&
        readSelection(input) === selected ? selected : null;
}"""


async def press_search_key(
    search: zendriver.Element, key: SpecialKeys,
) -> None:
    # zendriver encodes SpecialKeys.ENTER with key="\r", not the DOM name.
    # Explicit CDP payloads keep native events; send_keys focuses the input.
    name, code = key.value
    events: list[KeyEvents.Payload] = [
        {
            "type_": event_type, "key": name, "code": name,
            "windows_virtual_key_code": code,
            "modifiers": 0, "text": None, "native_virtual_key_code": None,
        }
        for event_type in ("rawKeyDown", "keyUp")
    ]
    await search.send_keys(events)


async def select_account_suggestion(
    search: zendriver.Element, handle: str, result_count: int,
) -> None:
    # X handles Down/Enter on the combobox, not the nested result button.
    # Include the initial selection check and the "Search for"/"Go to" options.
    selected = await search.apply(SELECTED_ACCOUNT_SCRIPT)

    for _ in range(result_count + 3):
        if (
            isinstance(selected, str)
            and selected.casefold() == handle.casefold()
        ):
            await asyncio.sleep(SEARCH_KEY_PAUSE_SECONDS)
            while True:
                committed = await search.apply(
                    CONFIRM_SELECTION_SCRIPT, await_promise=True,
                )

                if committed == selected:
                    break

                await asyncio.sleep(TYPEAHEAD_POLL_SECONDS)

            logger.info(
                "X search @%s: selected @%s; pressing Enter", handle, selected,
            )
            await press_search_key(search, SpecialKeys.ENTER)

            return

        previous = selected
        await asyncio.sleep(SEARCH_KEY_PAUSE_SECONDS)
        logger.info("X search @%s: pressing ArrowDown", handle)
        await press_search_key(search, SpecialKeys.ARROW_DOWN)

        # The query option has no avatar: wait for option identity, not just a
        # handle, to change. Never send another Down while X is still updating.
        while True:
            selected = await search.apply(SELECTED_ACCOUNT_SCRIPT)

            if isinstance(selected, str) and selected != previous:
                break

            await asyncio.sleep(TYPEAHEAD_POLL_SECONDS)

    raise TimeoutError(f"X search could not select @{handle}")


async def navigate_to_account(page: zendriver.Tab, handle: str) -> None:
    """Switch profiles through X's router without reloading the document."""
    await check_login_required(page)
    await page.activate()

    for attempt in range(1, 3):
        try:
            await search_for_account(page, handle, retry=attempt > 1)

            return
        except TimeoutError as error:
            await check_login_required(page)

            if attempt == 2:
                raise

            logger.info(
                "X search @%s: %s; retrying search once",
                handle, error,
            )


async def search_for_account(
    page: zendriver.Tab, handle: str, *, retry: bool,
) -> None:
    stage = "focusing search"

    try:
        async with asyncio.timeout(ACCOUNT_NAVIGATION_TIMEOUT_SECONDS):
            logger.info("X search @%s: focusing search with /", handle)
            # Release stale focus before invoking the shortcut.
            await page.evaluate("document.activeElement?.blur()")

            for event in KeyEvents.from_mixed_input(["/"]):
                await page.send(cdp.input_.dispatch_key_event(**event))

            search = await page.select(
                SEARCH_INPUT_SELECTOR, timeout=SEARCH_SHORTCUT_TIMEOUT_SECONDS,
            )

            if retry:
                # Focus the input if the shortcut did not open the popup.
                await search.click()

            stage = "clearing search"
            await search.clear_input()

            while True:
                if await search.apply("(input) => input.value") == "":
                    break

                await asyncio.sleep(TYPEAHEAD_POLL_SECONDS)

            await search.send_keys(f"@{handle}")

            stage = "opening suggestions"
            logger.info(
                "X search @%s: query entered; waiting for suggestions", handle,
            )

            await page.select(
                f'{SEARCH_INPUT_SELECTOR}[aria-expanded="true"]'
                ':is([aria-owns],[aria-controls])',
                timeout=ACCOUNT_NAVIGATION_TIMEOUT_SECONDS,
            )
            popup_id = await search.apply(
                "(input) => input.getAttribute('aria-owns') || "
                "input.getAttribute('aria-controls')",
            )
            popup_selector = f'[role="listbox"][id={json.dumps(popup_id)}]'
            avatar = json.dumps(f"UserAvatar-Container-{handle}")
            stage = "waiting for exact suggestion"
            logger.info("X search @%s: %s", handle, stage)
            await page.select(
                f'{popup_selector} [data-testid={avatar} i]',
                timeout=ACCOUNT_NAVIGATION_TIMEOUT_SECONDS,
            )

            results = cast(
                list[str], await search.apply(ACCOUNT_SUGGESTIONS_SCRIPT),
            )
            logger.info("X search @%s: found exact suggestion", handle)
            stage = "selecting exact suggestion"
            await select_account_suggestion(search, handle, len(results))

            stage = "waiting for profile route"
            logger.info("X search @%s: %s", handle, stage)

            while True:
                location = await get_location(page)

                if location and (
                    urlsplit(location).path.rstrip("/").casefold()
                    == f"/{handle.casefold()}"
                ):
                    return

                await asyncio.sleep(TYPEAHEAD_POLL_SECONDS)
    except asyncio.CancelledError:
        logger.info("X search @%s: interrupted while %s", handle, stage)
        raise
    except TimeoutError as error:
        raise TimeoutError(f"X search @{handle}: {stage}") from error


async def check_login_required(page: zendriver.Tab) -> None:
    raise_for_login_state(state=await read_session_state(page))


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


async def scroll(page: zendriver.Tab) -> None:
    # Reach pagination immediately; posts come from network payloads.
    await page.evaluate(
        "window.scrollTo(0, "
        "(document.scrollingElement || document.documentElement)"
        ".scrollHeight)",
    )


async def scroll_conversation(page: zendriver.Tab) -> None:
    await scroll(page)
