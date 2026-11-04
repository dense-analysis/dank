import asyncio
from itertools import pairwise
from unittest.mock import AsyncMock, MagicMock

import pytest
from zendriver import cdp
from zendriver.core.element import Element
from zendriver.core.keys import KeyEvents, SpecialKeys

from dank.scrape.x import (
    _scroll_pause_seconds,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.x.navigation import (
    PROFILE_HEADER_SELECTOR,
    SEARCH_KEY_PAUSE_SECONDS,
    navigate_to_account,
    press_search_key,
    scroll,
    select_account_suggestion,
)
from dank.scrape.x.session import LoginRequiredError


@pytest.mark.parametrize("header_states", [
    [["@writer"]], [["@previous"], [], ["@writer"]],
])
async def test_search_selects_exact_handle_and_waits_for_profile(
    header_states: list[list[str]],
) -> None:
    key_times: list[float] = []

    async def record_key(value: object) -> None:
        if isinstance(value, list):
            key_times.append(asyncio.get_running_loop().time())

    search = MagicMock(
        click=AsyncMock(), clear_input=AsyncMock(),
        send_keys=AsyncMock(side_effect=record_key),
        apply=AsyncMock(side_effect=[
            "@previous", "", "popup", ["writer_extra", "WrItEr"],
            None, None, "@query:0", "@query:0", "WrItEr", None, "WrItEr",
        ]),
    )
    header = MagicMock(apply=AsyncMock(side_effect=header_states))
    page = MagicMock(
        select=AsyncMock(return_value=search),
        query_selector=AsyncMock(return_value=None),
        query_selector_all=AsyncMock(),
        activate=AsyncMock(),
        get=AsyncMock(),
        send=AsyncMock(),
        evaluate=AsyncMock(side_effect=[
            "https://x.com/previous", None, "https://x.com/previous",
            "https://x.com/previous", "https://x.com/writer/",
            "https://x.com/writer/",
        ] + ["https://x.com/writer/"] * 10),
    )

    async def select(
        selector: str, **kwargs: object,
    ) -> MagicMock:
        if selector == PROFILE_HEADER_SELECTOR:
            return header

        return search

    page.select.side_effect = select
    await navigate_to_account(page, "writer")
    search.click.assert_not_awaited()
    events = KeyEvents.from_mixed_input(["/"])
    assert page.send.await_count == len(events)

    for call, event in zip(page.send.await_args_list, events, strict=True):
        assert next(call.args[0]) == next(
            cdp.input_.dispatch_key_event(**event),
        )

    search.clear_input.assert_awaited_once()
    calls = [call.args[0] for call in search.send_keys.await_args_list]
    assert calls[0] == "@writer"
    assert [[event["key"] for event in call] for call in calls[1:]] == [
        ["ArrowDown", "ArrowDown"], ["ArrowDown", "ArrowDown"],
        ["Enter", "Enter"],
    ]
    assert all(
        later - earlier >= SEARCH_KEY_PAUSE_SECONDS
        for earlier, later in pairwise(key_times)
    )
    page.activate.assert_awaited_once()
    selectors = [call.args[0] for call in page.select.await_args_list]
    assert selectors[2] == (
        '[role="listbox"][id="popup"] '
        '[data-testid="UserAvatar-Container-writer" i]'
    )
    assert 'aria-selected="true"' in selectors[3]
    assert all('progressbar' not in selector for selector in selectors)
    assert selectors.count(PROFILE_HEADER_SELECTOR) == len(header_states)
    page.query_selector_all.assert_not_awaited()
    page.get.assert_not_awaited()


async def test_missing_exact_result_times_out_without_clicking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dank.scrape.x.navigation.ACCOUNT_NAVIGATION_TIMEOUT_SECONDS", 0.01,
    )
    search = MagicMock(
        click=AsyncMock(), clear_input=AsyncMock(), send_keys=AsyncMock(),
        apply=AsyncMock(side_effect=["", "popup"]),
    )
    page = MagicMock(
        evaluate=AsyncMock(return_value="https://x.com/previous"),
        select=AsyncMock(side_effect=[search, search, TimeoutError()]),
        query_selector=AsyncMock(return_value=None),
        activate=AsyncMock(),
        get=AsyncMock(),
        send=AsyncMock(),
    )

    with pytest.raises(TimeoutError):
        await navigate_to_account(page, "writer")

    assert search.send_keys.await_count == 1
    page.get.assert_not_awaited()


async def test_search_stops_when_login_is_required() -> None:
    page = MagicMock(
        evaluate=AsyncMock(return_value="https://x.com/i/flow/login"),
    )

    with pytest.raises(LoginRequiredError):
        await navigate_to_account(page, "writer")

    page.select.assert_not_called()


@pytest.mark.parametrize("selected", [None, "other", "writer_extra"])
async def test_no_more_keys_until_selection_changes(
    selected: str | None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dank.scrape.x.navigation.SEARCH_KEY_PAUSE_SECONDS", 0,
    )
    search = MagicMock(
        send_keys=AsyncMock(), apply=AsyncMock(return_value=selected),
    )
    page = MagicMock(select=AsyncMock())

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await select_account_suggestion(
                search, "writer", result_count=2, page=page,
                popup_selector='[role="listbox"]',
            )

    assert search.send_keys.await_count == 1
    assert search.send_keys.await_args.args[0][0]["key"] == "ArrowDown"
    page.select.assert_not_awaited()


async def test_enter_waits_for_selected_option() -> None:
    ready = asyncio.Event()
    search = MagicMock(
        send_keys=AsyncMock(),
        apply=AsyncMock(side_effect=[None, "writer", "writer"]),
    )
    waiting = asyncio.Event()

    async def select(selector: str, **kwargs: object) -> MagicMock:
        assert 'aria-selected="true"' in selector
        assert 'progressbar' not in selector
        assert 'UserAvatar-Container-writer' in selector
        waiting.set()
        await ready.wait()

        return MagicMock()

    page = MagicMock(select=AsyncMock(side_effect=select))
    async with asyncio.timeout(1):
        async with asyncio.TaskGroup() as group:
            task = group.create_task(select_account_suggestion(
                search, "writer", result_count=1, page=page,
                popup_selector='[role="listbox"]',
            ))
            await waiting.wait()
            assert not task.done()
            assert search.send_keys.await_count == 1
            ready.set()

    assert search.send_keys.await_args.args[0][0]["key"] == "Enter"


async def test_enter_waits_for_committed_active_descendant() -> None:
    from dank.scrape.x.navigation import CONFIRM_SELECTION_SCRIPT

    ready = asyncio.Event()
    waiting = asyncio.Event()

    async def apply(script: str, **kwargs: object) -> str:
        if script == CONFIRM_SELECTION_SCRIPT:
            assert kwargs == {"await_promise": True}
            waiting.set()
            await ready.wait()

        return "writer"

    search = MagicMock(
        send_keys=AsyncMock(), apply=AsyncMock(side_effect=apply),
    )
    page = MagicMock(select=AsyncMock())

    async with asyncio.timeout(1):
        async with asyncio.TaskGroup() as group:
            group.create_task(select_account_suggestion(
                search, "writer", 1, page=page,
                popup_selector='[role="listbox"]',
            ))
            await waiting.wait()
            search.send_keys.assert_not_awaited()
            ready.set()

    assert search.send_keys.await_args.args[0][0]["key"] == "Enter"


async def test_already_selected_account_needs_only_enter() -> None:
    search = MagicMock(
        send_keys=AsyncMock(), apply=AsyncMock(return_value="WatcherGuru"),
    )
    page = MagicMock(select=AsyncMock())
    await select_account_suggestion(
        search, "watcherguru", 9, page=page,
        popup_selector='[role="listbox"][id="typeaheadDropdown-21"]',
    )
    assert search.send_keys.await_count == 1
    assert search.send_keys.await_args.args[0][0]["key"] == "Enter"
    selector = page.select.await_args.args[0]
    assert 'aria-selected="true"' in selector
    assert 'UserAvatar-Container-WatcherGuru' in selector
    assert 'progressbar' not in selector


async def test_scroll_reaches_loaded_pagination_cursor() -> None:
    page = MagicMock(evaluate=AsyncMock())
    await scroll(page)
    script = page.evaluate.await_args.args[0]
    assert "scrollHeight" in script
    assert "window.scrollTo" in script


@pytest.mark.parametrize(("configured", "idle", "expected"), [
    (1.5, 0, 0.1), (1.5, 1, 0.1), (1.5, 2, 1.5),
    (0.05, 0, 0.05), (0, 0, 0.1),
])
def test_fast_scroll_waits_back_off_when_idle(
    configured: float, idle: int, expected: float,
) -> None:
    assert _scroll_pause_seconds(configured, idle) == expected


@pytest.mark.parametrize(("key", "name", "code"), [
    (SpecialKeys.ARROW_DOWN, "ArrowDown", 40),
    (SpecialKeys.ENTER, "Enter", 13),
])
async def test_search_keys_dispatch_native_events_with_dom_key_names(
    key: SpecialKeys, name: str, code: int,
) -> None:
    # Exercise zendriver's real send_keys and inspect the wire commands.
    page = MagicMock(send=AsyncMock())
    search = MagicMock(apply=AsyncMock(), _tab=page)

    async def send_keys(
        value: str | SpecialKeys | list[KeyEvents.Payload],
    ) -> None:
        await Element.send_keys(search, value)

    search.send_keys = send_keys
    await press_search_key(search, key)
    search.apply.assert_awaited_once_with("(elem) => elem.focus()")
    commands = [next(call.args[0]) for call in page.send.await_args_list]
    assert commands == [
        {
            "method": "Input.dispatchKeyEvent",
            "params": {
                "type": event_type, "key": name, "code": name,
                "windowsVirtualKeyCode": code, "modifiers": 0,
            },
        }
        for event_type in ("rawKeyDown", "keyUp")
    ]
