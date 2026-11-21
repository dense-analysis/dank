from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections import deque
from typing import NamedTuple

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import EmailSettings

from .imap_email import (
    EMAIL_CODE_TIMEOUT_SECONDS,
    EmailVerificationError,
    wait_for_code,
)
from .onepassword import (
    FIELD_STATE,
    PASSWORD_SELECTOR,
    USERNAME_SELECTOR,
    VISIBLE_ELEMENT,
    LoginCredentials,
)
from .session import (
    EMAIL_ADDRESS_FIELDS,
    EMAIL_CODE_FIELDS,
    LOGIN_EMAIL_ADDRESS_MESSAGE,
    LOGIN_NO_IMAP_MESSAGE,
    LOGIN_VERIFICATION_MESSAGE,
    MANUAL_VERIFICATION,
    SESSION_STATE,
    X_LOGIN_URL,
    LoginRequiredError,
    wait_for_x_state,
)

logger = logging.getLogger(__name__)
SIGN_IN_TIMEOUT_SECONDS = 45
LOGIN_KEY_PAUSE_SECONDS = 0.1
FIND_SUBMIT_BUTTON = """(input, labels) => {
    const visible = (""" + VISIBLE_ELEMENT + """);
    const enabled = button => visible(button) && !button.disabled &&
        button.getAttribute('aria-disabled') !== 'true';
    // The onboarding form renders separate mobile and desktop submit buttons.
    let buttons = Array.from(input.form?.elements || []).filter(button =>
        button.matches('button[type="submit"], input[type="submit"]') &&
        enabled(button));
    if (!input.form) {
        const dialog = input.closest('[role="dialog"]') || document;
        buttons = Array.from(dialog.querySelectorAll('button,[role="button"]'))
            .filter(button => enabled(button) && (
                (input.type === 'password' &&
                    button.dataset.testid === 'LoginForm_Login_Button') ||
                labels.includes(button.textContent.trim().toLowerCase())
            ));
    }
    return buttons.length === 1 ? buttons[0] : null;
}"""
SUBMIT_FIELD = """input => {
    const visible = (""" + VISIBLE_ELEMENT + """);
    if ((""" + SESSION_STATE + """) !== STAGE || !visible(input) ||
        !input.value || !input.validity.valid) {
        return false;
    }
    const button = (""" + FIND_SUBMIT_BUTTON + """)(input, LABELS);
    if (!button) return false;
    button.click();
    return true;
}"""
LOGIN_STEP_READY = """state => {
    if (SIGNALS.includes(state)) return true;
    const step = WAITING_FIELDS[state];
    if (!step) return false;
    const visible = (""" + VISIBLE_ELEMENT + """);
    const findButton = (""" + FIND_SUBMIT_BUTTON + """);
    return Array.from(document.querySelectorAll(step[0])).some(input =>
        visible(input) && !input.disabled && !input.readOnly &&
        (input.value === '' ||
            (input.validity.valid && findButton(input, step[1])))
    );
}"""


class _LoginField(NamedTuple):
    selector: str
    value: str
    buttons: tuple[str, ...]


async def sign_in_automatically(
    page: zendriver.Tab, credentials: LoginCredentials,
    *, email: str = "", imap: EmailSettings | None = None,
) -> None:
    """Submit sign-in fields once, including a fresh email code if needed."""
    if not email and "@" in credentials.username:
        email = credentials.username

    steps = {
        "login": deque((
            _LoginField(USERNAME_SELECTOR, credentials.username,
                        ("next", "continue")),
            _LoginField(PASSWORD_SELECTOR, credentials.password,
                        ("log in", "sign in")),
        )),
        "email_address": deque([
            _LoginField(EMAIL_ADDRESS_FIELDS, email, ("next", "continue")),
        ] if email else []),
    }
    since = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    loop = asyncio.get_running_loop()

    try:
        async with asyncio.timeout(SIGN_IN_TIMEOUT_SECONDS) as deadline:
            await page.get(X_LOGIN_URL)

            while True:
                state = await wait_for_x_state(
                    page, deadline, ready=_ready_for_login_step(
                        steps, has_email=bool(email),
                    ),
                )

                if state is True:
                    return

                if state == "verify":
                    raise LoginRequiredError(LOGIN_VERIFICATION_MESSAGE)

                if state == "email_code" and state not in steps:
                    deadline.reschedule(
                        loop.time()
                        + EMAIL_CODE_TIMEOUT_SECONDS + SIGN_IN_TIMEOUT_SECONDS,
                    )
                    code = await _read_email_code(imap, since)
                    steps["email_code"] = deque([
                        _LoginField(EMAIL_CODE_FIELDS, code, (
                            "next", "continue", "verify", "submit",
                        )),
                    ])

                if isinstance(state, str):
                    await _advance_login_step(page, state, steps)
    except TimeoutError:
        raise LoginRequiredError(
            "X did not confirm automatic sign-in. Run make x-login "
            "(Docker) or make x-login MODE=native to inspect the prompt.",
        ) from None


def _ready_for_login_step(
    steps: dict[str, deque[_LoginField]], *, has_email: bool,
) -> str:
    signals: list[bool | str] = [True, "limited", "app_verification", "verify"]

    if "email_code" not in steps:
        signals.append("email_code")

    if not has_email:
        signals.append("email_address")

    fields = {
        stage: (queue[0].selector, queue[0].buttons)
        for stage, queue in steps.items() if queue
    }

    return LOGIN_STEP_READY.replace("SIGNALS", json.dumps(signals)).replace(
        "WAITING_FIELDS", json.dumps(fields),
    )


async def _advance_login_step(
    page: zendriver.Tab, stage: str, steps: dict[str, deque[_LoginField]],
) -> None:
    pending = steps.get(stage)

    if pending and await _submit_step(page, stage, pending[0]):
        pending.popleft()
    elif stage == "email_address" and not pending:
        raise LoginRequiredError(LOGIN_EMAIL_ADDRESS_MESSAGE)


async def _read_email_code(
    imap: EmailSettings | None, since: datetime.datetime,
) -> str:
    if imap is None:
        raise LoginRequiredError(LOGIN_NO_IMAP_MESSAGE)

    logger.info("X requires email verification. Checking the IMAP inbox.")

    try:
        code = await wait_for_code(imap, since)
    except EmailVerificationError as error:
        raise LoginRequiredError(
            f"{error} {MANUAL_VERIFICATION}",
        ) from None

    if code is None:
        raise LoginRequiredError(
            "No recent X verification email arrived. "
            f"{MANUAL_VERIFICATION}",
        )

    return code


async def _submit_step(
    page: zendriver.Tab, stage: str, step: _LoginField,
) -> bool:
    expected = json.dumps(stage)
    field_state = (
        f"input => ({SESSION_STATE}) === {expected} "
        f"? ({FIELD_STATE})(input) : null"
    )

    try:
        for field in await page.query_selector_all(step.selector):
            state = await field.apply(field_state)

            if state == "empty":
                await _type_login_value(field, step)
            elif state != "occupied":
                continue

            try:
                return await field.apply(
                    SUBMIT_FIELD.replace("STAGE", expected).replace(
                        "LABELS", json.dumps(step.buttons),
                    ),
                ) is True
            except (ConnectionError, ProtocolException):
                # Navigation can discard the result after the click dispatched.
                return True
    except (ConnectionError, ProtocolException):
        return False

    return False


async def _type_login_value(
    field: zendriver.Element, step: _LoginField,
) -> None:
    try:
        if step.selector == USERNAME_SELECTOR:
            await field.scroll_into_view()
            await field.mouse_click()

        # Let each native input event settle before typing the next character.
        for char in step.value:
            await asyncio.sleep(LOGIN_KEY_PAUSE_SECONDS)
            await field.send_keys(char)

        await asyncio.sleep(LOGIN_KEY_PAUSE_SECONDS)
    except Exception:
        # Driver errors can include the input payload.
        raise LoginRequiredError(
            "Could not fill the X sign-in form. Run make x-login.",
        ) from None
