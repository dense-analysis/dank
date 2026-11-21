from __future__ import annotations

import asyncio
import json
import math
from typing import Literal, cast

import zendriver
from zendriver.core.connection import ProtocolException

from .onepassword import (
    FIELD_STATE,
    PASSWORD_SELECTOR,
    USERNAME_SELECTOR,
    VISIBLE_ELEMENT,
    LoginCredentials,
    fill_field,
)

X_HOME_URL = "https://x.com/home"
X_LOGIN_URL = "https://x.com/i/flow/login"
SESSION_CHECK_TIMEOUT_SECONDS = 20
LOGIN_LIMITED_MESSAGE = (
    "X has temporarily limited sign-in. Stop retrying and wait for X's "
    "restriction to clear before running x-login again."
)
LOGIN_REQUIRED_MESSAGE = (
    "X sign-in required. Run make x-login (Docker) or "
    "make x-login MODE=native, then retry collection."
)
MANUAL_VERIFICATION = (
    "Complete verification with make x-login (Docker) "
    "or make x-login MODE=native."
)
LOGIN_VERIFICATION_MESSAGE = (
    "X requires verification beyond an emailed code. " + MANUAL_VERIFICATION
)
LOGIN_NO_IMAP_MESSAGE = (
    "X requires email verification. IMAP credentials are missing. "
    + MANUAL_VERIFICATION
)
LOGIN_EMAIL_ADDRESS_MESSAGE = (
    "X asks for the account email address. Set x.email. " + MANUAL_VERIFICATION
)
LOGIN_APP_VERIFICATION_MESSAGE = (
    "X requires human verification. "
    "Open the X app on your phone and complete verification there."
)

LOGIN_ROUTE = """path => ['/i/flow/login', '/i/jf/onboarding/web', '/login']
    .some(route => path === route || path.startsWith(route + '/'))"""
VERIFICATION_FIELDS = (
    'input[autocomplete~="one-time-code"], input[name="challenge_response"], '
    'input[name="verification_code"], input[name="code"], '
    'input[inputmode="numeric"]'
)
EMAIL_ADDRESS_FIELDS = (
    'input[data-testid="ocfEnterTextTextInput"], '
    'input[autocomplete~="email"], input[type="email"]'
)
EMAIL_CODE_FIELDS = VERIFICATION_FIELDS + (
    ', input[data-testid="ocfEnterTextTextInput"]'
)
type SessionState = bool | Literal[
    "login", "email_address", "email_code", "verify",
]

SESSION_STATE = r"""(() => {
    if (location.origin !== 'https://x.com') return false;
    const text = document.body?.innerText.toLowerCase().replace(/\s+/g, ' ')
        || '';
    if (text.includes('temporarily limited your login')) return 'limited';
    const path = location.pathname;
    const visible = (""" + VISIBLE_ELEMENT + """);
    const has = fields =>
        Array.from(document.querySelectorAll(fields)).some(visible);
    const loginRoute = (""" + LOGIN_ROUTE + r""")(path);
    const verificationRoute = ['/i/flow/', '/i/jf/onboarding/',
        '/account/access'].some(route => path.startsWith(route));
    if (verificationRoute &&
        text.includes('performing security verification')) {
        return 'app_verification';
    }
    if (loginRoute || verificationRoute || has(
        'input[name="username_or_email"], a[href="/i/flow/login"], ' +
        'a[href="/i/flow/signup"]')) {
        const emailCode = /\bcheck your (?:e-?mail|inbox)\b/.test(text) ||
            /\be-?mailed you (?:a |the )?code\b/.test(text) ||
            /\bcode\b/.test(text) &&
            /\bsent\b.{0,80}(?:\be-?mail\b|\binbox\b|@[\w.*-]+)/.test(text);
        if (emailCode && has(EMAIL_CODE_FIELDS)) return 'email_code';
        const verification = /\b(?:verify|verification|confirm|liveness)\b/;
        const xApp = String.raw`\b(?:x|twitter) app\b`;
        const appAction =
            String.raw`\b(?:open|use|check|log in to|sign in to)\b`;
        const openApp = new RegExp(`${appAction}.{0,32}${xApp}`);
        const inApp = new RegExp(`(?:in|with) (?:the |your )?${xApp}`);
        const requiresApp = openApp.test(text) || inApp.test(text);
        if (verification.test(text) && requiresApp) {
            return 'app_verification';
        }
        if (has(VERIFICATION_FIELDS)) return 'verify';
        const addressPrompt =
            /\b(?:enter|confirm|verify)\b.{0,45}\be-?mail address\b/;
        const emailAddress = addressPrompt.test(text);
        if (emailAddress && has(EMAIL_ADDRESS_FIELDS)) return 'email_address';
        if (!loginRoute && verificationRoute) {
            // X can change the route before rendering the verification form.
            return has('form input, button, [role="button"], iframe')
                ? 'verify' : false;
        }
        return 'login';
    }
    return Boolean(document.querySelector(
        '[data-testid=SideNav_AccountSwitcher_Button]') &&
        document.querySelector('[data-testid=AppTabBar_Home_Link]'));
})()""".replace("EMAIL_CODE_FIELDS", json.dumps(EMAIL_CODE_FIELDS)).replace(
    "EMAIL_ADDRESS_FIELDS", json.dumps(EMAIL_ADDRESS_FIELDS),
).replace("VERIFICATION_FIELDS", json.dumps(VERIFICATION_FIELDS))

# https://dom.spec.whatwg.org/#mutation-observers
WAIT_FOR_SESSION_STATE = """(ready, timeout) => new Promise(resolve => {
    const events = ['load', 'input', 'change', 'transitionend', 'resize',
                    'popstate'];
    const check = () => {
        const state = (SESSION_STATE);
        if (ready(state)) finish(state);
    };
    const observer = new MutationObserver(check);
    const timer = setTimeout(() => finish(null), timeout);
    function finish(state) {
        clearTimeout(timer);
        observer.disconnect();
        for (const event of events) {
            window.removeEventListener(event, check, true);
        }
        resolve(state);
    }
    observer.observe(document, {subtree: true, childList: true,
        attributes: true, characterData: true});
    for (const event of events) window.addEventListener(event, check, true);
    check();
})""".replace("SESSION_STATE", SESSION_STATE)
SESSION_READY = "state => Boolean(state)"


class LoginRequiredError(RuntimeError):
    pass


class AppVerificationRequiredError(LoginRequiredError):
    def __init__(self) -> None:
        super().__init__(LOGIN_APP_VERIFICATION_MESSAGE)


class SignInPromptError(LoginRequiredError):
    def __init__(
        self, stage: Literal["login", "email_code"] = "login",
    ) -> None:
        self.stage = stage
        super().__init__(
            "X requires email verification. " + MANUAL_VERIFICATION
            if stage == "email_code" else LOGIN_REQUIRED_MESSAGE,
        )


async def read_session_state(page: zendriver.Tab) -> SessionState:
    """Distinguish a signed-in page, a login page and a page still loading."""
    try:
        state = await page.evaluate(SESSION_STATE)
    except (ProtocolException, ConnectionError):
        # Navigation can briefly destroy the execution context.
        return False

    return _session_state(state)


async def wait_for_x_state(
    page: zendriver.Tab, deadline: asyncio.Timeout, *,
    ready: str = SESSION_READY,
) -> SessionState:
    """Wait for a usable X form or status in the current browser document."""
    expires = deadline.when()
    assert expires is not None
    loop = asyncio.get_running_loop()

    while True:
        if page.browser and page.browser.stopped is True:
            raise LoginRequiredError("Browser closed during X sign-in.")

        remaining_ms = math.ceil(1000 * (expires - loop.time()))

        if remaining_ms <= 0:
            raise TimeoutError

        try:
            result = await page.evaluate(
                f"({WAIT_FOR_SESSION_STATE})(({ready}), {remaining_ms})",
                await_promise=True,
            )
        except ProtocolException as error:
            if not any(fragment in str(error).lower() for fragment in (
                "context destroyed", "context was destroyed", "find context",
                "target navigated", "promise was collected",
            )):
                raise

            # Reattach to the new document if navigation replaced the observer.
            continue

        if result is None:
            raise TimeoutError

        return _session_state(result)


def _session_state(state: object) -> SessionState:
    if state == "limited":
        raise LoginRequiredError(LOGIN_LIMITED_MESSAGE)

    if state == "app_verification":
        raise AppVerificationRequiredError()

    if state in ("login", "email_address", "email_code", "verify"):
        return cast(SessionState, state)

    return state is True


async def is_signed_in(page: zendriver.Tab) -> bool:
    return await read_session_state(page) is True


async def check_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
) -> bool:
    try:
        async with asyncio.timeout(timeout_seconds) as deadline:
            return await wait_for_x_state(page, deadline) is True
    except TimeoutError:
        raise LoginRequiredError(
            "Could not confirm the saved X session. "
            "Check the connection and try again.",
        ) from None


async def wait_for_x_session(
    page: zendriver.Tab, *, timeout_seconds: float,
    credentials: LoginCredentials | None = None,
    stop_on_prompt: bool = False,
) -> None:
    fields = (
        [(USERNAME_SELECTOR, credentials.username),
         (PASSWORD_SELECTOR, credentials.password)] if credentials else []
    )

    try:
        async with asyncio.timeout(timeout_seconds) as deadline:
            while True:
                ready = (
                    SESSION_READY if stop_on_prompt
                    else _ready_for_autofill([
                        selector for selector, _ in fields
                    ])
                )
                state = await wait_for_x_state(page, deadline, ready=ready)

                if state is True:
                    return

                if stop_on_prompt:
                    raise_for_login_state(state=state)

                fields = [
                    (selector, value) for selector, value in fields
                    if not await fill_field(page, selector, value)
                ]
    except TimeoutError:
        raise LoginRequiredError(LOGIN_REQUIRED_MESSAGE) from None


def _ready_for_autofill(selectors: list[str]) -> str:
    terminal = json.dumps([True, "limited", "app_verification"])

    if not selectors:
        return f"state => {terminal}.includes(state)"

    fields = json.dumps(", ".join(selectors))

    return (
        f"state => {terminal}.includes(state) || "
        f"Array.from(document.querySelectorAll({fields}))"
        f".some(field => ({FIELD_STATE})(field) !== null)"
    )


def raise_for_login_state(*, state: SessionState) -> None:
    if state in ("login", "email_code"):
        raise SignInPromptError(stage=state)

    if state == "email_address":
        raise LoginRequiredError(LOGIN_EMAIL_ADDRESS_MESSAGE)

    if state == "verify":
        raise LoginRequiredError(LOGIN_VERIFICATION_MESSAGE)
