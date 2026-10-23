"""Google's standard login stages, with lazy 1Password credential reads."""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import sys
from typing import cast

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import YouTubeOnePasswordSettings
from dank.onepassword_login import OnePasswordLogin, OnePasswordLoginError

GOOGLE_ORIGIN = "https://accounts.google.com"
YOUTUBE_SIGN_IN_URL = "https://www.youtube.com/signin?next=%2F&hl=en-GB"
OP_TIMEOUT_SECONDS = 120
MAX_CREDENTIAL_BYTES = 8192
MAX_REPLY_BYTES = MAX_CREDENTIAL_BYTES * 6 + 100
LOGIN_POLL_SECONDS = 0.5
UNKNOWN_STAGE_SECONDS = 8.0
FIELD_SELECTORS = {
    "username": 'input[type="email"], input[name="identifier"]',
    "password": 'input[type="password"][name="Passwd"]',
    "otp": 'input[name="totpPin"], input#totpPin',
}
NEXT_SELECTORS = {
    "username": "#identifierNext", "password": "#passwordNext",
    "otp": "#totpNext",
}
VISIBLE_JS = """element => {
    if (!element || element.disabled) return false;
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0 &&
        getComputedStyle(element).visibility === 'visible';
}"""
ACTIONS = {
    "authenticator": r"(?:verification code from|use).*authenticator|"
                     r"^Google Authenticator$|^Authenticator app$",
    "another_account": r"^Use another account$",
    "another_way": r"^Try another way$",
}
CHOICES_JS = (
    "document.querySelectorAll('button,[role=button],[role=link],a,li')"
)
STATE_SCRIPT = """(() => {
    if (location.origin === 'https://www.youtube.com' &&
        window.ytcfg?.get('LOGGED_IN') === true) return 'signed_in';
    if (location.origin !== 'https://accounts.google.com') return 'waiting';
    const visible = VISIBLE;
    if (Array.from(document.querySelectorAll(
        'iframe[src*="recaptcha"],input[name="ca"],input[name="captcha"]'
    )).some(visible)) return 'captcha';
    if (Array.from(document.querySelectorAll('input[aria-invalid=true]'))
        .some(visible)) return 'rejected';
    for (const [stage, selector] of Object.entries(SELECTORS)) {
        if (Array.from(document.querySelectorAll(selector)).some(visible))
            return stage;
    }
    for (const [action, pattern] of Object.entries(ACTIONS)) {
        if (Array.from(CHOICES).some(element => visible(element) &&
            new RegExp(pattern, 'i').test(element.textContent.trim())))
            return action;
    }
    return 'unknown';
})()""".replace("VISIBLE", VISIBLE_JS).replace(
    "SELECTORS", json.dumps(FIELD_SELECTORS),
).replace("ACTIONS", json.dumps(ACTIONS)).replace("CHOICES", CHOICES_JS)


class GoogleLoginError(RuntimeError):
    """A safe, actionable explanation without provider or page payloads."""


class GoogleCredentialProvider:
    def __init__(
        self, settings: YouTubeOnePasswordSettings, *, stdio: bool = False,
    ) -> None:
        self.settings = settings
        self.stdio = stdio

        try:
            self._login = OnePasswordLogin(settings.account, {
                field: str(getattr(settings, field))
                for field in FIELD_SELECTORS
            })
        except OnePasswordLoginError as error:
            raise GoogleLoginError(str(error)) from None

        self._native_lock = asyncio.Lock()
        self._native_attempted = False
        self._reader: asyncio.StreamReader | None = None
        self._transport: asyncio.ReadTransport | None = None

    async def read(self, field: str) -> str:
        if field not in FIELD_SELECTORS:
            raise GoogleLoginError("Unknown Google sign-in credential field.")

        reference = str(getattr(self.settings, field))

        if not reference:
            raise GoogleLoginError(
                "Google requires an authenticator code. Configure "
                "youtube.onepassword.otp or use --manual.",
            )

        if self.stdio:
            value = await self._read_stdio(field)
        else:
            try:
                async with self._native_lock:
                    if not self._login.loaded:
                        await self._load_native()

                    value = self._login.read(field)
            except OnePasswordLoginError as error:
                raise GoogleLoginError(str(error)) from None

        if (not value or len(value.encode()) > MAX_CREDENTIAL_BYTES
                or any(ord(char) < 32 or ord(char) == 127 for char in value)
                or (field == "otp" and not re.fullmatch(
                    r"[0-9]{6,8}", value,
                ))):
            raise GoogleLoginError(
                "1Password returned an invalid login field.",
            )

        return value

    async def _load_native(self) -> None:
        if self._native_attempted:
            raise GoogleLoginError(
                "1Password access already failed for this login attempt.",
            )

        self._native_attempted = True
        executable = shutil.which("op")

        if executable is None:
            raise GoogleLoginError("Install 1Password CLI or use --manual.")

        process = await asyncio.create_subprocess_exec(
            *self._login.command(executable),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )

        try:
            async with asyncio.timeout(OP_TIMEOUT_SECONDS):
                stdout, _stderr = await process.communicate()
        except TimeoutError:
            raise GoogleLoginError("1Password access timed out.") from None
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

        if process.returncode != 0:
            raise GoogleLoginError(
                "1Password access failed. Unlock 1Password.",
            )

        self._login.load_payload(stdout)
        stdout = b""

    async def _read_stdio(self, field: str) -> str:
        if self._reader is None:
            self._reader = asyncio.StreamReader(limit=MAX_REPLY_BYTES)
            protocol = asyncio.StreamReaderProtocol(self._reader)
            transport, _ = await asyncio.get_running_loop().connect_read_pipe(
                lambda: protocol, sys.stdin.buffer,
            )
            self._transport = transport

        print("DANK_YOUTUBE_CREDENTIAL:" + field, flush=True)

        try:
            async with asyncio.timeout(OP_TIMEOUT_SECONDS):
                line = await self._reader.readline()

            response = json.loads(line)
        except (TimeoutError, ValueError, UnicodeError):
            raise GoogleLoginError(
                "Could not receive 1Password credentials.",
            ) from None

        if not isinstance(response, dict):
            raise GoogleLoginError("1Password credential request failed.")

        data = cast(dict[str, object], response)

        if data.get("error") or not isinstance(data.get("value"), str):
            raise GoogleLoginError("1Password credential request failed.")

        return cast(str, data["value"])

    def close(self) -> None:
        self._login.close()

        if self._transport is not None:
            self._transport.close()
            self._transport = None


async def _stage(page: zendriver.Tab) -> str:
    try:
        return str(await page.evaluate(STATE_SCRIPT))
    except (ProtocolException, ConnectionError):
        return "waiting"


async def _fill_and_submit(
    page: zendriver.Tab, field: str, provider: GoogleCredentialProvider,
) -> bool:
    # Check before reading the vault; check again atomically with insertion.
    if await _stage(page) != field:
        return False

    value = await provider.read(field)
    script = """(() => {
        if (location.origin !== 'https://accounts.google.com') return false;
        const visible = VISIBLE;
        const input = Array.from(document.querySelectorAll(SELECTOR))
            .find(visible);
        const next = document.querySelector(NEXT);
        if (!input || input.readOnly || input.value || !visible(next))
            return false;
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')
            .set.call(input, VALUE);
        input.dispatchEvent(new Event('input', {bubbles: true}));
        input.dispatchEvent(new Event('change', {bubbles: true}));
        if (location.origin !== 'https://accounts.google.com') return false;
        next.click();
        return true;
    })()""".replace("VISIBLE", VISIBLE_JS).replace(
        "SELECTOR", json.dumps(FIELD_SELECTORS[field]),
    ).replace("NEXT", json.dumps(NEXT_SELECTORS[field])).replace(
        "VALUE", json.dumps(value),
    )

    try:
        return await page.evaluate(script) is True
    except Exception:
        # Browser protocol failures can embed the command and its secret.
        raise GoogleLoginError(
            "Google sign-in could not submit a field. Retry with --manual.",
        ) from None
    finally:
        value = script = ""


async def _choose(page: zendriver.Tab, action: str) -> bool:
    script = """(() => {
        if (location.origin !== 'https://accounts.google.com') return false;
        const visible = VISIBLE;
        const matches = Array.from(CHOICES).filter(element =>
            visible(element) &&
            new RegExp(PATTERN, 'i').test(element.textContent.trim()));
        // Google nests the actionable link inside a matching list item.
        const choice = matches.find(element => !matches.some(other =>
            other !== element && element.contains(other)));
        if (!choice) return false;
        choice.click();
        return true;
    })()""".replace("VISIBLE", VISIBLE_JS).replace(
        "CHOICES", CHOICES_JS,
    ).replace("PATTERN", json.dumps(ACTIONS[action]))

    try:
        return await page.evaluate(script) is True
    except (ProtocolException, ConnectionError):
        return False


async def _advance(
    page: zendriver.Tab, stage: str, completed: set[str],
    provider: GoogleCredentialProvider,
) -> bool:
    if stage in {"captcha", "rejected"}:
        raise GoogleLoginError(
            "Google requires additional verification or rejected the login. "
            "Complete sign-in with --manual.",
        )

    if stage in completed:
        return False

    if stage in FIELD_SELECTORS:
        if not await _fill_and_submit(page, stage, provider):
            raise GoogleLoginError(
                "Google's login form changed. Use --manual.",
            )
    elif stage not in ACTIONS or not await _choose(page, stage):
        return False

    completed.add(stage)

    return True


async def google_sign_in(
    page: zendriver.Tab, provider: GoogleCredentialProvider,
    *, timeout_seconds: float,
) -> None:
    completed: set[str] = set()
    unknown_since: float | None = None
    loop = asyncio.get_running_loop()

    try:
        async with asyncio.timeout(timeout_seconds):
            await page.get(YOUTUBE_SIGN_IN_URL)

            while True:
                stage = await _stage(page)

                if stage == "signed_in":
                    return

                progressed = await _advance(page, stage, completed, provider)

                if progressed:
                    unknown_since = None
                elif stage in {"unknown", "waiting"} or stage in completed:
                    unknown_since = unknown_since or loop.time()

                    if loop.time() - unknown_since >= UNKNOWN_STAGE_SECONDS:
                        raise GoogleLoginError(
                            "Google requires a device confirmation or an "
                            "unsupported login step. Use --manual.",
                        )
                else:
                    unknown_since = None

                await asyncio.sleep(LOGIN_POLL_SECONDS)
    except TimeoutError:
        raise GoogleLoginError("Google sign-in timed out. Use --manual.") \
            from None
    except GoogleLoginError:
        raise
    except Exception:
        raise GoogleLoginError(
            "Google sign-in failed. Retry with --manual.",
        ) from None
    finally:
        provider.close()
