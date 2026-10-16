from __future__ import annotations

import asyncio
import logging
import shutil
from typing import BinaryIO, NamedTuple

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import XOnePasswordSettings

logger = logging.getLogger(__name__)
OP_TIMEOUT_SECONDS = 120
MAX_CREDENTIAL_BYTES = 8192
USERNAME_SELECTOR = (
    'input[name="username_or_email"], input[autocomplete~="username"]'
)
PASSWORD_SELECTOR = 'input[type="password"]'
FIELD_STATE = """element => {
    const rect = element.getBoundingClientRect();
    if (location.origin !== 'https://x.com' || element.disabled ||
        element.readOnly || !rect.width || !rect.height) return null;
    for (let node = element; node; node = node.parentElement) {
        const style = getComputedStyle(node);
        if (style.visibility !== 'visible' || Number(style.opacity) === 0)
            return null;
    }
    return element.value === '' ? 'empty' : 'occupied';
}"""


class OnePasswordError(RuntimeError):
    pass


class LoginCredentials(NamedTuple):
    username: str
    password: str

    def __repr__(self) -> str:
        return "LoginCredentials(<redacted>)"


def credentials_from_bytes(
    username: bytes, password: bytes,
) -> LoginCredentials:
    values: list[str] = []

    for value in (username, password):
        try:
            text = value.decode("utf-8")
        except UnicodeError:
            raise OnePasswordError("1Password returned invalid text") from None

        if (
            not text or len(value) > MAX_CREDENTIAL_BYTES
            or any(ord(char) < 32 or ord(char) == 127 for char in text)
        ):
            raise OnePasswordError(
                "1Password login fields must be non-empty, single-line text",
            )

        values.append(text)

    return LoginCredentials(*values)


def read_credentials(stream: BinaryIO) -> LoginCredentials:
    # NUL framing preserves spaces and shell metacharacters without escaping.
    data = stream.read(MAX_CREDENTIAL_BYTES * 2 + 3)
    parts = data.split(b"\0")

    if len(parts) != 3 or parts[-1]:
        raise OnePasswordError(
            "Could not receive complete 1Password credentials",
        )

    return credentials_from_bytes(parts[0], parts[1])


async def _read_reference(executable: str, reference: str) -> bytes:
    process = await asyncio.create_subprocess_exec(
        executable, "read", "--no-newline", reference,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    try:
        async with asyncio.timeout(OP_TIMEOUT_SECONDS):
            stdout, _stderr = await process.communicate()
    except TimeoutError:
        raise OnePasswordError("1Password access timed out") from None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    if process.returncode != 0:
        # CLI error output can contain sensitive item details.
        raise OnePasswordError("1Password access failed")

    return stdout


async def load_credentials(
    settings: XOnePasswordSettings | None,
) -> LoginCredentials | None:
    if settings is None:
        return None

    executable = shutil.which("op")

    if executable is None:
        logger.warning("1Password CLI is unavailable; continue manually.")
        return None

    try:
        username = await _read_reference(executable, settings.username)
        password = await _read_reference(executable, settings.password)
        return credentials_from_bytes(username, password)
    except (OnePasswordError, OSError):
        logger.warning("Could not read 1Password fields; continue manually.")
        return None


async def fill_field(page: zendriver.Tab, selector: str, value: str) -> bool:
    """Fill a visible, empty X field once; leave submission to the user."""
    try:
        for element in await page.query_selector_all(selector):
            state = await element.apply(FIELD_STATE)

            if state == "occupied":
                return True

            if state == "empty":
                try:
                    await element.send_keys(value)
                except Exception:
                    # Key-event errors can include the secret in their payload.
                    logger.warning(
                        "1Password could not fill a field; continue manually.",
                    )

                return True
    except (ProtocolException, ConnectionError):
        # A navigation may replace the DOM before the field is ready.
        return False

    return False
