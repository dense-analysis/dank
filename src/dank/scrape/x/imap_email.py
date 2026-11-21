from __future__ import annotations

import asyncio
import datetime
import html
import imaplib
import re
import ssl
from email.errors import MessageError
from email.message import EmailMessage
from email.parser import BytesParser
from email.policy import default
from email.utils import getaddresses, parsedate_to_datetime

from dank.config import EmailSettings

EMAIL_CODE_TIMEOUT_SECONDS = 120
EMAIL_POLL_SECONDS = 5
IMAP_SOCKET_TIMEOUT_SECONDS = 10
MAX_RECENT_MESSAGES = 12
# https://help.x.com/en/safety-and-security/fake-x-emails
X_SENDER_DOMAIN = "x.com"
CODE_LABEL = (
    r"(?:confirmation|verification|security|sign[- ]in|login|one-time) code"
)
CODE_TOKEN = r"[A-Za-z0-9]{6,12}"
LABELLED_CODE = re.compile(
    rf"\b{CODE_LABEL}\b(?:\s+is\b)?(?:\s*:\s*|\s+)({CODE_TOKEN})\b"
    rf"|\b({CODE_TOKEN})\s+is your (?:X )?{CODE_LABEL}\b",
    re.IGNORECASE,
)


class EmailVerificationError(RuntimeError):
    pass


async def wait_for_code(
    settings: EmailSettings, since: datetime.datetime,
) -> str | None:
    try:
        async with asyncio.timeout(EMAIL_CODE_TIMEOUT_SECONDS):
            while True:
                code = await asyncio.to_thread(
                    _fetch_latest_code, settings, since,
                )

                if code is not None:
                    return code

                await asyncio.sleep(EMAIL_POLL_SECONDS)
    except TimeoutError:
        return None


def _fetch_latest_code(
    settings: EmailSettings, since: datetime.datetime,
) -> str | None:
    try:
        with imaplib.IMAP4_SSL(
            settings.host, settings.port,
            ssl_context=ssl.create_default_context(),
            timeout=IMAP_SOCKET_TIMEOUT_SECONDS,
        ) as client:
            client.login(settings.username, settings.password)
            status, _ = client.select("INBOX", readonly=True)

            if status != "OK":
                raise EmailVerificationError("Could not open the IMAP inbox.")

            # IMAP SINCE uses calendar dates. Allow for the server's time zone
            # and compare each candidate's full timestamp against this attempt.
            date = imaplib.Time2Internaldate(
                since - datetime.timedelta(days=1),
            )
            day = date.split(maxsplit=1)[0].lstrip('"')
            status, data = client.uid(
                "SEARCH", "SINCE", day, "FROM", X_SENDER_DOMAIN,
            )

            if status != "OK":
                raise EmailVerificationError(
                    "Could not search the IMAP inbox.",
                )

            ids = data[0] if data else None

            if not isinstance(ids, bytes):
                return None

            for uid in reversed(ids.split()[-MAX_RECENT_MESSAGES:]):
                status, result = client.uid(
                    "FETCH", uid.decode("ascii"), "(BODY.PEEK[])",
                )

                if status != "OK":
                    continue

                for item in result:
                    if isinstance(item, tuple) and isinstance(item[1], bytes):
                        if code := code_from_message(item[1], since):
                            return code
    except (imaplib.IMAP4.error, MessageError, OSError, UnicodeError):
        raise EmailVerificationError(
            "Could not read the IMAP inbox.",
        ) from None

    return None


def code_from_message(raw: bytes, since: datetime.datetime) -> str | None:
    message = BytesParser(_class=EmailMessage, policy=default).parsebytes(raw)

    if not _is_recent_x_message(message, since):
        return None

    subject = str(message.get("Subject", ""))

    if re.search(r"\b(?:password reset|reset your password)\b", subject, re.I):
        return None

    part = message.get_body(("plain", "html"))

    try:
        body = part.get_content(errors="replace") if part is not None else ""
    except LookupError:
        body = ""

    if not isinstance(body, str):
        body = ""

    if part is not None and part.get_content_type() == "text/html":
        body = html.unescape(re.sub(r"<[^>]+>", "\n", body))

    return _extract_code(subject, body)


def _is_recent_x_message(
    message: EmailMessage, since: datetime.datetime,
) -> bool:
    senders = getaddresses(message.get_all("From", []))

    if len(senders) != 1:
        return False

    _, address = senders[0]

    if address.lower().partition("@")[2] != X_SENDER_DOMAIN:
        return False

    try:
        sent = parsedate_to_datetime(str(message.get("Date", "")))
    except (ValueError, OverflowError):
        return False

    if sent.tzinfo is None:
        sent = sent.replace(tzinfo=datetime.UTC)

    return sent >= since


def _extract_code(subject: str, body: str) -> str | None:
    text = f"{subject}\n{body}"

    for match in LABELLED_CODE.finditer(text):
        candidate = match[1] or match[2]

        if _looks_like_code(candidate):
            return candidate

    if re.search(rf"\b{CODE_LABEL}\b", text, re.IGNORECASE):
        for line in body.splitlines():
            candidate = line.strip()

            if re.fullmatch(CODE_TOKEN, candidate) and any(
                char.isdigit() for char in candidate
            ):
                return candidate

    return None


def _looks_like_code(candidate: str) -> bool:
    return candidate.isupper() or any(char.isdigit() for char in candidate)
