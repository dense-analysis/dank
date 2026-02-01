"""
IMAP email handling for reading OTP codes from emails.

NOTE: This code is untested and needs testing in the real world when
an OTP code arrives at last.
"""
from __future__ import annotations

import asyncio
import datetime
import email
import imaplib
import re
import time
from email.header import decode_header
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from typing import NamedTuple, cast

from dank.config import EmailSettings


class EmailSearchFilters(NamedTuple):
    domain: str
    since_epoch: float


async def wait_for_code(
    settings: EmailSettings,
    filters: EmailSearchFilters,
    *,
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 5.0,
) -> str | None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        code = await asyncio.to_thread(
            _fetch_latest_code,
            settings,
            filters,
        )

        if code:
            return code

        await asyncio.sleep(poll_interval_seconds)

    return None


def _fetch_latest_code(
    settings: EmailSettings,
    filters: EmailSearchFilters,
) -> str | None:
    with imaplib.IMAP4_SSL(settings.host, settings.port) as client:
        client.login(settings.username, settings.password)
        client.select("INBOX", readonly=True)
        since_date = datetime.datetime.fromtimestamp(
            filters.since_epoch,
            tz=datetime.UTC,
        )
        search_parts = [
            "SINCE",
            since_date.strftime("%d-%b-%Y"),
            "FROM",
            filters.domain,
        ]
        typ, data = client.search(None, *search_parts)

        if typ != "OK" or not data:
            return None

        message_ids = data[0].split()

        for message_id in reversed(message_ids):
            typ, msg_data = client.fetch(message_id, "(RFC822)")

            match msg_data:
                case (_, bytes() as raw):
                    pass
                case _:
                    raw = None
                    pass

            if typ != "OK" or raw is None:
                continue

            message = email.message_from_bytes(raw)

            if not _is_recent(message, filters.since_epoch):
                continue

            if not _matches_domain(message, filters.domain):
                continue

            code = _extract_code(message)

            if code:
                return code

    return None


def _is_recent(message: Message, since_epoch: float) -> bool:
    sent_at = _parse_message_date(message)

    if sent_at is None:
        return True

    return sent_at.timestamp() >= since_epoch


def _parse_message_date(message: Message) -> datetime.datetime | None:
    date_value = message.get("Date")

    if not date_value:
        return None

    try:
        parsed = parsedate_to_datetime(date_value)
    except (TypeError, ValueError):
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)

    return parsed.astimezone(datetime.UTC)


def _matches_domain(message: Message, domain: str) -> bool:
    addresses = getaddresses(message.get_all("From", []))

    for _, addr in addresses:
        if addr.lower().endswith("@" + domain.lower()):
            return True

    return False


def _extract_code(message: Message) -> str | None:
    subject = _decode_subject(message)
    code = _extract_code_from_text(subject)

    if code:
        return code

    body = _get_text_body(message)

    return _extract_code_from_text(body)


def _decode_subject(message: Message) -> str:
    subject = message.get("Subject", "")
    decoded: list[str] = []

    for part, encoding in decode_header(subject):
        if isinstance(part, bytes):
            decoded.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            decoded.append(part)

    return "".join(decoded)


def _get_text_body(message: Message) -> str:
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_type() == "text/plain":
                payload = cast(bytes | None, part.get_payload(decode=True))

                if not payload:
                    continue

                charset = part.get_content_charset() or "utf-8"

                return payload.decode(charset, errors="replace")

    payload = cast(bytes | None, message.get_payload(decode=True))

    if payload:
        return payload.decode("utf-8", errors="replace")

    return ""


def _extract_code_from_text(text: str) -> str | None:
    if not text:
        return None

    match = re.search(
        r"(?:confirmation|verification) code(?: is|:)?\s*([A-Za-z0-9]{6,12})",
        text,
        re.IGNORECASE,
    )

    if match:
        return match.group(1)

    for line in text.splitlines():
        candidate = line.strip()

        if not candidate:
            continue

        if re.fullmatch(
            r"(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9]{6,12}",
            candidate,
        ):
            return candidate

    return None
