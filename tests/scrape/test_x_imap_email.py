import datetime
import imaplib
from email.message import EmailMessage
from email.utils import format_datetime
from unittest.mock import MagicMock

import pytest

from dank.config import EmailSettings
from dank.scrape.x import imap_email

SINCE = datetime.datetime(2026, 11, 21, 20, tzinfo=datetime.UTC)
MAILBOX = EmailSettings("imap.test", "inbox@example.test", "fixture-secret")


def message_bytes(
    text: str = "Your X verification code is AB1234.", *,
    sender: str = "X <verify@x.com>",
    subject: str = "Your X verification code",
    sent_at: datetime.datetime | None = SINCE, subtype: str = "plain",
) -> bytes:
    message = EmailMessage()
    message["From"] = sender
    message["Subject"] = subject

    if sent_at is not None:
        message["Date"] = format_datetime(sent_at)

    message.set_content(text, subtype=subtype)

    return message.as_bytes()


@pytest.mark.parametrize(("body", "subject", "subtype"), [
    ("Your X verification code is AB1234.", "X", "plain"),
    ("AB1234\n", "Your X confirmation code", "plain"),
    ("Verification code: <strong>AB1234</strong>", "X", "html"),
    ("Enter it on X.", "AB1234 is your X confirmation code", "plain"),
])
def test_extracts_recent_verification_code(
    body: str, subject: str, subtype: str,
) -> None:
    raw = message_bytes(body, subject=subject, subtype=subtype)
    assert imap_email.code_from_message(raw, SINCE) == "AB1234"


@pytest.mark.parametrize("raw", [
    message_bytes(sender="X <verify@x.com.example.test>"),
    message_bytes(sender="x.com"),
    message_bytes(sent_at=SINCE - datetime.timedelta(seconds=1)),
    message_bytes(sent_at=None),
    message_bytes("Transaction ID\nAB1234", subject="Receipt"),
    message_bytes(subject="Reset your password"),
])
def test_skips_unrelated_and_old_messages(raw: bytes) -> None:
    assert imap_email.code_from_message(raw, SINCE) is None


async def test_reads_the_latest_matching_message_without_marking_it_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.select.return_value = ("OK", [b"2"])
    client.uid.side_effect = [
        ("OK", [b"42 43"]),
        ("OK", [(b"43 (BODY[] mock)", message_bytes(sender="wrong@test"))]),
        ("OK", [(b"42 (BODY[] mock)", message_bytes()), b")"]),
    ]
    connect = MagicMock()
    connect.return_value.__enter__.return_value = client
    monkeypatch.setattr(imap_email.imaplib, "IMAP4_SSL", connect)

    assert await imap_email.wait_for_code(MAILBOX, SINCE) == "AB1234"

    tls = connect.call_args.kwargs["ssl_context"]
    assert tls.check_hostname and tls.verify_mode
    client.select.assert_called_once_with("INBOX", readonly=True)
    assert [call.args for call in client.uid.call_args_list] == [
        ("SEARCH", "SINCE", "20-Nov-2026", "FROM", "x.com"),
        ("FETCH", "43", "(BODY.PEEK[])"),
        ("FETCH", "42", "(BODY.PEEK[])"),
    ]


async def test_imap_errors_omit_server_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = MagicMock(side_effect=imaplib.IMAP4.error(MAILBOX.password))
    monkeypatch.setattr(imap_email.imaplib, "IMAP4_SSL", connect)

    with pytest.raises(imap_email.EmailVerificationError) as info:
        await imap_email.wait_for_code(MAILBOX, SINCE)

    assert MAILBOX.password not in str(info.value)
