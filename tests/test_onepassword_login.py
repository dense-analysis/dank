from __future__ import annotations

import base64
import json
import subprocess
from typing import Any
from unittest.mock import Mock

import pytest

from dank import onepassword_login as login

REFERENCES = {
    "username": "op://vault-id/item-id/username",
    "password": "op://vault-id/item-id/password",
    "otp": "op://vault-id/item-id/otp?attribute=otp",
}
SEED = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
OTP_URI = "otpauth://totp/Example:test?secret=" + SEED


def item_payload() -> dict[str, Any]:
    return {
        "id": "item-id", "title": "Test account", "category": "LOGIN",
        "vault": {"id": "vault-id", "name": "Personal"},
        "fields": [
            {"id": "username", "label": "username", "type": "STRING",
             "purpose": "USERNAME", "value": "person@example.test"},
            {"id": "password", "label": "password", "type": "CONCEALED",
             "purpose": "PASSWORD", "value": "private 'password' $shell"},
            {"id": "otp", "label": "one-time password", "type": "OTP",
             "value": OTP_URI},
            {"id": "notesPlain", "type": "STRING",
             "value": "unrelated private notes"},
        ],
    }


@pytest.mark.parametrize("timestamp,expected", [
    (59, ("94287082", "46119246", "90693936")),
    (1111111109, ("07081804", "68084774", "25091201")),
    (1111111111, ("14050471", "67062674", "99943326")),
    (1234567890, ("89005924", "91819424", "93441116")),
    (2000000000, ("69279037", "90698825", "38618901")),
    (20000000000, ("65353130", "77737706", "47863826")),
])
@pytest.mark.parametrize("index,algorithm,seed", [
    (0, "SHA1", b"12345678901234567890"),
    (1, "SHA256", b"12345678901234567890123456789012"),
    (2, "SHA512", b"123456789012345678901234567890123456789012345678901234"
                   b"5678901234"),
])
def test_rfc_6238_appendix_b_vectors(
    timestamp: int, expected: tuple[str, ...], index: int,
    algorithm: str, seed: bytes,
) -> None:
    encoded = base64.b32encode(seed).decode().rstrip("=")
    uri = (f"otpauth://totp/Test?secret={encoded}&algorithm={algorithm}"
           "&digits=8&period=30")
    assert login.totp(uri, at=timestamp) == expected[index]


def test_totp_respects_custom_period_digits_and_default_algorithm() -> None:
    assert login.totp(OTP_URI, at=59) == "287082"
    assert login.totp(OTP_URI + "&period=60&digits=8", at=118) == "94287082"


@pytest.mark.parametrize("uri", [
    "otpauth://hotp/Test?secret=" + SEED,
    "otpauth://totp/Test?secret=private-invalid-seed",
    OTP_URI + "&period=0", OTP_URI + "&digits=12",
    OTP_URI + "&algorithm=MD5", OTP_URI + "&secret=duplicate",
    "otpauth://totp/Test", "private-literal-secret",
])
def test_invalid_totp_errors_do_not_include_the_secret(uri: str) -> None:
    with pytest.raises(login.OnePasswordLoginError) as failure:
        login.totp(uri)

    assert uri not in str(failure.value)
    assert SEED not in str(failure.value)


def test_single_item_read_supplies_all_fields_and_fresh_codes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    execute = Mock(return_value=subprocess.CompletedProcess(
        [], 0, json.dumps(item_payload()).encode(), b"",
    ))
    clock = Mock(return_value=59)
    monkeypatch.setattr(login.shutil, "which", Mock(
        return_value="/fixture/op",
    ))
    monkeypatch.setattr(login.subprocess, "run", execute)
    monkeypatch.setattr(login.time, "time", clock)
    session = login.OnePasswordLogin("account-id", REFERENCES)
    assert not session.loaded
    execute.assert_not_called()
    assert session.read("username") == "person@example.test"
    assert session.read("password") == "private 'password' $shell"
    assert session.read("otp") == "287082"
    clock.return_value = 1111111109
    assert session.read("otp") == "081804"
    execute.assert_called_once_with(
        ["/fixture/op", "item", "get", "--account", "account-id", "--vault",
         "vault-id", "item-id", "--format", "json"],
        capture_output=True, timeout=120, check=False,
    )
    assert repr(session) == "OnePasswordLogin(<redacted>)"
    assert capsys.readouterr().out == ""
    assert session.loaded
    session.close()
    assert not session.loaded

    with pytest.raises(login.OnePasswordLoginError):
        session.read("password")

    assert execute.call_count == 1


def test_field_ids_labels_and_sections_select_only_requested_fields() -> None:
    payload = item_payload()
    payload["fields"].extend([
        {"id": "password-alt", "label": "password", "value": "wrong-secret",
         "section": {"id": "other-section", "label": "Other"}},
        {"id": "otp-section", "label": "authenticator", "type": "OTP",
         "section": {"id": "login-section", "label": "Sign in"},
         "value": OTP_URI},
    ])
    session = login.OnePasswordLogin("account-id", {
        **REFERENCES,
        "otp": "op://vault-id/item-id/Sign in/authenticator?attribute=otp",
    })
    session.load_payload(json.dumps(payload))
    assert session.read("password") == "private 'password' $shell"
    values = session._values  # pyright: ignore[reportPrivateUsage]
    assert set(values) == {"username", "password", "otp"}
    assert "unrelated private notes" not in values.values()
    assert "wrong-secret" not in values.values()


@pytest.mark.parametrize("replacement", [
    "op://other-vault/item-id/password", "op://vault-id/other-item/password",
    "literal-secret", "op://vault-id/item-id/password?attribute=value",
])
def test_references_must_identify_one_item_before_vault_access(
    monkeypatch: pytest.MonkeyPatch, replacement: str,
) -> None:
    execute = Mock()
    monkeypatch.setattr(login.subprocess, "run", execute)

    with pytest.raises(login.OnePasswordLoginError) as failure:
        login.OnePasswordLogin("account-id", {
            **REFERENCES, "password": replacement,
        })

    execute.assert_not_called()
    assert replacement not in str(failure.value)


def test_ambiguous_labels_do_not_silently_choose_another_credential() -> None:
    payload = item_payload()
    payload["fields"].extend([
        {"id": "one", "label": "alternate", "value": "first-secret"},
        {"id": "two", "label": "alternate", "value": "second-secret"},
    ])
    session = login.OnePasswordLogin("account-id", {
        **REFERENCES, "password": "op://vault-id/item-id/alternate",
    })

    with pytest.raises(login.OnePasswordLoginError, match="ambiguous"):
        session.load_payload(json.dumps(payload))

    assert not session.loaded


def test_failed_item_access_is_not_retried_and_does_not_expose_cli_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    execute = Mock(return_value=subprocess.CompletedProcess(
        [], 1, b"partial-private-payload", b"private-account-error",
    ))
    monkeypatch.setattr(login.shutil, "which", Mock(
        return_value="/fixture/op",
    ))
    monkeypatch.setattr(login.subprocess, "run", execute)
    session = login.OnePasswordLogin("account-id", REFERENCES)

    for field in ("username", "password"):
        with pytest.raises(login.OnePasswordLoginError) as failure:
            session.read(field)

        assert "private" not in str(failure.value)

    assert execute.call_count == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("payload", [
    b"private-invalid-json", b"[]", b'{"fields":false}',
    b'{"fields":["private-string"]}', b'{"fields":[]}',
])
def test_invalid_item_payloads_are_redacted(payload: bytes) -> None:
    session = login.OnePasswordLogin("account-id", REFERENCES)

    with pytest.raises(login.OnePasswordLoginError) as failure:
        session.load_payload(payload)

    assert "private" not in str(failure.value)
    assert not session.loaded
