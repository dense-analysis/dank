"""One in-memory 1Password item per login attempt; no credential files."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import shutil
import subprocess
import time
from collections.abc import Mapping
from typing import Any, NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

OP_TIMEOUT_SECONDS = 120
MAX_FIELD_BYTES = 8192
MAX_ITEM_BYTES = 2 * 1024 * 1024
CREDENTIAL_FIELDS = frozenset({"username", "password", "otp"})


class OnePasswordLoginError(RuntimeError):
    """Safe provider errors never include item payloads or CLI output."""


class FieldReference(NamedTuple):
    vault: str
    item: str
    section: str
    field: str


def _reference(value: str) -> FieldReference:
    path, _, query = value.partition("?")
    parts = path.removeprefix("op://").split("/")

    if (not value.startswith("op://") or len(parts) not in {3, 4}
            or not all(parts) or "#" in value
            or query not in {"", "attribute=otp", "attr=otp"}
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise OnePasswordLoginError("Invalid 1Password login reference.")

    return FieldReference(parts[0], parts[1],
                          parts[2] if len(parts) == 4 else "", parts[-1])


def _matches(data: Mapping[str, object], name: str) -> bool:
    return any(str(data.get(key, "")).casefold() == name.casefold()
               for key in ("id", "label"))


def _field_value(
    fields: list[dict[str, Any]], reference: FieldReference,
) -> str:
    matches: list[dict[str, Any]] = []

    for field in fields:
        section = field.get("section", {})

        if (reference.section and (
            not isinstance(section, dict)
            or not _matches(
                cast(dict[str, object], section), reference.section,
            )
        )):
            continue

        if _matches(field, reference.field):
            matches.append(field)

    # IDs are unambiguous even when another field has the same display label.
    ids = [
        field for field in matches
        if str(field.get("id", "")).casefold() == reference.field.casefold()
    ]
    matches = ids or matches

    if len(matches) != 1:
        raise OnePasswordLoginError(
            "A configured 1Password login field is missing or ambiguous.",
        )

    value = matches[0].get("value")

    if (not isinstance(value, str) or not value
            or len(value.encode()) > MAX_FIELD_BYTES
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise OnePasswordLoginError("1Password returned an invalid field.")

    return value


def totp(uri: str, *, at: float | None = None) -> str:
    """Generate the current code without rereading or exposing the seed."""
    try:
        parsed = urlsplit(uri)
        values = parse_qs(parsed.query, keep_blank_values=True,
                          max_num_fields=20)

        if (parsed.scheme != "otpauth" or parsed.netloc != "totp"
                or any(len(values.get(key, [])) > 1 for key in (
                    "secret", "algorithm", "period", "digits",
                ))):
            raise ValueError

        seed = values["secret"][0].replace(" ", "").rstrip("=")
        algorithm = values.get("algorithm", ["SHA1"])[0].upper()
        period = int(values.get("period", ["30"])[0])
        digits = int(values.get("digits", ["6"])[0])
        timestamp = time.time() if at is None else at

        if (algorithm not in {"SHA1", "SHA256", "SHA512"}
                or not 1 <= period <= 86400 or not 6 <= digits <= 8
                or timestamp < 0 or not seed):
            raise ValueError

        key = base64.b32decode(seed + "=" * (-len(seed) % 8), casefold=True)
        # RFC 6238: HOTP over an eight-byte time step.
        # https://www.rfc-editor.org/rfc/rfc6238#section-4
        counter = int(timestamp // period).to_bytes(8, "big")
        digest = hmac.new(key, counter, getattr(hashlib, algorithm.lower()))
        signature = digest.digest()
        offset = signature[-1] & 0x0F
        truncated = int.from_bytes(signature[offset:offset + 4], "big")
        number = truncated & 0x7FFFFFFF

        return str(number % 10 ** digits).zfill(digits)
    except (ValueError, KeyError, OverflowError, TypeError):
        raise OnePasswordLoginError(
            "The configured authenticator field is not a valid TOTP secret.",
        ) from None


class OnePasswordLogin:
    """Read one item lazily; retain only its configured login fields."""

    def __init__(self, account: str, references: Mapping[str, str]) -> None:
        if (not account.strip() or any(ord(char) < 32 or ord(char) == 127
                                      for char in account)
                or set(references) - CREDENTIAL_FIELDS
                or not all(references.get(key) for key in (
                    "username", "password",
                ))):
            raise OnePasswordLoginError("Invalid 1Password login references.")

        self.account = account
        self.references = {field: _reference(value)
                           for field, value in references.items() if value}
        targets = {(ref.vault.casefold(), ref.item.casefold())
                   for ref in self.references.values()}

        if len(targets) != 1:
            raise OnePasswordLoginError(
                "YouTube login fields must reference the same 1Password item.",
            )

        self._values: dict[str, str] = {}
        self._attempted = False
        self._closed = False

    def __repr__(self) -> str:
        return "OnePasswordLogin(<redacted>)"

    @property
    def loaded(self) -> bool:
        return bool(self._values)

    def command(self, executable: str = "op") -> list[str]:
        reference = self.references["username"]

        return [
            executable, "item", "get", "--account", self.account,
            "--vault", reference.vault, reference.item, "--format", "json",
        ]

    def load_payload(self, payload: bytes | str) -> None:
        if self._closed:
            raise OnePasswordLoginError(
                "The login credential session is closed.",
            )

        self._attempted = True

        try:
            if len(payload) > MAX_ITEM_BYTES:
                raise ValueError

            decoded = json.loads(payload)

            if not isinstance(decoded, dict):
                raise ValueError

            fields: Any = cast(dict[str, Any], decoded).get("fields")

            if not isinstance(fields, list) or not all(
                isinstance(field, dict) for field in cast(list[object], fields)
            ):
                raise ValueError

            selected = {
                field: _field_value(cast(list[dict[str, Any]], fields), ref)
                for field, ref in self.references.items()
            }

            if "otp" in selected:
                totp(selected["otp"])
        except (ValueError, UnicodeError):
            raise OnePasswordLoginError(
                "1Password returned an invalid login item.",
            ) from None

        self._values = selected

    def _load(self) -> None:
        if self._attempted:
            raise OnePasswordLoginError(
                "1Password access already failed for this login attempt.",
            )

        self._attempted = True
        executable = shutil.which("op")

        if executable is None:
            raise OnePasswordLoginError("Install 1Password CLI to sign in.")

        try:
            result = subprocess.run(
                self.command(executable), capture_output=True,
                timeout=OP_TIMEOUT_SECONDS, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise OnePasswordLoginError(
                "1Password access failed or timed out.",
            ) from None

        if result.returncode:
            raise OnePasswordLoginError(
                "1Password access was not authorized. Unlock 1Password.",
            )

        self.load_payload(result.stdout)

    def read(self, field: str) -> str:
        if self._closed or field not in self.references:
            raise OnePasswordLoginError(
                "The requested login field is unavailable.",
            )

        if not self.loaded:
            self._load()

        value = self._values[field]

        return totp(value) if field == "otp" else value

    def close(self) -> None:
        self._values.clear()
        self._closed = True
