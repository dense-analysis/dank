"""Bridge host 1Password fields to Docker over private standard input."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

# The Docker launcher also runs directly with the host's standard Python.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dank.onepassword_login import (
    OnePasswordLogin,
    OnePasswordLoginError,
)

REQUEST_PREFIX = "DANK_YOUTUBE_CREDENTIAL:"
CREDENTIAL_FIELDS = frozenset({"username", "password", "otp"})


class CredentialAccessError(OnePasswordLoginError):
    pass


def parse_references(payload: str) -> dict[str, str]:
    try:
        parsed: Any = json.loads(payload)
    except ValueError:
        raise CredentialAccessError(
            "Could not read YouTube login references",
        ) from None

    if not isinstance(parsed, dict):
        raise CredentialAccessError("Could not read YouTube login references")

    data = cast(dict[str, object], parsed)

    if not data:
        return {}

    if set(data) != {"account", *CREDENTIAL_FIELDS}:
        raise CredentialAccessError("Invalid YouTube login reference fields")

    if not all(isinstance(value, str) for value in data.values()):
        raise CredentialAccessError("Invalid YouTube login references")

    values = cast(dict[str, str], data)

    if (not values["account"] or any(
        not values[field].startswith("op://")
        for field in ("username", "password")
    ) or (values["otp"] and not values["otp"].startswith("op://"))):
        raise CredentialAccessError("Invalid YouTube login references")

    return values


def broker(command: Sequence[str], references: dict[str, str]) -> int:
    """Unlock one item once; pass only the field Google currently needs."""
    credentials = OnePasswordLogin(
        references["account"],
        {field: references.get(field, "") for field in CREDENTIAL_FIELDS},
    )
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        text=True, encoding="utf-8", bufsize=1,
    )
    assert process.stdin is not None and process.stdout is not None

    try:
        for line in process.stdout:
            if not line.startswith(REQUEST_PREFIX):
                print(line, end="", flush=True)
                continue

            field = line.removeprefix(REQUEST_PREFIX).strip()

            try:
                if field not in CREDENTIAL_FIELDS or not references.get(field):
                    raise CredentialAccessError(
                        "The requested 1Password field is not configured",
                    )

                value = credentials.read(field)
                reply = json.dumps({"value": value})
            except OnePasswordLoginError as error:
                print(str(error), file=sys.stderr, flush=True)
                reply = json.dumps({"error": True})

            process.stdin.write(reply + "\n")
            process.stdin.flush()
            reply = ""
            value = ""

        return process.wait()
    finally:
        credentials.close()

        with suppress(OSError):
            process.stdin.close()

        with suppress(OSError):
            process.stdout.close()

        if process.poll() is None:
            process.terminate()

            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def run(compose: list[str], arguments: list[str]) -> int:
    if any(arg in {"--help", "-h"} for arg in arguments):
        return subprocess.call([
            *compose, "run", "--build", "--rm", "--no-deps", "-T", "dank",
            "youtube-login", *arguments,
        ])

    status = subprocess.call([*compose, "build", "dank"])

    if status:
        return status

    base = [*compose, "run", "--rm", "--no-deps", "-T", "dank"]
    status = subprocess.call([
        *base, "youtube-login", "--check-session", *arguments,
    ])

    if status != 3 or "--check-session" in arguments:
        return status

    if "--manual" in arguments:
        return subprocess.call([
            *compose, "run", "--rm", "--no-deps", "-T", "-p",
            "127.0.0.1:6081:6080", "dank", "dank-x-login", "--youtube",
            *arguments,
        ])

    result = subprocess.run(
        [*base, "youtube-login", "--onepassword-references", *arguments],
        capture_output=True, text=True, check=False,
    )

    if result.returncode:
        print("Could not load YouTube login references.", file=sys.stderr)
        return result.returncode

    references = parse_references(result.stdout)

    if not references:
        print(
            "Configure [youtube.onepassword] for Google sign-in "
            "with 1Password, "
            "or explicitly choose --manual for the browser sign-in window.",
            file=sys.stderr,
        )
        return 3

    print(
        "Signing in to Google with 1Password. "
        "Authorize vault access if prompted.", flush=True,
    )
    return broker(
        [*base, "youtube-login", "--credentials-stdio", *arguments],
        references,
    )


def cancel_on_termination(_number: int, _frame: object) -> None:
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGTERM, cancel_on_termination)
    arguments = sys.argv[1:]

    if "--" not in arguments or arguments.index("--") == 0:
        print("Expected a Compose command followed by -- and login arguments.",
              file=sys.stderr)
        raise SystemExit(2)

    separator = arguments.index("--")

    try:
        status = run(arguments[:separator], arguments[separator + 1:])
    except OnePasswordLoginError as error:
        print(str(error), file=sys.stderr)
        status = 1
    except (OSError, ValueError):
        print("Could not start YouTube sign-in.", file=sys.stderr)
        status = 1
    except KeyboardInterrupt:
        print("YouTube sign-in cancelled.", file=sys.stderr)
        status = 130

    raise SystemExit(status)


if __name__ == "__main__":
    main()
