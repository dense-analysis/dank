"""Runs the Atlas CLI. Nothing else in DANK does, so it stays replaceable."""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import re
import shutil
import sys
import tempfile
from enum import StrEnum
from typing import Any, NamedTuple, cast
from urllib.parse import quote

from dank.config import PostgresSettings, read_postgres_password
from dank.db.install_atlas import VERSION

DB_DIR = pathlib.Path(__file__).parent
ENV = "dank"
# Matches the Compose image, as Atlas needs the same major version to compare
# schemas. A test checks this against compose.yaml.
DOCKER_DEV_URL = "docker://postgres/18/dev?search_path=public"
# A process waiting on another's migration fails after this long. Atlas's 10 s
# default would stop a second server during any slower migration.
LOCK_TIMEOUT = "5m"
# Telemetry and update checks are on unless these opt out.
# https://atlasgo.io/cli/data-privacy
OPT_OUTS = {
    "ATLAS_NO_ANON_TELEMETRY": "true",
    "ATLAS_NO_UPDATE_NOTIFIER": "true",
    "ATLAS_NO_UPGRADE_SUGGESTIONS": "true",
}
INSTALL_HINT = "Run `make atlas`; see docs/setup.md#atlas."
NOT_INSTALLED = f"Atlas isn't installed. {INSTALL_HINT}"
# The first line of `atlas version`, as in "atlas version v1.3.0".
VERSION_LINE = re.compile(r"^atlas version (v\S+)$", re.MULTILINE)


class SchemaError(Exception):
    """The Postgres schema can't be used, or Atlas failed to change it."""


class SchemaAheadError(SchemaError):
    """The database has migrations this code doesn't know about."""


class SchemaState(StrEnum):
    AT_HEAD = "at head"
    BEHIND = "behind the code"
    AHEAD = "ahead of the code"


class SchemaStatus(NamedTuple):
    applied: frozenset[str]
    available: frozenset[str]

    @property
    def state(self) -> SchemaState:
        # Atlas calls a database that's ahead "OK", so compare the versions
        # rather than trusting its Status.
        if self.applied - self.available:
            return SchemaState.AHEAD

        if self.available - self.applied:
            return SchemaState.BEHIND

        return SchemaState.AT_HEAD


def parse_status(output: str) -> SchemaStatus:
    """Read the output of `migrate status --format '{{ json . }}'`."""
    report: dict[str, Any] = json.loads(output)

    def versions(key: str) -> frozenset[str]:
        # Atlas writes null rather than an empty list.
        files = cast("list[dict[str, Any]] | None", report.get(key)) or []

        return frozenset(str(file["Version"]) for file in files)

    return SchemaStatus(versions("Applied"), versions("Available"))


def atlas_url(settings: PostgresSettings, password: str) -> str:
    # Atlas needs its own scheme, and "prefer" matches psycopg's default. Its
    # driver would otherwise require TLS, which Compose's Postgres lacks.
    host = f"[{settings.host}]" if ":" in settings.host else settings.host
    credentials = f"{quote(settings.user, safe='')}:{quote(password, safe='')}"
    database = quote(settings.database, safe="")

    return (
        f"postgres://{credentials}@{host}:{settings.port}/{database}"
        "?sslmode=prefer&search_path=public"
    )


def _search_path() -> str:
    # Finds Atlas and the provider however DANK started, with or without uv.
    return os.pathsep.join(
        (str(pathlib.Path(sys.prefix) / "bin"), os.environ.get("PATH", "")),
    )


def find_atlas() -> str | None:
    return shutil.which("atlas", path=_search_path())


def child_environment(url: str = "", dev_url: str = "") -> dict[str, str]:
    """Environment for Atlas alone. DANK itself configures through TOML."""
    return {
        **os.environ,
        "PATH": _search_path(),
        "DANK_ATLAS_URL": url,
        "DANK_ATLAS_DEV_URL": dev_url,
        **OPT_OUTS,
    }


def scrub(text: str, password: str) -> str:
    if not password:
        return text

    return text.replace(password, "***").replace(
        quote(password, safe=""),
        "***",
    )


def _failure(stderr: str, stdout: str) -> str:
    # Atlas follows its error with suggestions to log in and install things.
    output = stderr or stdout
    errors = [
        line for line in output.splitlines() if line.startswith("Error:")
    ]

    return "\n".join(errors) or output.strip() or "Atlas failed"


async def _require_atlas() -> str:
    """Find Atlas, and refuse any version but the pinned one.

    A copy shared between checkouts, on PATH, stays behind when a checkout
    moves the pin, and nothing else would notice.
    """
    atlas = find_atlas()

    if atlas is None:
        raise SchemaError(NOT_INSTALLED)

    process = await asyncio.create_subprocess_exec(
        atlas,
        "version",
        env=child_environment(),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    found = (
        None if process.returncode
        else VERSION_LINE.search(stdout.decode())
    )

    if found is None:
        raise SchemaError(
            f"{atlas} didn't say which Atlas version it is, and DANK needs "
            f"{VERSION}. {INSTALL_HINT}",
        )

    if found[1] != VERSION:
        raise SchemaError(
            f"{atlas} is Atlas {found[1]}, but DANK needs {VERSION}. "
            f"{INSTALL_HINT}",
        )

    return atlas


async def _run(
    *args: str,
    root: pathlib.Path,
    url: str = "",
    dev_url: str = "",
    password: str = "",
) -> str:
    process = await asyncio.create_subprocess_exec(
        await _require_atlas(),
        *args,
        cwd=root,
        env=child_environment(url, dev_url),
        # Never wait for an answer: Atlas prompts only in a terminal.
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    if process.returncode:
        raise SchemaError(
            scrub(_failure(stderr.decode(), stdout.decode()), password),
        )

    return stdout.decode()


async def _run_on_database(
    settings: PostgresSettings,
    *args: str,
    root: pathlib.Path,
) -> str:
    # Read here, so only the commands that connect need the password file.
    password = read_postgres_password(settings)

    return await _run(
        *args,
        root=root,
        url=atlas_url(settings, password),
        password=password,
    )


async def schema_status(
    settings: PostgresSettings,
    *,
    root: pathlib.Path = DB_DIR,
) -> SchemaStatus:
    output = await _run_on_database(
        settings,
        "migrate", "status", "--env", ENV, "--format", "{{ json . }}",
        root=root,
    )

    return parse_status(output)


def _ahead_message(status: SchemaStatus) -> str:
    versions = ", ".join(sorted(status.applied - status.available))

    return (
        f"The Postgres database has migrations this code doesn't know about "
        f"({versions}). Run code at least as new as the database, or restore "
        "a backup. See docs/development.md."
    )


async def upgrade(
    settings: PostgresSettings,
    *,
    root: pathlib.Path = DB_DIR,
) -> None:
    """Apply pending migrations. Concurrent callers migrate once."""
    status = await schema_status(settings, root=root)

    # Atlas would report success here, and old code would then run against a
    # schema it doesn't understand.
    if status.state is SchemaState.AHEAD:
        raise SchemaAheadError(_ahead_message(status))

    await _run_on_database(
        settings,
        "migrate", "apply", "--env", ENV, "--lock-timeout", LOCK_TIMEOUT,
        root=root,
    )


async def require_head(
    settings: PostgresSettings,
    *,
    root: pathlib.Path = DB_DIR,
) -> None:
    """Stop a CLI that needs Postgres unless the schema is at head.

    CLIs never migrate: that's for the web app and `dank-db upgrade`.
    """
    status = await schema_status(settings, root=root)

    match status.state:
        case SchemaState.AT_HEAD:
            pass
        case SchemaState.AHEAD:
            raise SchemaAheadError(_ahead_message(status))
        case SchemaState.BEHIND:
            raise SchemaError(
                "The Postgres schema is behind this code. Start the web app "
                "or run `dank-db upgrade`.",
            )


async def validate_migrations(
    dev_url: str = DOCKER_DEV_URL,
    *,
    root: pathlib.Path = DB_DIR,
) -> None:
    """Replay the migrations on the dev database, checking atlas.sum."""
    await _run("migrate", "validate", "--env", ENV, root=root, dev_url=dev_url)


async def find_drift(
    dev_url: str = DOCKER_DEV_URL,
    *,
    root: pathlib.Path = DB_DIR,
) -> str:
    """Return the SQL needed to bring the migrations up to the models.

    `migrate diff` writes that SQL as a new migration, so this runs on a
    temporary copy to leave the real directory alone.
    """
    with tempfile.TemporaryDirectory() as temporary:
        copy = pathlib.Path(temporary) / "migrations"
        shutil.copytree(root / "migrations", copy)
        before = set(copy.iterdir())
        await _run(
            "migrate", "diff", "drift", "--env", ENV,
            "--dir", f"file://{copy}",
            root=root,
            dev_url=dev_url,
        )

        return "\n".join(
            path.read_text()
            for path in sorted(set(copy.iterdir()) - before)
            if path.suffix == ".sql"
        )


async def run_interactive(
    *args: str,
    dev_url: str = DOCKER_DEV_URL,
    root: pathlib.Path = DB_DIR,
) -> int:
    """Run Atlas in this terminal, so it can ask about renames."""
    process = await asyncio.create_subprocess_exec(
        await _require_atlas(),
        *args,
        cwd=root,
        env=child_environment(dev_url=dev_url),
    )

    return await process.wait()
