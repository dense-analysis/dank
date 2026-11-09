"""dank-db: manage the Postgres schema by hand, and import sources."""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import platform
import sys

import psycopg

from dank.config import ConfigError, PostgresSettings, Settings, load_settings
from dank.db import atlas, install_atlas
from dank.db.engine import async_engine, async_sessions, create_database
from dank.db.sources import PostgresSourceRepository
from dank.errors import DankError
from dank.sources.service import ImportResult, SourceService


def _settings(args: argparse.Namespace) -> PostgresSettings:
    return load_settings(args.config).postgres


def _upgrade(args: argparse.Namespace) -> int:
    settings = _settings(args)
    asyncio.run(atlas.upgrade(settings))
    print(f"Postgres database {settings.database} is at head.")

    return 0


def _current(args: argparse.Namespace) -> int:
    settings = _settings(args)
    status = asyncio.run(atlas.schema_status(settings))
    print(f"Postgres database {settings.database} is {status.state}.")
    print(f"Applied: {', '.join(sorted(status.applied)) or 'none'}")
    print(f"In code: {', '.join(sorted(status.available)) or 'none'}")

    return 0 if status.state is atlas.SchemaState.AT_HEAD else 1


def _check(args: argparse.Namespace) -> int:
    asyncio.run(atlas.validate_migrations(args.dev_url))
    drift = asyncio.run(atlas.find_drift(args.dev_url))

    if drift:
        print("The models have changes no migration covers:", file=sys.stderr)
        print(drift, file=sys.stderr)
        print("Generate one with `dank-db diff <name>`.", file=sys.stderr)

        return 1

    print("Migrations are valid and match the models.")

    return 0


def _diff(args: argparse.Namespace) -> int:
    return asyncio.run(atlas.run_interactive(
        "migrate", "diff", args.name, "--env", atlas.ENV,
        dev_url=args.dev_url,
    ))


def _create(args: argparse.Namespace) -> int:
    settings = _settings(args)
    created = create_database(settings)
    print(
        f"Created Postgres database {settings.database}."
        if created else
        f"Postgres database {settings.database} already exists.",
    )

    return 0


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


async def import_sources(settings: Settings) -> ImportResult:
    """Save config.toml's sources in Postgres, once per database."""
    if not settings.postgres.enabled:
        raise ConfigError(
            "Postgres is turned off in config.toml "
            "([postgres] enabled = false), so sources can't be imported.",
        )

    # Like every CLI, this never migrates.
    await atlas.require_head(settings.postgres)
    engine = async_engine(settings.postgres)

    try:
        repository = PostgresSourceRepository(async_sessions(engine))

        return await SourceService(repository).import_settings(settings)
    finally:
        await engine.dispose()


def _import_sources(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)

    if not settings.sources:
        print("config.toml lists no sources, so nothing was imported.")

        return 0

    try:
        result = asyncio.run(import_sources(settings))
    except DankError as error:
        print(f"dank-db: {error}", file=sys.stderr)

        for problem in error.fields:
            print(f"  {problem.path}: {problem.message}", file=sys.stderr)

        if error.fields:
            print(
                "Sources are numbered from 0 in the order config.toml lists "
                "them.",
                file=sys.stderr,
            )

        return 1

    print(
        f"Imported {_plural(result.imported, 'source')} into Postgres "
        f"database {settings.postgres.database}.",
    )

    if result.duplicates:
        print(
            f"Skipped {_plural(result.duplicates, 'exact duplicate')} in "
            "config.toml.",
        )

    print("Collection still reads config.toml for now.")

    return 0


def _run_atlas(args: argparse.Namespace) -> int:
    return asyncio.run(
        atlas.run_interactive(*args.arguments, dev_url=args.dev_url),
    )


def _install_atlas(_args: argparse.Namespace) -> int:
    target = pathlib.Path(sys.prefix) / "bin" / "atlas"
    install_atlas.install(
        target,
        install_atlas.platform_name(platform.system(), platform.machine()),
    )
    print(f"Installed Atlas {install_atlas.VERSION} at {target}")

    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dank-db", description=__doc__)
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def command(
        name: str,
        handler: object,
        description: str,
        *,
        dev_url: bool = False,
    ) -> argparse.ArgumentParser:
        sub = commands.add_parser(
            name,
            help=description,
            description=description,
        )
        sub.set_defaults(handler=handler)

        if dev_url:
            sub.add_argument(
                "--dev-url",
                default=atlas.DOCKER_DEV_URL,
                help=(
                    "An empty database for Atlas to compare schemas in; "
                    "defaults to a temporary Docker container"
                ),
            )

        return sub

    command("upgrade", _upgrade, "Apply pending migrations")
    command("current", _current, "Show migrations; exit 1 unless at head")
    command(
        "check",
        _check,
        "Check the migrations are valid and match the models",
        dev_url=True,
    )
    command(
        "diff",
        _diff,
        "Generate a migration from model changes",
        dev_url=True,
    ).add_argument("name", help="A short name for the migration")
    command("create", _create, "Create the configured database if missing")
    command(
        "import-sources",
        _import_sources,
        "Save config.toml's sources in Postgres; only once per database",
    )
    command(
        "atlas",
        _run_atlas,
        "Run any other Atlas command, with telemetry off",
        dev_url=True,
    ).add_argument("arguments", nargs=argparse.REMAINDER)
    command("install-atlas", _install_atlas, "Install the pinned Atlas binary")

    return parser


def main() -> None:
    args = _parser().parse_args()

    try:
        sys.exit(args.handler(args))
    except (
        ConfigError,
        atlas.SchemaError,
        install_atlas.InstallError,
        psycopg.Error,
    ) as error:
        # psycopg's messages name the user and host, never the password.
        print(f"dank-db: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
