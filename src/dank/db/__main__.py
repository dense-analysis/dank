"""dank-db: manage the Postgres schema by hand."""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import platform
import sys

import psycopg

from dank.config import ConfigError, PostgresSettings, load_settings
from dank.db import atlas, install_atlas
from dank.db.engine import create_database


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
