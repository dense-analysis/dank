from __future__ import annotations

import argparse
import asyncio
import re
import sys
from typing import Any, cast

from dank.config import load_settings
from dank.storage.clickhouse import ClickHouseClient

WRITE_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "optimize",
    "system",
    "attach",
    "detach",
    "rename",
    "grant",
    "revoke",
    "kill",
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.tools.clickhouse_query")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "-q",
        "--query",
        default=None,
        required=True,
        help="A single SELECT query to run",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Show full output (select ouput is truncated by default)",
    )
    args = parser.parse_args()

    try:
        safe_query = _validate_select_query(args.query)
    except ValueError as e:
        sys.exit(str(e))

    asyncio.run(_run_query(args.config, safe_query, full=args.full))


def _validate_select_query(query: str) -> str:
    normalized = query.strip()

    if normalized.endswith(";"):
        normalized = normalized[:-1].strip()

    lowered = normalized.lower()

    if not lowered.startswith("select") and not lowered.startswith("show"):
        raise ValueError("Only SELECT or SHOW queries are allowed")

    if ";" in normalized:
        raise ValueError("Only a single statement is allowed")

    if re.search(r"\binto\s+outfile\b", lowered):
        raise ValueError("INTO OUTFILE is not allowed")

    if lowered.startswith("select"):
        for keyword in WRITE_KEYWORDS:
            if re.search(rf"\b{keyword}\b", lowered):
                raise ValueError(f"Keyword {keyword!r} is not allowed")

    return normalized


async def _run_query(
    config_path: str,
    query: str,
    *,
    full: bool = False,
) -> None:
    settings = load_settings(config_path)

    async with ClickHouseClient(settings.clickhouse) as clickhouse_client:
        try:
            result = await clickhouse_client.fetch_json(query)
        except Exception as e:
            match e.args:
                case (str() as msg, *_) if "DB::Exception" in msg:
                    sys.exit(msg.split("DB::Exception: ", 1)[-1])
                case _:
                    raise

        if query.lower().startswith("show"):
            print(result.rows[0]["statement"])
        else:
            print(f"**{len(result.rows)} rows returned**")

            for row_num, row in enumerate(result.rows, 1):
                print()
                print(f"## Row {row_num}")

                for key, value in row.items():
                    if full:
                        # Print full values.
                        print(f"{key}: {value!r}")
                    else:
                        # Print truncated values.
                        match value:
                            case str() if len(value) > 70:
                                # Truncate strings.
                                out = repr(value[:67] + "...")
                            case list() if len(value) > 4:  # type: ignore
                                # Truncate arrays like embeddings.
                                value = cast(list[Any], value)[:4]
                                out = repr(value)[:-1] + ", ...]"
                            case _:
                                out = repr(value)

                        print(f"{key}: {out}")


if __name__ == "__main__":
    main()
