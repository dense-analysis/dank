from __future__ import annotations

import pathlib
import tomllib
from typing import Any, NamedTuple, cast


class XSettings(NamedTuple):
    email: str
    username: str
    password: str
    max_posts: int
    max_scrolls: int
    scroll_pause_seconds: float


class ClickHouseSettings(NamedTuple):
    host: str
    port: int
    database: str
    username: str
    password: str
    secure: bool
    use_http: bool


class SourceConfig(NamedTuple):
    domain: str
    accounts: tuple[str, ...]


class BrowserSettings(NamedTuple):
    executable_path: pathlib.Path | None
    connection_timeout: float | None
    connection_max_tries: int | None


class EmailSettings(NamedTuple):
    host: str
    username: str
    password: str
    port: int


class LoggingSettings(NamedTuple):
    file_path: pathlib.Path
    level: str


class Settings(NamedTuple):
    clickhouse: ClickHouseSettings
    x: XSettings
    assets_dir: pathlib.Path
    max_asset_bytes: int | None
    feed_staleness_days: int
    sources: tuple[SourceConfig, ...]
    browser: BrowserSettings
    email: EmailSettings | None
    logging: LoggingSettings


def _as_dict(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)

    return None


def _as_list(value: object) -> list[object] | None:
    if isinstance(value, list):
        return cast(list[object], value)

    return None


def _parse_sources(raw_sources: object) -> tuple[SourceConfig, ...]:
    sources = _as_list(raw_sources)

    if not sources:
        return ()

    parsed: list[SourceConfig] = []

    for item in sources:
        # Extract just strings for domains or domains with account lists.
        match item:
            case str() as domain:
                accounts = []
            case {
                "domain": str() as domain,
                "accounts": list() as accounts,  # type: ignore
            }:
                pass
            case {"domain": str() as domain}:
                accounts = []
            case _:
                domain = ""
                accounts = []

        domain = domain.lower().strip()
        accounts = [
            x for x in cast(list[object], accounts) if isinstance(x, str)
        ]

        if domain:
            parsed.append(
                SourceConfig(domain=domain, accounts=tuple(accounts)),
            )

    return tuple(parsed)


def _parse_path(value: object) -> pathlib.Path | None:
    if not isinstance(value, str):
        return None

    trimmed = value.strip()

    if not trimmed:
        return None

    return pathlib.Path(trimmed).expanduser()


def _parse_optional_float(value: object) -> float | None:
    if isinstance(value, int | float):
        return float(value)

    return None


def _parse_optional_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    return None


def load_settings(path: str | pathlib.Path = "config.toml") -> Settings:
    config_path = pathlib.Path(path)

    with config_path.open("rb") as file:
        data = tomllib.load(file)

    x_data: dict[str, Any] = _as_dict(data.get("x")) or {}

    sources = _parse_sources(data.get("sources"))

    x_settings = XSettings(
        email=str(x_data.get("email", "")),
        username=str(x_data.get("username", "")),
        password=str(x_data.get("password", "")),
        max_posts=int(x_data.get("max_posts", 200)),
        max_scrolls=int(x_data.get("max_scrolls", 20)),
        scroll_pause_seconds=float(x_data.get("scroll_pause_seconds", 1.5)),
    )

    clickhouse: dict[str, Any] = _as_dict(data.get("clickhouse")) or {}
    use_http = bool(clickhouse.get("use_http", True))
    default_port = 8123 if use_http else 9000
    clickhouse_settings = ClickHouseSettings(
        host=str(clickhouse.get("host", "localhost")),
        port=int(clickhouse.get("port", default_port)),
        database=str(clickhouse.get("database", "dank")),
        username=str(clickhouse.get("username", "default")),
        password=str(clickhouse.get("password", "")),
        secure=bool(clickhouse.get("secure", False)),
        use_http=use_http,
    )

    storage_data: dict[str, Any] = _as_dict(data.get("storage")) or {}
    assets_dir = pathlib.Path(storage_data.get("assets_dir", "data/assets"))
    max_asset_bytes = _parse_optional_int(storage_data.get("max_asset_bytes"))
    if max_asset_bytes is not None and max_asset_bytes <= 0:
        max_asset_bytes = None

    rss_data: dict[str, Any] = _as_dict(data.get("rss")) or {}
    feed_staleness_days = int(rss_data.get("feed_staleness_days", 14))
    if feed_staleness_days <= 0:
        feed_staleness_days = 14

    browser_data: dict[str, Any] = _as_dict(data.get("browser")) or {}
    browser_settings = BrowserSettings(
        executable_path=_parse_path(browser_data.get("executable_path")),
        connection_timeout=_parse_optional_float(
            browser_data.get("connection_timeout"),
        ),
        connection_max_tries=_parse_optional_int(
            browser_data.get("connection_max_tries"),
        ),
    )

    email_data: dict[str, Any] = _as_dict(data.get("email")) or {}
    email_settings: EmailSettings | None = None

    if email_data:
        host = str(email_data.get("host", "")).strip()
        username = str(email_data.get("username", "")).strip()
        password = str(email_data.get("password", "")).strip()

        if host and username and password:
            email_settings = EmailSettings(
                host=host,
                username=username,
                password=password,
                port=int(email_data.get("port", 993)),
            )

    logging_data: dict[str, Any] = _as_dict(data.get("logging")) or {}
    file_path = _parse_path(logging_data.get("file"))

    if file_path is None:
        file_path = pathlib.Path("dank.log")

    level = str(logging_data.get("level", "INFO")).strip().upper()

    if not level:
        level = "INFO"

    logging_settings = LoggingSettings(file_path=file_path, level=level)

    return Settings(
        clickhouse=clickhouse_settings,
        x=x_settings,
        assets_dir=assets_dir,
        max_asset_bytes=max_asset_bytes,
        feed_staleness_days=feed_staleness_days,
        sources=sources,
        browser=browser_settings,
        email=email_settings,
        logging=logging_settings,
    )
