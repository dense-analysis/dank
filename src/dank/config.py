from __future__ import annotations

import math
import pathlib
import re
import tomllib
from enum import StrEnum
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

MEDIA_DOWNLOAD_TYPES = ("image", "audio", "video")


class ConfigError(RuntimeError):
    pass


class BrowserScrapeSettings(NamedTuple):
    """Collection limits and pacing for one browser-backed listing."""

    max_posts: int = 100
    max_scrolls: int = 20
    scroll_pause_seconds: float = 1.5


class XOnePasswordSettings(NamedTuple):
    username: str
    password: str


class XConversationSettings(NamedTuple):
    enabled: bool = True
    max_posts_per_account: int = 5
    max_replies: int = 50
    max_scrolls: int = 3
    timeout_seconds: float = 30.0


class XSettings(NamedTuple):
    scrape: BrowserScrapeSettings = BrowserScrapeSettings(max_posts=200)
    onepassword: XOnePasswordSettings | None = None
    conversation: XConversationSettings = XConversationSettings()


class YouTubeSettings(NamedTuple):
    """Feed discovery is independent of optional video archiving."""

    scrape: BrowserScrapeSettings = BrowserScrapeSettings(max_posts=200)
    scrape_videos: bool = False
    include_shorts: bool = True
    include_posts: bool = True
    authenticated: bool = False
    homepage: bool = False
    hype: bool = True
    news_tabs: tuple[str, ...] = ("top_stories", "business", "world")
    region: str = "GB"
    stall_scrolls: int = 3
    known_overlap: int = 5
    profile_dir: pathlib.Path | None = None


class RedditCommentSort(StrEnum):
    BEST = "best"
    TOP = "top"
    NEW = "new"
    CONTROVERSIAL = "controversial"
    OLD = "old"
    QA = "qa"


class RedditSettings(NamedTuple):
    """Scrape limits and stall detection applied to each Reddit community."""

    scrape: BrowserScrapeSettings = BrowserScrapeSettings()
    stall_scrolls: int = 3
    capture_comments: bool = True
    comment_limit: int = 50
    comment_sort: RedditCommentSort = RedditCommentSort.BEST
    download_videos: bool = False
    request_interval_seconds: float = 2.0
    detail_cache_hours: float = 24.0


class RedditSort(StrEnum):
    BEST = "best"
    HOT = "hot"
    NEW = "new"
    TOP = "top"
    RISING = "rising"


class RedditTimeFilter(StrEnum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


class RedditSourceConfig(NamedTuple):
    """The ranking and time window used to select community posts."""

    sort: RedditSort = RedditSort.TOP
    time_filter: RedditTimeFilter = RedditTimeFilter.DAY


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
    feed_urls: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    reddit: RedditSourceConfig | None = None


class BrowserSettings(NamedTuple):
    executable_path: pathlib.Path | None
    connection_timeout: float | None
    connection_max_tries: int | None


class LoggingSettings(NamedTuple):
    file_path: pathlib.Path
    level: str


class ScrapeSettings(NamedTuple):
    source_concurrency: int = 4
    http_concurrency: int = 16
    http_per_host: int = 2
    media_concurrency: int = 4
    queue_batches: int = 2


class Settings(NamedTuple):
    clickhouse: ClickHouseSettings
    x: XSettings
    data_dir: pathlib.Path
    max_asset_bytes: int | None
    feed_staleness_days: int
    sources: tuple[SourceConfig, ...]
    browser: BrowserSettings
    logging: LoggingSettings
    keep_feed_on_fetch_failure: bool = False
    max_entries_per_feed: int = 0
    scrape: ScrapeSettings = ScrapeSettings()
    media_download_types: tuple[str, ...] = MEDIA_DOWNLOAD_TYPES
    reddit: RedditSettings = RedditSettings()
    youtube: YouTubeSettings = YouTubeSettings()


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
            parsed.append(_parse_source(domain, tuple(accounts), item))

    return tuple(parsed)


def _parse_source(
    domain: str, accounts: tuple[str, ...], item: object,
) -> SourceConfig:
    options = _as_dict(item) or {}
    tags = tuple(dict.fromkeys(
        tag.lower() for tag in _source_strings(options.get("tags", []), "tags")
    ))
    reddit_domains = {
        f"{scheme}{host}{suffix}"
        for scheme in ("", "http://", "https://")
        for host in ("reddit.com", "www.reddit.com", "old.reddit.com")
        for suffix in ("", "/")
    }

    if domain in reddit_domains:
        feeds = _source_strings(options.get("feed_urls", []), "feed_urls")
        communities: dict[str, str] = {}

        for url in feeds:
            canonical = _reddit_feed_url(url)
            communities.setdefault(canonical.lower(), canonical)

        if not communities or accounts:
            raise ConfigError(
                "Reddit sources require community feed_urls, not accounts",
            )

        return SourceConfig(
            domain="reddit.com", accounts=(),
            feed_urls=tuple(communities.values()), tags=tags,
            reddit=_parse_reddit_source(options),
        )

    if {"reddit_sort", "reddit_time_filter"}.intersection(options):
        raise ConfigError("Reddit sorting options require a Reddit source")

    if domain in {"youtube.com", "www.youtube.com", "https://youtube.com",
                  "https://www.youtube.com"}:
        return SourceConfig(
            "youtube.com", accounts,
            tuple(dict.fromkeys(
                _youtube_feed_url(url)
                for url in _source_strings(options.get("feed_urls", []),
                                           "feed_urls")
            )), tags,
        )

    feeds = _parse_feed_urls(options.get("feed_urls", []))

    if domain == "x.com" and feeds:
        raise ConfigError("sources.feed_urls is not supported for x.com")

    return SourceConfig(domain, accounts, feeds, tags)


def _reddit_feed_url(value: str) -> str:
    # Canonical URLs also give the reader a stable community identity.
    path = value

    if "://" in value:
        try:
            parsed = urlsplit(value)
        except ValueError:
            raise ConfigError(
                "sources.feed_urls must be Reddit community URLs",
            ) from None

        if (
            parsed.scheme not in {"http", "https"}
            or parsed.netloc.lower() not in {
                "reddit.com", "www.reddit.com", "old.reddit.com",
            }
            or parsed.query or parsed.fragment
        ):
            raise ConfigError(
                "sources.feed_urls must be Reddit community URLs",
            )

        path = parsed.path

    match = re.fullmatch(r"/?r/([A-Za-z0-9_]{2,21})/?", path)

    if not match:
        raise ConfigError(
            "sources.feed_urls must contain r/community or a Reddit "
            "community URL; use reddit_sort for sorting",
        )

    return f"https://www.reddit.com/r/{match[1]}/"


def _parse_reddit_source(options: dict[str, Any]) -> RedditSourceConfig:
    defaults = RedditSourceConfig()

    return RedditSourceConfig(
        sort=_parse_enum(
            RedditSort, options.get("reddit_sort", defaults.sort),
            "sources.reddit_sort",
        ),
        time_filter=_parse_enum(
            RedditTimeFilter,
            options.get("reddit_time_filter", defaults.time_filter),
            "sources.reddit_time_filter",
        ),
    )


def _parse_enum[T: StrEnum](
    enum: type[T], value: object, field: str,
) -> T:
    try:
        if not isinstance(value, str):
            raise ValueError

        return enum(value.strip().lower())
    except ValueError:
        raise ConfigError(
            f"{field} must be one of {', '.join(enum)}",
        ) from None


def _parse_reddit_settings(value: object) -> RedditSettings:
    data = _as_dict(value)

    if data is None:
        raise ConfigError("reddit must be a table")

    defaults = RedditSettings()
    default_limits = {
        **defaults.scrape._asdict(), "stall_scrolls": defaults.stall_scrolls,
    }
    limits: dict[str, int] = {}

    for key in ("max_posts", "max_scrolls", "stall_scrolls"):
        setting = data.get(key, default_limits[key])
        minimum = 0 if key == "max_scrolls" else 1

        if (
            not isinstance(setting, int) or isinstance(setting, bool)
            or not minimum <= setting < 2 ** 32
        ):
            raise ConfigError(f"reddit.{key} must be an integer >= {minimum}")

        limits[key] = setting

    pause = data.get(
        "scroll_pause_seconds", defaults.scrape.scroll_pause_seconds,
    )

    if (
        not isinstance(pause, int | float) or isinstance(pause, bool)
        or not math.isfinite(pause) or not 0.25 <= pause <= 60
    ):
        raise ConfigError("reddit.scroll_pause_seconds must be 0.25 to 60")

    for key in ("capture_comments", "download_videos"):
        if not isinstance(data.get(key, getattr(defaults, key)), bool):
            raise ConfigError(f"reddit.{key} must be a boolean")

    comment_limit = data.get("comment_limit", defaults.comment_limit)

    if (
        not isinstance(comment_limit, int) or isinstance(comment_limit, bool)
        or not 1 <= comment_limit <= 500
    ):
        raise ConfigError(
            "reddit.comment_limit must be an integer from 1 to 500",
        )

    intervals: dict[str, float] = {}

    for key, minimum, maximum in (
        ("request_interval_seconds", 1, 60),
        ("detail_cache_hours", 0, 168),
    ):
        setting = data.get(key, getattr(defaults, key))

        if (
            not isinstance(setting, int | float) or isinstance(setting, bool)
            or not math.isfinite(setting) or not minimum <= setting <= maximum
        ):
            raise ConfigError(f"reddit.{key} must be {minimum} to {maximum}")

        intervals[key] = float(setting)

    return RedditSettings(
        request_interval_seconds=intervals["request_interval_seconds"],
        detail_cache_hours=intervals["detail_cache_hours"],
        capture_comments=data.get(
            "capture_comments", defaults.capture_comments,
        ),
        download_videos=data.get("download_videos", defaults.download_videos),
        comment_limit=comment_limit,
        comment_sort=_parse_enum(
            RedditCommentSort, data.get("comment_sort", defaults.comment_sort),
            "reddit.comment_sort",
        ),
        scrape=BrowserScrapeSettings(
            max_posts=limits["max_posts"], max_scrolls=limits["max_scrolls"],
            scroll_pause_seconds=float(pause),
        ),
        stall_scrolls=limits["stall_scrolls"],
    )


def _source_strings(value: object, field: str) -> tuple[str, ...]:
    values = _as_list(value)

    if values is None or any(
        not isinstance(item, str) or not item.strip() for item in values
    ):
        raise ConfigError(
            f"sources.{field} must be an array of non-empty strings",
        )

    return tuple(dict.fromkeys(str(item).strip() for item in values))


def _parse_feed_urls(value: object) -> tuple[str, ...]:
    urls = _source_strings(value, "feed_urls")

    for url in urls:
        try:
            parsed = urlsplit(url)
            valid = (
                parsed.scheme in {"http", "https"}
                and bool(parsed.hostname)
                and not any(char.isspace() for char in url)
            )
            # Accessing the port validates malformed host:port combinations.
            _ = parsed.port
        except ValueError:
            valid = False

        if not valid:
            raise ConfigError("sources.feed_urls must contain HTTP(S) URLs")

    return urls


def _parse_feed_entry_limit(value: object) -> int:
    if (
        not isinstance(value, int) or isinstance(value, bool)
        or not 0 <= value < 2 ** 64
    ):
        raise ConfigError(
            "rss.max_entries_per_feed must be a non-negative 64-bit integer",
        )

    return value


def _parse_media_download_types(value: object) -> tuple[str, ...]:
    data = _as_dict(value)

    if data is None:
        raise ConfigError("media must be a table")

    if "download_types" not in data:
        return MEDIA_DOWNLOAD_TYPES

    values = _as_list(data["download_types"])

    if values is None or any(not isinstance(item, str) for item in values):
        raise ConfigError("media.download_types must be an array of strings")

    types = tuple(dict.fromkeys(str(item).strip().lower() for item in values))

    if any(kind not in MEDIA_DOWNLOAD_TYPES for kind in types):
        raise ConfigError(
            "media.download_types accepts only image, audio and video",
        )

    return types


def _parse_scrape_settings(value: object) -> ScrapeSettings:
    data = _as_dict(value)

    if data is None:
        raise ConfigError("scrape must be a table")

    values: dict[str, int] = {}

    for key, default in ScrapeSettings()._asdict().items():
        setting = data.get(key, default)

        if (
            not isinstance(setting, int) or isinstance(setting, bool)
            or not 1 <= setting < 2 ** 32
        ):
            raise ConfigError(
                f"scrape.{key} must be a positive 32-bit integer",
            )

        values[key] = setting

    return ScrapeSettings(**values)


def _parse_path(value: object) -> pathlib.Path | None:
    """Return either a path from a trimmed string or None."""
    if isinstance(value, str) and (value := value.strip()):
        return pathlib.Path(value).expanduser()

    return None


def _parse_optional_float(value: object) -> float | None:
    if isinstance(value, int | float):
        return float(value)

    return None


def _parse_optional_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    return None


def _parse_x_conversation(value: object) -> XConversationSettings:
    data = _as_dict(value)

    if data is None:
        raise ConfigError("x.conversation must be a table")

    defaults = XConversationSettings()
    enabled = data.get("enabled", defaults.enabled)

    if not isinstance(enabled, bool):
        raise ConfigError("x.conversation.enabled must be a boolean")

    values: dict[str, int] = {}

    for key, minimum, maximum in (
        ("max_posts_per_account", 1, 100), ("max_replies", 1, 200),
        ("max_scrolls", 0, 20),
    ):
        setting = data.get(key, getattr(defaults, key))

        if (not isinstance(setting, int) or isinstance(setting, bool)
                or not minimum <= setting <= maximum):
            raise ConfigError(
                f"x.conversation.{key} must be an integer "
                f"between {minimum} and {maximum}",
            )

        values[key] = setting

    timeout = data.get("timeout_seconds", defaults.timeout_seconds)

    if (not isinstance(timeout, int | float) or isinstance(timeout, bool)
            or not math.isfinite(timeout) or not 1 <= timeout <= 120):
        raise ConfigError(
            "x.conversation.timeout_seconds must be between 1 and 120",
        )

    return XConversationSettings(
        enabled=enabled, timeout_seconds=float(timeout), **values,
    )


def _parse_x_onepassword(value: object) -> XOnePasswordSettings | None:
    if value is None:
        return None

    data = _as_dict(value)

    if data is None:
        raise ConfigError("x.onepassword must be a table of secret references")

    references: list[str] = []

    for field in ("username", "password"):
        reference = data.get(field)

        if (
            not isinstance(reference, str)
            or not reference.startswith("op://")
            or len(reference.removeprefix("op://").split("/")) < 3
            or any(not part for part in reference[5:].split("/"))
            or any(ord(char) < 32 or ord(char) == 127 for char in reference)
        ):
            raise ConfigError(
                f"x.onepassword.{field} must be an op:// secret reference",
            )

        references.append(reference)

    return XOnePasswordSettings(*references)


def load_settings(path: str | pathlib.Path = "config.toml") -> Settings:
    config_path = pathlib.Path(path)

    try:
        with config_path.open("rb") as file:
            data = tomllib.load(file)
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"Broken {path}: {error}") from None

    x_data: dict[str, Any] = _as_dict(data.get("x")) or {}

    sources = _parse_sources(data.get("sources"))
    x_defaults = XSettings().scrape

    x_settings = XSettings(
        scrape=BrowserScrapeSettings(
            max_posts=int(x_data.get("max_posts", x_defaults.max_posts)),
            max_scrolls=int(x_data.get("max_scrolls", x_defaults.max_scrolls)),
            scroll_pause_seconds=float(x_data.get(
                "scroll_pause_seconds", x_defaults.scroll_pause_seconds,
            )),
        ),
        onepassword=_parse_x_onepassword(x_data.get("onepassword")),
        conversation=_parse_x_conversation(x_data.get("conversation", {})),
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
    data_dir = _parse_path(storage_data.get("data_dir"))

    if data_dir is None:
        data_dir = pathlib.Path("data")

    max_asset_bytes = _parse_optional_int(storage_data.get("max_asset_bytes"))

    if max_asset_bytes is not None and max_asset_bytes <= 0:
        max_asset_bytes = None

    rss_data: dict[str, Any] = _as_dict(data.get("rss")) or {}
    keep_feed_on_fetch_failure = rss_data.get(
        "keep_feed_on_fetch_failure", False,
    )

    if not isinstance(keep_feed_on_fetch_failure, bool):
        raise ConfigError("rss.keep_feed_on_fetch_failure must be a boolean")

    max_entries_per_feed = _parse_feed_entry_limit(
        rss_data.get("max_entries_per_feed", 0),
    )
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
        data_dir=data_dir,
        max_asset_bytes=max_asset_bytes,
        feed_staleness_days=feed_staleness_days,
        sources=sources,
        browser=browser_settings,
        logging=logging_settings,
        keep_feed_on_fetch_failure=keep_feed_on_fetch_failure,
        max_entries_per_feed=max_entries_per_feed,
        scrape=_parse_scrape_settings(data.get("scrape", {})),
        media_download_types=_parse_media_download_types(
            data.get("media", {}),
        ),
        reddit=_parse_reddit_settings(data.get("reddit", {})),
        youtube=_parse_youtube_settings(data.get("youtube", {})),
    )


YOUTUBE_NEWS_TABS = (
    "top_stories", "sports", "entertainment", "business", "world",
)


def _youtube_feed_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ConfigError(
            "YouTube feed_urls must be YouTube listing URLs",
        ) from None

    path = parsed.path.rstrip("/")
    allowed = (
        path in {"", "/feed/hype", "/feed/news_destination", "/playlist"}
        or path.startswith("/feed/news_destination/")
        or re.fullmatch(
            r"/(?:@[^/?#]+|(?:channel|c|user)/[^/?#]+)"
            r"(?:/(?:videos|shorts|posts|community|featured))?", path,
        ) is not None
    )

    if (parsed.scheme not in {"http", "https"}
            or parsed.netloc.lower() not in {"youtube.com", "www.youtube.com"}
            or not allowed or parsed.fragment
            or any(char.isspace() for char in value)):
        raise ConfigError("YouTube feed_urls must be YouTube listing URLs")

    if re.fullmatch(r"/(?:@[^/]+|(?:channel|c|user)/[^/]+)", path):
        path += "/videos"

    return "https://www.youtube.com" + (path or "/") + (
        "?" + parsed.query if parsed.query else ""
    )


def _parse_youtube_settings(value: object) -> YouTubeSettings:
    data = _as_dict(value)

    if data is None:
        raise ConfigError("youtube must be a table")

    defaults = YouTubeSettings()
    flags: dict[str, bool] = {}

    for key in ("scrape_videos", "include_shorts", "include_posts",
                "authenticated", "homepage", "hype"):
        flag = data.get(key, getattr(defaults, key))

        if not isinstance(flag, bool):
            raise ConfigError(f"youtube.{key} must be a boolean")

        flags[key] = flag

    if flags["homepage"] and not flags["authenticated"]:
        raise ConfigError("youtube.homepage requires youtube.authenticated")

    limits: dict[str, int] = {}

    for key in ("max_posts", "max_scrolls", "stall_scrolls", "known_overlap"):
        fallback = getattr(defaults.scrape, key, getattr(defaults, key, None))
        setting = data.get(key, fallback)
        minimum = 0 if key == "max_scrolls" else 1

        if (not isinstance(setting, int) or isinstance(setting, bool)
                or not minimum <= setting < 2 ** 32):
            raise ConfigError(f"youtube.{key} must be an integer >= {minimum}")

        limits[key] = setting

    pause = data.get(
        "scroll_pause_seconds", defaults.scrape.scroll_pause_seconds,
    )

    if (not isinstance(pause, int | float) or isinstance(pause, bool)
            or not math.isfinite(pause) or not 0.25 <= pause <= 60):
        raise ConfigError("youtube.scroll_pause_seconds must be 0.25 to 60")

    tabs = data.get("news_tabs", list(defaults.news_tabs))

    if (not isinstance(tabs, list) or any(
            tab not in YOUTUBE_NEWS_TABS for tab in cast(list[object], tabs))):
        raise ConfigError(
            "youtube.news_tabs accepts " + ", ".join(YOUTUBE_NEWS_TABS),
        )

    region = data.get("region", defaults.region)

    if not isinstance(region, str) or not re.fullmatch("[A-Za-z]{2}", region):
        raise ConfigError("youtube.region must be a two-letter country code")


    return YouTubeSettings(
        **flags, region=region.upper(),
        news_tabs=tuple(dict.fromkeys(cast(list[str], tabs))),
        profile_dir=_youtube_profile(data.get("profile_dir")),
        stall_scrolls=limits["stall_scrolls"],
        known_overlap=limits["known_overlap"],
        scrape=BrowserScrapeSettings(
            max_posts=limits["max_posts"], max_scrolls=limits["max_scrolls"],
            scroll_pause_seconds=float(pause),
        ),
    )


def _youtube_profile(value: object) -> pathlib.Path | None:
    if value is not None and _parse_path(value) is None:
        raise ConfigError("youtube.profile_dir must be a non-empty path")

    return _parse_path(value)
