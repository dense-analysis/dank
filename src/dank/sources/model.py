"""Sources people manage in the app: their shape, rules and compiler.

Pure: nothing here knows about Postgres or HTTP. Values are normalized with
the same functions config.toml uses, so a source means the same thing
whichever place it came from.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, NamedTuple, cast

from dank.config import (
    ConfigError,
    HackerNewsFilter,
    HackerNewsSourceConfig,
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
    canonical_source_domain,
    default_source_adapter,
    feed_url,
    reddit_feed_url,
    youtube_feed_url,
)
from dank.errors import FieldProblem, InvalidInput

# Project heuristics, far above any real source, that bound the size of one
# row and one request.
MAX_TARGETS = 1_000
MAX_TAGS = 32
MAX_TEXT = 2_048
# There are five kinds; only RSS repeats, with different feeds.
MAX_ADAPTERS = 16
# DNS names are at most 253 characters (RFC 1035, section 2.3.4).
MAX_DOMAIN = 253

# Dot-separated labels of letters, digits and inner hyphens, and an optional
# port. Unicode letters count, so internationalized domains work.
_HOST = re.compile(
    r"(?:[^\W_](?:[\w-]*[^\W_])?\.)*[^\W_](?:[\w-]*[^\W_])?(?::\d{1,5})?",
)


class Target(NamedTuple):
    """One account handle or feed URL, which can be turned off on its own."""

    value: str
    enabled: bool = True


class Adapter(NamedTuple):
    kind: SourceAdapter
    # Only RSS adapters have feeds of their own. None means the source's.
    feeds: tuple[Target, ...] | None = None


class NewSource(NamedTuple):
    """A source before the server has given it an ID."""

    enabled: bool
    domain: str
    accounts: tuple[Target, ...] = ()
    feeds: tuple[Target, ...] = ()
    tags: tuple[str, ...] = ()
    # None collects with the domain's default adapter.
    adapters: tuple[Adapter, ...] | None = None
    reddit: RedditSourceConfig | None = None
    hacker_news: HackerNewsSourceConfig | None = None

    def with_id(self, source_id: uuid.UUID) -> Source:
        return Source(
            source_id, self.enabled, self.domain, self.accounts, self.feeds,
            self.tags, self.adapters, self.reddit, self.hacker_news,
        )


class Source(NamedTuple):
    id: uuid.UUID
    enabled: bool
    domain: str
    accounts: tuple[Target, ...] = ()
    feeds: tuple[Target, ...] = ()
    tags: tuple[str, ...] = ()
    adapters: tuple[Adapter, ...] | None = None
    reddit: RedditSourceConfig | None = None
    hacker_news: HackerNewsSourceConfig | None = None


class InvalidSource(InvalidInput):
    code = "invalid_source"

    def __init__(
        self,
        fields: Sequence[FieldProblem],
        message: str = "This source needs attention.",
    ) -> None:
        super().__init__(message, fields=fields)


class _Rule(NamedTuple):
    """How one list's values are normalized, and when two are the same."""

    normalize: Callable[[str], str]
    message: str
    key: Callable[[str], str] = str


_ACCOUNTS = _Rule(str, "")
_FEEDS = _Rule(feed_url, "Use an HTTP or HTTPS feed URL.")
_FEEDS_BY_PLATFORM = {
    SourceAdapter.REDDIT: _Rule(
        reddit_feed_url, "Use r/community or a Reddit community URL.",
        # Reddit treats community names without regard to case.
        str.lower,
    ),
    SourceAdapter.YOUTUBE: _Rule(
        youtube_feed_url, "Use a YouTube channel, playlist or feed URL.",
    ),
}
# Adapters that collect from their platform's own pages, never from RSS.
_NATIVE_ONLY = {SourceAdapter.X, SourceAdapter.REDDIT, SourceAdapter.YOUTUBE}


def normalized[S: (Source, NewSource)](source: S) -> S:
    """The source with every value normalized.

    Raises InvalidSource listing every problem, each at its path in the
    source, such as feeds.1.value.
    """
    problems: list[FieldProblem] = []
    domain = _domain(source.domain, problems)
    native = default_source_adapter(domain)
    accounts = _accounts(source.accounts, native, problems)
    feeds = _feeds(source.feeds, native, problems)
    tags = _tags(source.tags, problems)
    _check_options(source, native, problems)
    adapters = _adapters(source, domain, native, feeds, problems)

    if problems:
        raise InvalidSource(problems)

    return source._replace(
        domain=domain, accounts=accounts, feeds=feeds, tags=tags,
        adapters=adapters,
    )


def _problem(
    problems: list[FieldProblem], path: str, code: str, message: str,
) -> None:
    problems.append(FieldProblem(path, code, message))


def _domain(value: str, problems: list[FieldProblem]) -> str:
    domain = canonical_source_domain(value)

    if len(domain) > MAX_DOMAIN or not _HOST.fullmatch(domain):
        _problem(
            problems, "domain", "invalid_domain",
            "Use a domain such as example.com.",
        )

    return domain


def _text(
    path: str, value: str, problems: list[FieldProblem],
) -> str | None:
    if any(unicodedata.category(char) == "Cc" for char in value):
        _problem(problems, path, "invalid_text", "Remove control characters.")

        return None

    value = value.strip()

    if not value:
        _problem(problems, path, "empty_value", "Enter a value.")

        return None

    return None if _too_long(path, value, problems) else value


def _too_long(path: str, value: str, problems: list[FieldProblem]) -> bool:
    # Checked again after normalizing, which can lengthen a value.
    if len(value) <= MAX_TEXT:
        return False

    _problem(
        problems, path, "too_long", f"Use at most {MAX_TEXT:,} characters.",
    )

    return True


def _targets(
    path: str,
    targets: tuple[Target, ...],
    rule: _Rule,
    problems: list[FieldProblem],
) -> tuple[Target, ...]:
    if len(targets) > MAX_TARGETS:
        _problem(
            problems, path, "too_many",
            f"Use at most {MAX_TARGETS:,} entries.",
        )

        return targets

    seen: set[str] = set()
    result: list[Target] = []

    for index, target in enumerate(targets):
        where = f"{path}.{index}.value"
        value = _text(where, target.value, problems)

        if value is None:
            result.append(target)

            continue

        try:
            value = rule.normalize(value)
        except ConfigError:
            _problem(problems, where, "invalid_url", rule.message)
            result.append(target)

            continue

        if _too_long(where, value, problems):
            result.append(target)

            continue

        if rule.key(value) in seen:
            _problem(
                problems, where, "duplicate_target",
                "This is already in the list.",
            )

        seen.add(rule.key(value))
        result.append(target._replace(value=value))

    return tuple(result)


def _accounts(
    accounts: tuple[Target, ...],
    native: SourceAdapter,
    problems: list[FieldProblem],
) -> tuple[Target, ...]:
    match native:
        case SourceAdapter.REDDIT if accounts:
            _problem(
                problems, "accounts", "invalid_platform_options",
                "Reddit sources follow communities, not accounts.",
            )
        case SourceAdapter.HACKER_NEWS if accounts:
            _problem(
                problems, "accounts", "invalid_platform_options",
                "Hacker News sources have no accounts.",
            )
        case _:
            return _targets("accounts", accounts, _ACCOUNTS, problems)

    return accounts


def _feeds(
    feeds: tuple[Target, ...],
    native: SourceAdapter,
    problems: list[FieldProblem],
) -> tuple[Target, ...]:
    if native is SourceAdapter.X and feeds:
        _problem(
            problems, "feeds", "invalid_platform_options",
            "X sources follow accounts, not feeds.",
        )

        return feeds

    rule = _FEEDS_BY_PLATFORM.get(native, _FEEDS)

    return _targets("feeds", feeds, rule, problems)


def _tags(
    tags: tuple[str, ...], problems: list[FieldProblem],
) -> tuple[str, ...]:
    if len(tags) > MAX_TAGS:
        _problem(problems, "tags", "too_many", f"Use at most {MAX_TAGS} tags.")

        return tags

    result: list[str] = []

    for index, tag in enumerate(tags):
        path = f"tags.{index}"
        value = _text(path, tag, problems)

        # As in config.toml, tags ignore case and repeats.
        if value is not None and not _too_long(
            path, value := value.lower(), problems,
        ):
            result.append(value)

    return tuple(dict.fromkeys(result))


def _check_options(
    source: Source | NewSource,
    native: SourceAdapter,
    problems: list[FieldProblem],
) -> None:
    if native is SourceAdapter.REDDIT:
        if not source.feeds:
            _problem(
                problems, "feeds", "invalid_platform_options",
                "Add at least one community.",
            )

        if source.reddit is None:
            _problem(
                problems, "reddit", "invalid_platform_options",
                "Choose how to sort the communities' posts.",
            )
    elif source.reddit is not None:
        _problem(
            problems, "reddit", "invalid_platform_options",
            "Only Reddit sources take Reddit options.",
        )

    if native is SourceAdapter.HACKER_NEWS:
        if source.hacker_news is None:
            _problem(
                problems, "hacker_news", "invalid_platform_options",
                "Choose which Hacker News stories to collect.",
            )
    elif source.hacker_news is not None:
        _problem(
            problems, "hacker_news", "invalid_platform_options",
            "Only Hacker News sources take Hacker News options.",
        )


def _adapters(
    source: Source | NewSource,
    domain: str,
    native: SourceAdapter,
    feeds: tuple[Target, ...],
    problems: list[FieldProblem],
) -> tuple[Adapter, ...] | None:
    adapters = source.adapters
    has_rss = adapters is not None and any(
        adapter.kind is SourceAdapter.RSS for adapter in adapters
    )

    # config.toml adds this adapter itself, but only when it can't be
    # combined with others; here it's always written out.
    if native is SourceAdapter.HACKER_NEWS and source.feeds and not has_rss:
        _problem(
            problems, "adapters", "unsupported_adapter",
            "Hacker News feeds need an RSS adapter.",
        )

    if adapters is None:
        return None

    if not adapters:
        _problem(
            problems, "adapters", "unsupported_adapter",
            "Leave adapters out to use the default, or list at least one.",
        )

        return adapters

    if len(adapters) > MAX_ADAPTERS:
        _problem(
            problems, "adapters", "too_many",
            f"Use at most {MAX_ADAPTERS} adapters.",
        )

        return adapters

    result: list[Adapter] = []
    seen: set[tuple[SourceAdapter, tuple[str, ...]]] = set()

    for index, adapter in enumerate(adapters):
        path = f"adapters.{index}"
        adapter = _adapter(path, adapter, domain, native, problems)
        # As config.toml does, compare the feeds each adapter ends up with.
        resolved = adapter.feeds if adapter.feeds is not None else feeds
        key = (adapter.kind, tuple(target.value for target in resolved))

        if key in seen:
            _problem(
                problems, path, "unsupported_adapter",
                "Another adapter already collects these feeds.",
            )

        seen.add(key)
        result.append(adapter)

    return tuple(result)


def _adapter(
    path: str,
    adapter: Adapter,
    domain: str,
    native: SourceAdapter,
    problems: list[FieldProblem],
) -> Adapter:
    if adapter.kind is SourceAdapter.RSS:
        if native in _NATIVE_ONLY:
            _problem(
                problems, f"{path}.kind", "unsupported_adapter",
                "RSS adapters only work for publishers and Hacker News.",
            )

        if adapter.feeds is None:
            return adapter

        return adapter._replace(feeds=_targets(
            f"{path}.feeds", adapter.feeds, _FEEDS, problems,
        ))

    if adapter.kind is not native:
        _problem(
            problems, f"{path}.kind", "unsupported_adapter",
            f"{domain} can't use the {adapter.kind} adapter.",
        )

    if adapter.feeds is not None:
        _problem(
            problems, f"{path}.feeds", "unsupported_adapter",
            "Only RSS adapters have feeds of their own.",
        )

    return adapter


class CompiledSources(NamedTuple):
    # Every source and target: archive reads, filters and processing.
    known: tuple[SourceConfig, ...]
    # Enabled sources and targets, after the never-broaden rule.
    collectable: tuple[SourceConfig, ...]


def compile_sources(sources: Iterable[Source]) -> CompiledSources:
    """The SourceConfig values DANK's collectors and readers already use."""
    sources = tuple(sources)
    known = (_config(source, _everything) for source in sources)
    collectable = (
        _config(source, _enabled) for source in sources if source.enabled
    )

    return CompiledSources(
        known=tuple(config for config in known if config is not None),
        collectable=tuple(
            config for config in collectable if config is not None
        ),
    )


def _everything(_target: Target) -> bool:
    return True


def _enabled(target: Target) -> bool:
    return target.enabled


def _values(
    targets: tuple[Target, ...], keep: Callable[[Target], bool],
) -> tuple[str, ...]:
    return tuple(target.value for target in targets if keep(target))


def _emptied(targets: tuple[Target, ...], kept: tuple[str, ...]) -> bool:
    """A list that had targets and has none left."""
    return bool(targets) and not kept


def _config(
    source: Source, keep: Callable[[Target], bool],
) -> SourceConfig | None:
    """The source with only the targets kept, or None if nothing remains.

    A method whose targets were all left out is removed rather than run
    without them, because DANK reads an empty list as "everything": an X
    source with no accounts means For You, and a publisher with no feeds
    means feed discovery.
    """
    accounts = _values(source.accounts, keep)
    feeds = _values(source.feeds, keep)
    methods = source.adapters or (
        Adapter(default_source_adapter(source.domain)),
    )
    kept: list[SourceAdapterConfig] = []

    for adapter in methods:
        match adapter:
            case Adapter(SourceAdapter.RSS, tuple() as own):
                values = _values(own, keep)
                method = None if _emptied(own, values) else (
                    SourceAdapterConfig(SourceAdapter.RSS, values)
                )
            case Adapter(kind) if not _broadens(
                kind, source, accounts, feeds,
            ):
                method = SourceAdapterConfig(kind, feeds)
            case _:
                method = None

        # Disabled feeds can leave two adapters collecting the same thing.
        if method is not None and method not in kept:
            kept.append(method)

    if not kept:
        return None

    return SourceConfig(
        domain=source.domain,
        accounts=accounts,
        feed_urls=feeds,
        tags=source.tags,
        reddit=source.reddit,
        hacker_news=source.hacker_news,
        adapters=() if source.adapters is None else tuple(kept),
    )


def _broadens(
    kind: SourceAdapter,
    source: Source,
    accounts: tuple[str, ...],
    feeds: tuple[str, ...],
) -> bool:
    """Whether running this method with what's left would collect more."""
    match kind:
        case SourceAdapter.X:
            return _emptied(source.accounts, accounts)
        case SourceAdapter.YOUTUBE:
            return _emptied(source.accounts + source.feeds, accounts + feeds)
        case SourceAdapter.HACKER_NEWS:
            return False
        case SourceAdapter.RSS | SourceAdapter.REDDIT:
            return _emptied(source.feeds, feeds)


def sources_from_settings(
    settings: Settings, new_id: Callable[[], uuid.UUID],
) -> tuple[Source, ...]:
    """config.toml's sources, each with a new ID, enabled.

    They compile back to the same SourceConfig values, apart from accounts
    that config.toml listed twice or left blank: a source lists each once,
    and leaves blanks out.
    """
    return tuple(_from_config(config, new_id()) for config in settings.sources)


def _from_config(config: SourceConfig, source_id: uuid.UUID) -> Source:
    def own_feeds(adapter: SourceAdapterConfig) -> tuple[Target, ...] | None:
        if (adapter.kind is not SourceAdapter.RSS
                or adapter.feed_urls == config.feed_urls):
            return None

        return _new_targets(adapter.feed_urls)

    adapters = tuple(
        Adapter(adapter.kind, own_feeds(adapter))
        for adapter in config.adapters
    )

    return Source(
        id=source_id,
        enabled=True,
        domain=config.domain,
        # config.toml doesn't strip accounts, or drop blanks and repeats.
        accounts=_new_targets(dict.fromkeys(
            account.strip() for account in config.accounts if account.strip()
        )),
        feeds=_new_targets(config.feed_urls),
        tags=config.tags,
        adapters=adapters or None,
        reddit=config.reddit,
        hacker_news=config.hacker_news,
    )


def _new_targets(values: Iterable[str]) -> tuple[Target, ...]:
    return tuple(Target(value) for value in values)


class StoredSourceError(ValueError):
    """A stored source this code can't read. A person needs to look."""


_CONFIG_FIELDS = frozenset({
    "domain", "accounts", "feeds", "tags", "adapters", "reddit",
    "hacker_news",
})
_TARGET_FIELDS = frozenset({"value", "enabled"})
_ADAPTER_FIELDS = frozenset({"kind", "feeds"})
_REDDIT_FIELDS = frozenset({"sort", "time_filter"})
_HACKER_NEWS_FIELDS = frozenset({"filter"})


def encode_config(source: Source | NewSource) -> dict[str, Any]:
    """Everything but the ID and enabled flag, as JSON."""
    return {
        "domain": source.domain,
        "accounts": _encode_targets(source.accounts),
        "feeds": _encode_targets(source.feeds),
        "tags": list(source.tags),
        "adapters": None if source.adapters is None else [
            {
                "kind": adapter.kind.value,
                "feeds": (
                    None if adapter.feeds is None
                    else _encode_targets(adapter.feeds)
                ),
            }
            for adapter in source.adapters
        ],
        "reddit": None if source.reddit is None else {
            "sort": source.reddit.sort.value,
            "time_filter": source.reddit.time_filter.value,
        },
        "hacker_news": None if source.hacker_news is None else {
            "filter": source.hacker_news.filter.value,
        },
    }


def _encode_targets(targets: tuple[Target, ...]) -> list[dict[str, Any]]:
    return [
        {"value": target.value, "enabled": target.enabled}
        for target in targets
    ]


def decode_source(
    source_id: uuid.UUID, document: object, *, enabled: bool,
) -> Source:
    """A stored source, read as strictly as it was written.

    Raises StoredSourceError for anything encode_config wouldn't write, so a
    row in an old shape or edited by hand fails loudly.
    """
    try:
        fields = _exactly(document, _CONFIG_FIELDS)
        adapters = fields["adapters"]
        reddit = fields["reddit"]
        hacker_news = fields["hacker_news"]

        return Source(
            id=source_id,
            enabled=enabled,
            domain=_string(fields["domain"]),
            accounts=_decode_targets(fields["accounts"]),
            feeds=_decode_targets(fields["feeds"]),
            tags=tuple(_string(tag) for tag in _list(fields["tags"])),
            adapters=None if adapters is None else tuple(
                _decode_adapter(adapter) for adapter in _list(adapters)
            ),
            reddit=None if reddit is None else _decode_reddit(reddit),
            hacker_news=(
                None if hacker_news is None
                else _decode_hacker_news(hacker_news)
            ),
        )
    except (TypeError, ValueError) as error:
        raise StoredSourceError(
            f"Stored source {source_id} can't be read: {error}",
        ) from error


def _exactly(value: object, keys: frozenset[str]) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected an object")

    fields = cast(dict[str, object], value)

    if fields.keys() != keys:
        raise TypeError(f"expected the keys {', '.join(sorted(keys))}")

    return fields


def _string(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("expected text")

    return value


def _list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("expected a list")

    return cast(list[object], value)


def _decode_targets(value: object) -> tuple[Target, ...]:
    targets: list[Target] = []

    for item in _list(value):
        fields = _exactly(item, _TARGET_FIELDS)
        enabled = fields["enabled"]

        if not isinstance(enabled, bool):
            raise TypeError("expected enabled to be true or false")

        targets.append(Target(_string(fields["value"]), enabled))

    return tuple(targets)


def _decode_adapter(value: object) -> Adapter:
    fields = _exactly(value, _ADAPTER_FIELDS)
    feeds = fields["feeds"]

    return Adapter(
        SourceAdapter(_string(fields["kind"])),
        None if feeds is None else _decode_targets(feeds),
    )


def _decode_reddit(value: object) -> RedditSourceConfig:
    fields = _exactly(value, _REDDIT_FIELDS)

    return RedditSourceConfig(
        RedditSort(_string(fields["sort"])),
        RedditTimeFilter(_string(fields["time_filter"])),
    )


def _decode_hacker_news(value: object) -> HackerNewsSourceConfig:
    fields = _exactly(value, _HACKER_NEWS_FIELDS)

    return HackerNewsSourceConfig(HackerNewsFilter(_string(fields["filter"])))
