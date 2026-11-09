"""Sources' pure rules: normalizing, validating, compiling and encoding."""

import pathlib
import uuid
from collections.abc import Iterable, Iterator

import pytest

from dank.config import (
    HackerNewsFilter,
    HackerNewsSourceConfig,
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
    canonical_source_domain,
    load_settings,
)
from dank.sources.model import (
    MAX_ADAPTERS,
    MAX_TARGETS,
    MAX_TEXT,
    Adapter,
    InvalidSource,
    NewSource,
    Source,
    StoredSourceError,
    Target,
    compile_sources,
    decode_source,
    encode_config,
    normalized,
    sources_from_settings,
)

ID = uuid.UUID("0d6b2f3c-0000-4000-8000-000000000001")
REDDIT = RedditSourceConfig(RedditSort.TOP, RedditTimeFilter.DAY)
HACKER_NEWS = HackerNewsSourceConfig(HackerNewsFilter.TOP50)


def targets(values: Iterable[str | Target]) -> tuple[Target, ...]:
    return tuple(
        value if isinstance(value, Target) else Target(value)
        for value in values
    )


def source(
    domain: str,
    *,
    accounts: Iterable[str | Target] = (),
    feeds: Iterable[str | Target] = (),
    tags: Iterable[str] = (),
    adapters: tuple[Adapter, ...] | None = None,
    reddit: RedditSourceConfig | None = None,
    hacker_news: HackerNewsSourceConfig | None = None,
    enabled: bool = True,
) -> Source:
    return Source(
        ID, enabled, domain, targets(accounts), targets(feeds), tuple(tags),
        adapters, reddit, hacker_news,
    )


def problems(value: Source) -> list[tuple[str, str]]:
    with pytest.raises(InvalidSource) as caught:
        normalized(value)

    return [(problem.path, problem.code) for problem in caught.value.fields]


def ids() -> Iterator[uuid.UUID]:
    return iter(uuid.UUID(int=n) for n in range(1, 10_000))


@pytest.mark.parametrize(("raw", "domain"), [
    (" Example.COM ", "example.com"),
    ("https://hckrnews.com/news", "news.ycombinator.com"),
    ("news.ycombinator.com", "news.ycombinator.com"),
    ("WWW.Reddit.com/", "reddit.com"),
    ("https://www.youtube.com", "youtube.com"),
    ("blog.codinghorror.com", "blog.codinghorror.com"),
])
def test_known_aliases_fold_to_one_domain(raw: str, domain: str) -> None:
    assert canonical_source_domain(raw) == domain


def test_sources_are_normalized_as_toml_normalizes_them() -> None:
    reddit = normalized(source(
        " Reddit.com ",
        feeds=["r/LocalLLaMA", "https://old.reddit.com/r/programming"],
        tags=[" AI ", "ai", "News"],
        reddit=REDDIT,
    ))
    youtube = normalized(source(
        "youtube.com",
        accounts=[" @Channel4News "],
        feeds=["https://youtube.com/@Channel4News"],
    ))

    assert reddit == source(
        "reddit.com",
        feeds=[
            "https://www.reddit.com/r/LocalLLaMA/",
            "https://www.reddit.com/r/programming/",
        ],
        tags=["ai", "news"],
        reddit=REDDIT,
    )
    assert youtube == source(
        "youtube.com",
        accounts=["@Channel4News"],
        feeds=["https://www.youtube.com/@Channel4News/videos"],
    )


def test_normalizing_keeps_enabled_flags_and_works_without_an_id() -> None:
    new = NewSource(
        enabled=False,
        domain="x.com",
        accounts=targets([Target("Example", enabled=False)]),
    )

    assert normalized(new) == new


@pytest.mark.parametrize("domain", [
    "https://example.com", "example.com/blog", "", "two words.com", "a@b.com",
])
def test_a_domain_is_a_host_name(domain: str) -> None:
    assert problems(source(domain)) == [("domain", "invalid_domain")]


@pytest.mark.parametrize(("value", "path"), [
    (source("reddit.com", feeds=["programming"], reddit=REDDIT),
     "feeds.0.value"),
    (source("youtube.com", feeds=["https://example.com/@channel"]),
     "feeds.0.value"),
    (source("example.com", feeds=["ftp://example.com/feed"]),
     "feeds.0.value"),
    (source("example.com", feeds=["https://example.com/a feed"]),
     "feeds.0.value"),
    (source(
        "news.ycombinator.com", feeds=["https://example.com/feed", "nope"],
        adapters=(Adapter(SourceAdapter.RSS),), hacker_news=HACKER_NEWS,
    ), "feeds.1.value"),
    (source("example.com", adapters=(
        Adapter(SourceAdapter.RSS, targets(["not a url"])),
    )), "adapters.0.feeds.0.value"),
])
def test_feeds_must_be_urls_the_platform_collects(
    value: Source, path: str,
) -> None:
    assert problems(value) == [(path, "invalid_url")]


@pytest.mark.parametrize(("value", "path"), [
    (source("x.com", feeds=["https://x.com/feed"]), "feeds"),
    (source("reddit.com", accounts=["someone"], feeds=["r/programming"],
            reddit=REDDIT), "accounts"),
    (source("reddit.com", reddit=REDDIT), "feeds"),
    (source("reddit.com", feeds=["r/programming"]), "reddit"),
    (source("example.com", reddit=REDDIT), "reddit"),
    (source("news.ycombinator.com", accounts=["pg"],
            hacker_news=HACKER_NEWS), "accounts"),
    (source("news.ycombinator.com"), "hacker_news"),
    (source("example.com", hacker_news=HACKER_NEWS), "hacker_news"),
])
def test_each_platform_takes_only_its_own_options(
    value: Source, path: str,
) -> None:
    assert problems(value) == [(path, "invalid_platform_options")]


@pytest.mark.parametrize(("value", "path"), [
    (source("x.com", adapters=(Adapter(SourceAdapter.RSS),)),
     "adapters.0.kind"),
    (source("youtube.com", adapters=(Adapter(SourceAdapter.RSS),)),
     "adapters.0.kind"),
    (source("example.com", adapters=(Adapter(SourceAdapter.HACKER_NEWS),)),
     "adapters.0.kind"),
    (source("x.com", adapters=(Adapter(SourceAdapter.X, targets(["a"])),)),
     "adapters.0.feeds"),
    (source("example.com", adapters=()), "adapters"),
    (source("example.com", adapters=(
        Adapter(SourceAdapter.RSS), Adapter(SourceAdapter.RSS),
    )), "adapters.1"),
    (source("news.ycombinator.com", feeds=["https://example.com/feed"],
            hacker_news=HACKER_NEWS), "adapters"),
])
def test_adapters_follow_the_toml_rules(value: Source, path: str) -> None:
    assert problems(value) == [(path, "unsupported_adapter")]


@pytest.mark.parametrize("value", [
    # The source's feeds, and the same feeds again as the adapter's own.
    source("example.com", feeds=["https://example.com/a"], adapters=(
        Adapter(SourceAdapter.RSS),
        Adapter(SourceAdapter.RSS, targets(["https://example.com/a"])),
    )),
    # No feeds of its own, and the source's, which are none.
    source("example.com", adapters=(
        Adapter(SourceAdapter.RSS, ()), Adapter(SourceAdapter.RSS),
    )),
])
def test_two_adapters_that_collect_the_same_feeds_are_duplicates(
    value: Source,
) -> None:
    # config.toml compares adapters by the feeds they end up with.
    assert problems(value) == [("adapters.1", "unsupported_adapter")]


def test_too_many_adapters() -> None:
    value = source("example.com", adapters=tuple(
        Adapter(SourceAdapter.RSS, targets([f"https://example.com/{n}"]))
        for n in range(MAX_ADAPTERS + 1)
    ))

    assert problems(value) == [("adapters", "too_many")]


def test_values_are_measured_after_normalizing() -> None:
    # Normalizing adds a host and /videos, and lowercasing İ doubles it.
    handle = "@" + "a" * (MAX_TEXT - len("https://youtube.com/@"))
    value = source(
        "youtube.com",
        feeds=[f"https://youtube.com/{handle}"],
        tags=["\u0130" * (MAX_TEXT // 2 + 1)],
    )

    assert problems(value) == [
        ("feeds.0.value", "too_long"),
        ("tags.0", "too_long"),
    ]


def test_rss_adapters_can_combine_with_hacker_news() -> None:
    value = source(
        "news.ycombinator.com",
        feeds=["https://example.com/feed"],
        adapters=(
            Adapter(SourceAdapter.HACKER_NEWS), Adapter(SourceAdapter.RSS),
        ),
        hacker_news=HACKER_NEWS,
    )

    assert normalized(value) == value


def test_a_target_appears_once_in_its_list() -> None:
    value = source(
        "reddit.com",
        feeds=["r/LocalLLaMA", "https://old.reddit.com/r/localllama/"],
        reddit=REDDIT,
    )
    accounts = source("x.com", accounts=["a", " a "])

    assert problems(value) == [("feeds.1.value", "duplicate_target")]
    assert problems(accounts) == [("accounts.1.value", "duplicate_target")]


def test_text_without_control_characters_or_blanks() -> None:
    value = source(
        "example.com",
        accounts=["a\x00b", "  "],
        tags=["ok", "bad\ttag"],
    )

    assert problems(value) == [
        ("accounts.0.value", "invalid_text"),
        ("accounts.1.value", "empty_value"),
        ("tags.1", "invalid_text"),
    ]


def test_every_problem_is_reported_at_once() -> None:
    value = source(
        "reddit.com", accounts=["someone"], feeds=["nope"], reddit=REDDIT,
    )

    assert problems(value) == [
        ("accounts", "invalid_platform_options"),
        ("feeds.0.value", "invalid_url"),
    ]


def test_lists_have_a_size_limit() -> None:
    value = source(
        "x.com", accounts=[f"a{n}" for n in range(MAX_TARGETS + 1)],
    )

    assert problems(value) == [("accounts", "too_many")]


def test_problems_carry_a_message_for_people() -> None:
    with pytest.raises(InvalidSource) as caught:
        normalized(source("reddit.com", feeds=["nope"], reddit=REDDIT))

    assert caught.value.code == "invalid_source"
    assert [problem.message for problem in caught.value.fields] == [
        "Use r/community or a Reddit community URL.",
    ]


def test_adapters_left_the_same_by_disabled_feeds_collect_once() -> None:
    value = source("example.com", adapters=(
        Adapter(SourceAdapter.RSS, targets([
            "https://example.com/a",
            Target("https://example.com/b", enabled=False),
        ])),
        Adapter(SourceAdapter.RSS, targets(["https://example.com/a"])),
    ))

    assert compile_sources([value]).collectable == (
        SourceConfig("example.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.RSS, ("https://example.com/a",)),
        )),
    )


def test_known_sources_keep_everything_disabled() -> None:
    sources = (
        source("off.example", enabled=False),
        source("x.com", accounts=[Target("a", enabled=False), "b"]),
    )

    compiled = compile_sources(sources)

    assert compiled.known == (
        SourceConfig("off.example", ()),
        SourceConfig("x.com", ("a", "b")),
    )
    assert compiled.collectable == (SourceConfig("x.com", ("b",)),)


@pytest.mark.parametrize("value", [
    # For You only, if X's accounts compiled to nothing.
    source("x.com", accounts=[Target("a", enabled=False)]),
    # Feed discovery, if a publisher's feeds did.
    source("example.com", feeds=[
        Target("https://example.com/feed", enabled=False),
    ]),
    # YouTube's global feeds, if both its lists did.
    source("youtube.com", accounts=[Target("@a", enabled=False)], feeds=[
        Target("https://www.youtube.com/@b/videos", enabled=False),
    ]),
    # Hacker News' front page, if its only RSS adapter went.
    source(
        "news.ycombinator.com",
        feeds=[Target("https://example.com/feed", enabled=False)],
        adapters=(Adapter(SourceAdapter.RSS),),
        hacker_news=HACKER_NEWS,
    ),
    source("example.com", adapters=(Adapter(SourceAdapter.RSS, targets([
        Target("https://example.com/feed", enabled=False),
    ])),)),
])
def test_disabling_every_target_never_broadens_collection(
    value: Source,
) -> None:
    assert compile_sources([value]).collectable == ()


def test_sources_without_targets_still_collect_as_before() -> None:
    sources = (source("x.com"), source("example.com"), source("youtube.com"))

    assert compile_sources(sources).collectable == (
        SourceConfig("x.com", ()),
        SourceConfig("example.com", ()),
        SourceConfig("youtube.com", ()),
    )


def test_disabling_some_targets_keeps_the_rest() -> None:
    youtube = source(
        "youtube.com",
        accounts=[Target("@a", enabled=False)],
        feeds=["https://www.youtube.com/@b/videos"],
    )
    reddit = source(
        "reddit.com",
        feeds=[
            Target("https://www.reddit.com/r/a/", enabled=False),
            "https://www.reddit.com/r/b/",
        ],
        reddit=REDDIT,
    )
    hacker_news = source(
        "news.ycombinator.com",
        feeds=[Target("https://example.com/feed", enabled=False)],
        adapters=(
            Adapter(SourceAdapter.HACKER_NEWS), Adapter(SourceAdapter.RSS),
        ),
        hacker_news=HACKER_NEWS,
    )

    assert compile_sources([youtube, reddit, hacker_news]).collectable == (
        SourceConfig(
            "youtube.com", (), ("https://www.youtube.com/@b/videos",),
        ),
        SourceConfig(
            "reddit.com", (), ("https://www.reddit.com/r/b/",), reddit=REDDIT,
        ),
        SourceConfig(
            "news.ycombinator.com", (), hacker_news=HACKER_NEWS,
            adapters=(SourceAdapterConfig(SourceAdapter.HACKER_NEWS),),
        ),
    )


PARITY = [
    'sources = ["blog.codinghorror.com", "Example.COM"]',
    'sources = [{ domain = "x.com", accounts = ["example", "other"] }]',
    'sources = [{ domain = "x.com" }]',
    '''[[sources]]
    domain = "www.reddit.com"
    feed_urls = ["r/LocalLLaMA", "https://old.reddit.com/r/programming"]
    reddit_sort = "new"
    reddit_time_filter = "week"''',
    '''[[sources]]
    domain = "youtube.com"
    accounts = ["@Channel4News"]
    feed_urls = [
        "https://youtube.com/@BBCNews", "https://www.youtube.com/feed/hype",
    ]''',
    'sources = [{ domain = "hckrnews.com", hacker_news_filter = "top10" }]',
    '''[[sources]]
    domain = "news.ycombinator.com"
    feed_urls = ["https://example.com/feed"]''',
    '''[[sources]]
    domain = "news.ycombinator.com"
    hacker_news_filter = "top20"
    feed_urls = ["https://example.com/feed"]
    adapters = [{ type = "hacker_news" }, { type = "rss" }]''',
    '''sources = [{ domain = "example.com", adapters = [
        { type = "rss", feed_urls = ["https://example.com/a.xml"] },
        { type = "rss", feed_urls = ["https://example.com/b.xml"] } ] }]''',
    '''[[sources]]
    domain = "example.com"
    feed_urls = ["https://example.com/feed"]
    adapters = [
        { type = "rss" },
        { type = "rss", feed_urls = ["https://example.com/other.xml"] },
    ]''',
    '''sources = [
        { domain = "example.com", tags = ["News", "news", "ai"] },
        { domain = "example.com", feed_urls = ["https://example.com/feed"] },
    ]''',
]


@pytest.mark.parametrize("toml", PARITY)
def test_imported_sources_collect_exactly_what_toml_does(
    tmp_path: pathlib.Path, toml: str,
) -> None:
    config = tmp_path / "config.toml"
    config.write_text(toml)
    settings = load_settings(config)

    imported = sources_from_settings(settings, ids().__next__)

    assert [normalized(value) for value in imported] == list(imported)
    assert compile_sources(imported).collectable == settings.sources


def test_blank_accounts_are_left_out_of_an_import(
    tmp_path: pathlib.Path,
) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        'sources = [{ domain = "x.com", accounts = ["", " a ", "a"] }]',
    )

    [imported] = sources_from_settings(load_settings(config), ids().__next__)

    assert imported.accounts == (Target("a"),)


def test_imported_sources_get_new_ids_and_start_enabled(
    tmp_path: pathlib.Path,
) -> None:
    config = tmp_path / "config.toml"
    config.write_text('sources = ["a.example", "b.example"]')

    imported = sources_from_settings(load_settings(config), ids().__next__)

    assert [(value.id.int, value.enabled) for value in imported] == [
        (1, True), (2, True),
    ]


def test_the_stored_document_round_trips_every_field() -> None:
    value = source(
        "news.ycombinator.com",
        feeds=[Target("https://example.com/feed", enabled=False)],
        tags=["ai"],
        adapters=(
            Adapter(SourceAdapter.HACKER_NEWS),
            Adapter(SourceAdapter.RSS),
            Adapter(SourceAdapter.RSS, targets(["https://example.com/b"])),
        ),
        hacker_news=HACKER_NEWS,
        enabled=False,
    )
    reddit = source("reddit.com", feeds=["https://www.reddit.com/r/a/"],
                    reddit=REDDIT)

    for original in (value, reddit):
        document = encode_config(original)

        assert decode_source(
            ID, document, enabled=original.enabled,
        ) == original
        assert "id" not in document and "enabled" not in document


def test_the_stored_document_is_plain_json() -> None:
    document = encode_config(source(
        "reddit.com", feeds=["https://www.reddit.com/r/a/"], reddit=REDDIT,
    ))

    assert document == {
        "domain": "reddit.com",
        "accounts": [],
        "feeds": [{"value": "https://www.reddit.com/r/a/", "enabled": True}],
        "tags": [],
        "adapters": None,
        "reddit": {"sort": "top", "time_filter": "day"},
        "hacker_news": None,
    }


@pytest.mark.parametrize("change", [
    {"surprise": 1},
    {"domain": 3},
    {"accounts": [{"value": "a"}]},
    {"feeds": [{"value": "a", "enabled": "yes"}]},
    {"tags": ["ok", 2]},
    {"adapters": [{"kind": "carrier pigeon", "feeds": None}]},
    {"reddit": {"sort": "top"}},
    {"hacker_news": {"filter": "top7"}},
])
def test_decoding_refuses_anything_it_did_not_write(
    change: dict[str, object],
) -> None:
    document = {**encode_config(source("example.com")), **change}

    with pytest.raises(StoredSourceError):
        decode_source(ID, document, enabled=True)


def test_decoding_refuses_a_missing_field() -> None:
    document = encode_config(source("example.com"))
    del document["tags"]

    with pytest.raises(StoredSourceError):
        decode_source(ID, document, enabled=True)
