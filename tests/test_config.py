import pathlib

import pytest

from dank.config import (
    ConfigError,
    MediaSizeLimits,
    SourceConfig,
    load_settings,
)


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ("", False),
        ("[rss]\nfeed_staleness_days = 7\n", False),
        ("[rss]\nkeep_feed_on_fetch_failure = false\n", False),
        ("[rss]\nkeep_feed_on_fetch_failure = true\n", True),
    ],
)
def test_load_feed_failure_retention_setting(
    *,
    tmp_path: pathlib.Path,
    config: str,
    expected: bool,
) -> None:
    config_path = tmp_path / "test-settings.toml"
    config_path.write_text(config)

    settings = load_settings(config_path)

    assert settings.keep_feed_on_fetch_failure is expected


@pytest.mark.parametrize("value", ['"false"', "1"])
def test_feed_failure_retention_requires_boolean(
    tmp_path: pathlib.Path,
    value: str,
) -> None:
    config_path = tmp_path / "test-settings.toml"
    config_path.write_text(f"[rss]\nkeep_feed_on_fetch_failure = {value}\n")

    with pytest.raises(ConfigError, match="must be a boolean"):
        load_settings(config_path)


def test_domain_only_sources_keep_existing_defaults(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = ["blog.codinghorror.com", "nichegamer.com", '
        '"order-order.com"]\n',
    )
    settings = load_settings(path)
    assert settings.sources == tuple(
        SourceConfig(domain, ()) for domain in (
            "blog.codinghorror.com", "nichegamer.com", "order-order.com",
        )
    )
    assert settings.max_entries_per_feed == 0


def test_sources_can_mix_legacy_accounts_tags_and_explicit_feeds(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [\n'
        '  "old.test",\n'
        '  { domain = "Tagged.Test ", tags = [" DDD ", "ddd", '
        '"Architecture"] },\n'
        '  { domain = "feeds.test", feed_urls = ['
        '" https://provider.test/rss ", "https://provider.test/rss", '
        '"http://provider.test/atom"], tags = ["Web"] },\n'
        '  { domain = "x.com", accounts = ["person"], tags = ["News"] },\n'
        ']\n[rss]\nmax_entries_per_feed = 20\n',
    )
    settings = load_settings(path)
    assert settings.sources == (
        SourceConfig("old.test", ()),
        SourceConfig("tagged.test", (), (), ("ddd", "architecture")),
        SourceConfig("feeds.test", (), (
            "https://provider.test/rss", "http://provider.test/atom",
        ), ("web",)),
        SourceConfig("x.com", ("person",), (), ("news",)),
    )
    assert settings.max_entries_per_feed == 20


@pytest.mark.parametrize("field, value", [
    ("tags", '"ddd"'), ("tags", '[" "]'), ("tags", '[1]'),
    ("feed_urls", '"https://example.test/feed"'),
    ("feed_urls", '[""]'), ("feed_urls", '[true]'),
    ("feed_urls", '["ftp://example.test/feed"]'),
    ("feed_urls", '["/feed.xml"]'),
    ("feed_urls", '["https:///feed.xml"]'),
    ("feed_urls", '["https://example.test:bad/feed"]'),
    ("feed_urls", '["https://example.test/feed path"]'),
])
def test_invalid_source_options_are_rejected(
    tmp_path: pathlib.Path, field: str, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        f'sources = [{{ domain = "example.test", {field} = {value} }}]',
    )

    with pytest.raises(ConfigError, match=field):
        load_settings(path)


def test_empty_optional_arrays_use_automatic_discovery(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{ domain = "example.test", tags = [], feed_urls = [] }]',
    )
    assert load_settings(path).sources == (SourceConfig("example.test", ()),)


@pytest.mark.parametrize("value", [
    "-1", '"20"', "1.5", "true", "false", "18446744073709551616",
])
def test_entry_limit_requires_a_nonnegative_integer(
    tmp_path: pathlib.Path, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[rss]\nmax_entries_per_feed = {value}\n")

    with pytest.raises(ConfigError, match="max_entries_per_feed"):
        load_settings(path)


@pytest.mark.parametrize("value", [0, 1, 20])
def test_explicit_entry_limit(tmp_path: pathlib.Path, value: int) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[rss]\nmax_entries_per_feed = {value}\n")
    assert load_settings(path).max_entries_per_feed == value


def test_x_cannot_silently_ignore_explicit_rss_feeds(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{ domain = "x.com", '
        'feed_urls = ["https://example.test/rss"] }]',
    )

    with pytest.raises(ConfigError, match=r"not supported for x\.com"):
        load_settings(path)


@pytest.mark.parametrize("field", [
    "source_concurrency", "http_concurrency", "http_per_host",
    "media_concurrency", "queue_batches",
])
@pytest.mark.parametrize("value", [
    "0", "-1", "true", "1.5", '"4"', "4294967296",
])
def test_invalid_scrape_limits(
    tmp_path: pathlib.Path, field: str, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[scrape]\n{field} = {value}\n")

    with pytest.raises(ConfigError, match=f"scrape.{field}"):
        load_settings(path)


def test_scrape_limits_are_optional_and_configurable(tmp_path: pathlib.Path):
    path = tmp_path / "settings.fixture.toml"
    path.write_text("")
    limits = load_settings(path).scrape
    assert tuple(limits) == (4, 16, 2, 4, 2)
    path.write_text("[scrape]\nsource_concurrency = 1\nhttp_per_host = 1\n")
    changed = load_settings(path).scrape
    assert tuple(changed) == (1, 16, 1, 4, 2)


@pytest.mark.parametrize("text, expected", [
    ("", ("image", "audio", "video")),
    ("[media]", ("image", "audio", "video")),
    ('[media]\ndownload_types = ["image"]', ("image",)),
    ('[media]\ndownload_types = [" Video ", "IMAGE", "video"]',
     ("video", "image")),
    ('[media]\ndownload_types = []', ()),
])
def test_media_download_types_configuration(
    tmp_path: pathlib.Path, text: str, expected: tuple[str, ...],
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(text)
    assert load_settings(path).media_download_types == expected


@pytest.mark.parametrize("text", [
    'media = false', 'media = ["image"]',
    '[media]\ndownload_types = "image"',
    '[media]\ndownload_types = false',
    '[media]\ndownload_types = [1]',
    '[media]\ndownload_types = ["image", true]',
    '[media]\ndownload_types = ["image", "youtube"]',
    '[media]\ndownload_types = [" "]',
    '[media]\ndownload_types = ["images"]',
])
def test_invalid_media_download_types_are_rejected(
    tmp_path: pathlib.Path, text: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(text)

    with pytest.raises(ConfigError, match="media"):
        load_settings(path)


@pytest.mark.parametrize("text", ["", "[media]", "[media.max_bytes]"])
def test_media_size_limits_are_empty_unless_set(
    tmp_path: pathlib.Path, text: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(text)

    assert load_settings(path).media_size_limits == MediaSizeLimits()


@pytest.mark.parametrize("text, expected", [
    ("[media.max_bytes]\nvideo = 100",
     MediaSizeLimits(kinds={"video": 100})),
    ("[media.max_bytes]\nVideo = 100",
     MediaSizeLimits(kinds={"video": 100})),
    ('[media.max_bytes."youtube.com"]\nvideo = 200',
     MediaSizeLimits(sites={"youtube.com": {"video": 200}})),
    # Zero or less means no limit, as it does for storage.max_asset_bytes.
    ("[media.max_bytes]\nvideo = 0\naudio = -1",
     MediaSizeLimits(kinds={"video": None, "audio": None})),
    ('[media.max_bytes."x.com"]\nvideo = 0',
     MediaSizeLimits(sites={"x.com": {"video": None}})),
    # Sites are named as a person would write them.
    ('[media.max_bytes." WWW.YouTube.com "]\nvideo = 200',
     MediaSizeLimits(sites={"youtube.com": {"video": 200}})),
    ('[media.max_bytes."twitter.com"]\nvideo = 200',
     MediaSizeLimits(sites={"x.com": {"video": 200}})),
    ('[media.max_bytes]\nimage = 1\n[media.max_bytes."x.com"]\nimage = 2',
     MediaSizeLimits(kinds={"image": 1}, sites={"x.com": {"image": 2}})),
])
def test_media_size_limits_configuration(
    tmp_path: pathlib.Path, text: str, expected: MediaSizeLimits,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(text)

    assert load_settings(path).media_size_limits == expected


@pytest.mark.parametrize("text", [
    "[media]\nmax_bytes = 5",
    '[media]\nmax_bytes = "big"',
    '[media.max_bytes]\nvideo = "big"',
    "[media.max_bytes]\nvideo = true",
    "[media.max_bytes]\nvideo = 1.5",
    # A typo must not silently leave a limit unset.
    "[media.max_bytes]\nvidoe = 100",
    '[media.max_bytes."youtube.com"]\nvidoe = 100',
    '[media.max_bytes."youtube.com"]\nvideo = "big"',
    '[media.max_bytes."youtube.com"]\nvideo = false',
    '[media.max_bytes." "]\nvideo = 100',
])
def test_invalid_media_size_limits_are_rejected(
    tmp_path: pathlib.Path, text: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(text)

    with pytest.raises(ConfigError, match=r"media\.max_bytes"):
        load_settings(path)


LIMITS = MediaSizeLimits(
    kinds={"video": 8, "image": None},
    sites={"youtube.com": {"video": 10, "image": 5, "audio": None}},
)


@pytest.mark.parametrize(("site", "kind", "default", "expected"), [
    # The site wins over the type, and the type over the default.
    ("youtube.com", "video", 6, 10),
    ("x.com", "video", 6, 8),
    ("x.com", "audio", 6, 6),
    # More specific can mean lower as well as higher.
    ("youtube.com", "video", 100, 10),
    ("youtube.com", "image", 6, 5),
    # "No limit" is a choice, which has to beat a default.
    ("x.com", "image", 6, None),
    ("youtube.com", "audio", 6, None),
    # And it's distinct from having no opinion.
    ("x.com", "audio", None, None),
    ("youtube.com", "image", None, 5),
    # A kind with no rule of its own takes the default.
    ("youtube.com", "file", 6, 6),
    # However the site's domain was saved.
    ("www.youtube.com", "video", 6, 10),
    ("YouTube.com", "video", 6, 10),
])
def test_the_most_specific_media_limit_wins(
    site: str, kind: str, default: int | None, expected: int | None,
) -> None:
    assert LIMITS.for_file(site, kind, default) == expected


def test_no_media_limits_leave_the_default_alone() -> None:
    assert MediaSizeLimits().for_file("x.com", "video", 6) == 6
    assert MediaSizeLimits().for_file("x.com", "video", None) is None


@pytest.mark.parametrize("site", ["twitter.com", "www.twitter.com", "X.com"])
def test_x_keeps_its_limit_under_either_name(site: str) -> None:
    limits = MediaSizeLimits(sites={"x.com": {"video": 3}})

    assert limits.for_file(site, "video", 6) == 3


@pytest.mark.parametrize("name", [
    "config.example.toml", "config.native.example.toml",
])
def test_the_example_configs_set_no_media_size_limits(name: str) -> None:
    root = pathlib.Path(__file__).parents[1]

    assert load_settings(root / name).media_size_limits == MediaSizeLimits()


@pytest.mark.parametrize(("field", "value"), [
    ("enabled", '"true"'), ("enabled", "1"),
    ("max_posts_per_account", "0"), ("max_posts_per_account", "101"),
    ("max_posts_per_account", "true"), ("max_replies", "0"),
    ("max_replies", "201"), ("max_replies", '"50"'),
    ("max_replies", "50.5"), ("max_scrolls", "-1"), ("max_scrolls", "21"),
    ("max_scrolls", "false"), ("timeout_seconds", "0"),
    ("timeout_seconds", "121"), ("timeout_seconds", "nan"),
    ("timeout_seconds", "inf"), ("timeout_seconds", "true"),
])
def test_invalid_x_conversation_settings(
    tmp_path: pathlib.Path, field: str, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[x.conversation]\n{field} = {value}\n")

    with pytest.raises(ConfigError, match=f"x.conversation.{field}"):
        load_settings(path)


def test_x_conversation_defaults_and_explicit_limits(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("")
    assert tuple(load_settings(path).x.conversation) == (True, 5, 50, 3, 30.0)
    path.write_text(
        "[x.conversation]\nenabled = false\nmax_posts_per_account = 2\n"
        "max_replies = 20\nmax_scrolls = 0\ntimeout_seconds = 10.5\n",
    )
    assert tuple(load_settings(path).x.conversation) == (False, 2, 20, 0, 10.5)


def test_x_conversation_requires_table(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("[x]\nconversation = false\n")

    with pytest.raises(ConfigError, match=r"x\.conversation"):
        load_settings(path)
