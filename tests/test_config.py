import pathlib

import pytest

from dank.config import ConfigError, SourceConfig, load_settings


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
