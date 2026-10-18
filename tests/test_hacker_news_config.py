import pathlib

import pytest

from dank.config import (
    ConfigError,
    HackerNewsFilter,
    HackerNewsSettings,
    HackerNewsSourceConfig,
    load_settings,
)


@pytest.mark.parametrize("domain", [
    "hckrnews.com", "https://hckrnews.com/", "http://www.hckrnews.com/",
    "news.ycombinator.com", "https://news.ycombinator.com/news",
])
def test_hacker_news_aliases_and_defaults(tmp_path: pathlib.Path, domain: str):
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f'sources = ["{domain}"]')
    settings = load_settings(path)
    assert settings.sources[0].domain == "news.ycombinator.com"
    assert settings.sources[0].hacker_news == HackerNewsSourceConfig()
    assert settings.hacker_news == HackerNewsSettings()


@pytest.mark.parametrize("ranking", list(HackerNewsFilter))
def test_hacker_news_configurable_ranking(
    tmp_path: pathlib.Path, ranking: str,
):
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{ domain = "hckrnews.com", '
        f'hacker_news_filter = "{ranking}", tags = ["Tech"] }}]',
    )
    source = load_settings(path).sources[0]
    assert source.hacker_news == HackerNewsSourceConfig(
        HackerNewsFilter(ranking),
    )
    assert source.tags == ("tech",)


@pytest.mark.parametrize("source", [
    '{ domain = "hckrnews.com", hacker_news_filter = "best" }',
    '{ domain = "hckrnews.com", accounts = ["person"] }',
    '{ domain = "hckrnews.com", reddit_sort = "top" }',
    '{ domain = "example.test", hacker_news_filter = "top10" }',
    '{ domain = "reddit.com", feed_urls = ["r/python"], '
    'hacker_news_filter = "all" }',
])
def test_hacker_news_rejects_inappropriate_source_options(
    tmp_path: pathlib.Path, source: str,
):
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"sources = [{source}]")

    with pytest.raises(ConfigError):
        load_settings(path)


@pytest.mark.parametrize("field,value", [
    ("days", "0"), ("days", "91"), ("days", "true"),
    ("comment_limit", "0"), ("comment_limit", "-1"),
    ("comment_limit", "true"), ("comment_limit", "1.5"),
    ("comment_limit", '"unknown"'),
    ("detail_max_posts", "101"), ("detail_max_posts", "1.5"),
    ("detail_timeout_seconds", "0"),
    ("detail_timeout_seconds", '"unknown"'),
    ("detail_timeout_seconds", "nan"), ("detail_timeout_seconds", "true"),
])
def test_hacker_news_limits_are_bounded(
    tmp_path: pathlib.Path, field: str, value: str,
):
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[hacker_news]\n{field} = {value}")

    with pytest.raises(ConfigError, match=field):
        load_settings(path)


def test_hacker_news_limits_configurable(tmp_path: pathlib.Path):
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        "[hacker_news]\ndays = 14\ncomment_limit = 100\n"
        "detail_max_posts = 10\ndetail_timeout_seconds = 30",
    )
    assert load_settings(path).hacker_news == HackerNewsSettings(
        14, 100, 10, 30,
    )


def test_hacker_news_comments_default_to_all(tmp_path: pathlib.Path):
    path = tmp_path / "settings.fixture.toml"
    path.write_text('[hacker_news]\ncomment_limit = "all"\n'
                    'detail_timeout_seconds = "all"')
    assert load_settings(path).hacker_news == HackerNewsSettings()
    assert HackerNewsSettings().comment_limit is None
    assert HackerNewsSettings().detail_timeout_seconds is None


def test_hacker_news_explicit_large_capture_budget(tmp_path: pathlib.Path):
    path = tmp_path / "settings.fixture.toml"
    path.write_text('[hacker_news]\ncomment_limit = 2000\n'
                    'detail_timeout_seconds = 3600')
    settings = load_settings(path).hacker_news
    assert settings.comment_limit == 2000
    assert settings.detail_timeout_seconds == 3600
