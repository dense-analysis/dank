import pathlib

import pytest

from dank.config import ConfigError, RedditSettings, load_settings


def test_reddit_sources_accept_urls_and_community_shorthand(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('''
sources = [
  "blog.test",
  { domain = "https://www.reddit.com", feed_urls = [
    "r/LocalLLaMA", "https://www.reddit.com/r/localllama/", "/r/accelerate/"
  ], tags = ["AI"] },
  { domain = "reddit.com", feed_urls = [
    "r/python"], reddit_sort = "Top", reddit_time_filter = "week" }
]
''')
    settings = load_settings(path)
    source = settings.sources[1]
    assert source.domain == "reddit.com"
    assert source.feed_urls == (
        "https://www.reddit.com/r/LocalLLaMA/",
        "https://www.reddit.com/r/accelerate/",
    )
    assert source.tags == ("ai",)
    assert source.reddit_sort == "best"
    assert source.reddit_time_filter == "day"
    assert settings.reddit == RedditSettings()
    assert settings.sources[2].reddit_sort == "top"
    assert settings.sources[2].reddit_time_filter == "week"


@pytest.mark.parametrize("options", [
    'feed_urls = []',
    'accounts = ["LocalLLaMA"]',
    'feed_urls = ["r/python"], accounts = ["person"]',
    'feed_urls = ["https://evil.test/r/python/"]',
    'feed_urls = ["https://reddit.com@evil.test/r/python/"]',
    'feed_urls = ["https://www.reddit.com/r/python/comments/abc/"]',
    'feed_urls = ["https://www.reddit.com/r/python/top/?t=day"]',
    'feed_urls = ["r/python+news"]',
    'feed_urls = ["r/../python"]',
    'feed_urls = ["r/python"], reddit_sort = "quality"',
    'feed_urls = ["r/python"], reddit_time_filter = true',
])
def test_invalid_reddit_source_is_not_sent_to_rss(
    tmp_path: pathlib.Path, options: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f'sources = [{{ domain = "reddit.com", {options} }}]')

    with pytest.raises(ConfigError):
        load_settings(path)


@pytest.mark.parametrize("option", [
    'max_posts = 0', 'max_posts = true', 'max_scrolls = -1',
    'stall_scrolls = 0', 'scroll_pause_seconds = nan',
    'scroll_pause_seconds = inf', 'scroll_pause_seconds = true',
    'scroll_pause_seconds = 0', 'scroll_pause_seconds = 61',
])
def test_invalid_reddit_collection_limits(
    tmp_path: pathlib.Path, option: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[reddit]\n{option}\n")

    with pytest.raises(ConfigError, match="reddit"):
        load_settings(path)


def test_reddit_limits_can_capture_only_the_initial_page(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("[reddit]\nmax_posts = 5\nmax_scrolls = 0\n")
    assert load_settings(path).reddit == RedditSettings(
        max_posts=5, max_scrolls=0,
    )


def test_reddit_options_on_other_sources_are_rejected(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('sources = [{domain="blog.test", reddit_sort="top"}]')

    with pytest.raises(ConfigError, match="Reddit source"):
        load_settings(path)
