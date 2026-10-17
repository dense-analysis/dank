import pathlib

import pytest

from dank.config import (
    BrowserScrapeSettings,
    ConfigError,
    RedditSettings,
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
    load_settings,
)
from dank.scrape.reddit import community_url


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
    assert source.reddit is not None
    assert source.reddit.sort is RedditSort.TOP
    assert source.reddit.time_filter is RedditTimeFilter.DAY
    assert community_url(
        "LocalLLaMA", sort=source.reddit.sort,
        time_filter=source.reddit.time_filter,
    ) == "https://www.reddit.com/r/LocalLLaMA/top/?t=day"
    assert settings.sources[0].reddit is None
    assert settings.reddit == RedditSettings()
    other = settings.sources[2].reddit
    assert other is not None
    assert other.sort is RedditSort.TOP
    assert other.time_filter is RedditTimeFilter.WEEK


@pytest.mark.parametrize("options", [
    'feed_urls = []',
    'accounts = ["LocalLLaMA"]',
    'feed_urls = ["r/python"], accounts = ["person"]',
    'feed_urls = ["https://evil.test/r/python/"]',
    'feed_urls = ["https://[bad/r/python/"]',
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
    'request_interval_seconds = 0', 'request_interval_seconds = true',
    'request_interval_seconds = nan', 'request_interval_seconds = 61',
    'detail_cache_hours = -1', 'detail_cache_hours = inf',
    'detail_cache_hours = true', 'detail_cache_hours = 169',
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
        scrape=BrowserScrapeSettings(max_posts=5, max_scrolls=0),
    )


def test_reddit_options_on_other_sources_are_rejected(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('sources = [{domain="blog.test", reddit_sort="top"}]')

    with pytest.raises(ConfigError, match="Reddit source"):
        load_settings(path)


@pytest.mark.parametrize("sort", list(RedditSort))
def test_sort_enum_values_are_normalized_at_the_toml_boundary(
    tmp_path: pathlib.Path, sort: RedditSort,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{domain="reddit.com", feed_urls=["r/python"], '
        f'reddit_sort=" {sort.value.upper()} "}}]',
    )
    options = load_settings(path).sources[0].reddit
    assert options is not None
    assert options.sort is sort
    assert options.time_filter is RedditTimeFilter.DAY
    url = community_url("python", sort=options.sort)
    suffix = f"/{sort}/?t=day" if sort is RedditSort.TOP else f"/{sort}/"
    assert url.endswith(suffix)


@pytest.mark.parametrize("window", list(RedditTimeFilter))
def test_time_window_enum_controls_the_top_listing(
    tmp_path: pathlib.Path, window: RedditTimeFilter,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{domain="reddit.com", feed_urls=["r/python"], '
        f'reddit_time_filter=" {window.value.upper()} "}}]',
    )
    options = load_settings(path).sources[0].reddit
    assert options is not None
    assert options.sort is RedditSort.TOP
    assert options.time_filter is window
    assert community_url("python", time_filter=window).endswith(f"?t={window}")


@pytest.mark.parametrize("field", ["reddit_sort", "reddit_time_filter"])
@pytest.mark.parametrize("value", ['""', '"unknown"', "true", "1", "[]", "{}"])
def test_invalid_enum_options_report_the_exact_source_field(
    tmp_path: pathlib.Path, field: str, value: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{domain="reddit.com", feed_urls=["r/python"], '
        f'{field}={value}}}]',
    )

    with pytest.raises(ConfigError, match=f"sources.{field} must be one of"):
        load_settings(path)


def test_browser_composition_preserves_flat_toml_and_platform_defaults(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("")
    settings = load_settings(path)
    assert settings.x.scrape == BrowserScrapeSettings(max_posts=200)
    assert settings.reddit.scrape == BrowserScrapeSettings(max_posts=100)
    assert RedditSourceConfig().sort is RedditSort.TOP
    path.write_text('''
[x]
max_posts = 23
max_scrolls = 7
scroll_pause_seconds = 0
[reddit]
max_posts = 12
max_scrolls = 0
scroll_pause_seconds = 2.5
stall_scrolls = 4
''')
    settings = load_settings(path)
    assert settings.x.scrape == BrowserScrapeSettings(23, 7, 0)
    assert settings.reddit == RedditSettings(
        BrowserScrapeSettings(12, 0, 2.5), 4,
    )


@pytest.mark.parametrize("option", [
    "capture_comments = 1", 'download_videos = "false"',
    "comment_limit = 0", "comment_limit = 501", "comment_limit = true",
    'comment_sort = "rising"', "comment_sort = true",
])
def test_invalid_reddit_archive_settings(
    tmp_path: pathlib.Path, option: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(f"[reddit]\n{option}\n")

    with pytest.raises(ConfigError, match="reddit"):
        load_settings(path)


def test_archive_settings_defaults_and_overrides(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("")
    defaults = load_settings(path).reddit
    assert defaults.capture_comments is True
    assert defaults.comment_limit == 50
    assert defaults.comment_sort == "best"
    assert defaults.download_videos is False
    path.write_text('''[reddit]
capture_comments = false
comment_limit = 500
comment_sort = " NEW "
download_videos = true
''')
    settings = load_settings(path).reddit
    assert settings.capture_comments is False
    assert settings.comment_limit == 500
    assert settings.comment_sort == "new"
    assert settings.download_videos is True
