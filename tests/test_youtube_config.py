from pathlib import Path

import pytest

from dank.config import (
    ConfigError,
    SourceConfig,
    YouTubeSettings,
    load_settings,
)
from dank.scrape.youtube.feeds import configured_feeds


def test_youtube_public_defaults_and_channels(tmp_path: Path) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{domain="www.youtube.com", accounts=["@Channel4News"], '
        'feed_urls=["https://youtube.com/@Channel4News/videos"]}]',
    )
    settings = load_settings(path)
    assert settings.youtube == YouTubeSettings()
    assert not settings.youtube.scrape_videos
    assert settings.youtube.include_shorts
    assert not settings.youtube.authenticated
    feeds = configured_feeds(settings.youtube, settings.sources[0])
    assert len(feeds) == 5
    assert [feed.tab for feed in feeds if feed.kind == "news"] == [
        "top_stories",
        "business",
        "world",
    ]
    assert feeds[-1].url == "https://www.youtube.com/@Channel4News/videos"


@pytest.mark.parametrize(
    "option",
    [
        "max_posts = false",
        "max_posts = 0",
        "max_scrolls = -1",
        "scroll_pause_seconds = nan",
        "scroll_pause_seconds = inf",
        "scroll_pause_seconds = true",
        "known_overlap = 0",
        "stall_scrolls = 0",
        'scrape_videos = "false"',
        "include_shorts = 1",
        "homepage = true",
        'news_tabs = ["unknown"]',
        'region = "United Kingdom"',
        "profile_dir = 42",
    ],
)
def test_youtube_settings_validation(tmp_path: Path, option: str) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("[youtube]\n" + option)

    with pytest.raises(ConfigError, match="youtube"):
        load_settings(path)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.test/@channel/videos",
        "https://youtube.com/watch?v=abcdefghijk",
        "https://youtube.com@evil.test/feed/hype",
        "https://[bad/feed/hype",
    ],
)
def test_only_listing_urls_are_accepted(tmp_path: Path, url: str) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        f'sources = [{{domain="youtube.com",feed_urls=["{url}"]}}]',
    )

    with pytest.raises(ConfigError, match="YouTube"):
        load_settings(path)


def test_home_cannot_be_enabled_by_a_feed_url() -> None:
    with pytest.raises(ValueError, match="authenticated"):
        configured_feeds(
            YouTubeSettings(),
            SourceConfig(
                "youtube.com",
                (),
                ("https://www.youtube.com/",),
            ),
        )


def test_news_channel_alias_and_disabled_defaults() -> None:
    feeds = configured_feeds(
        YouTubeSettings(hype=False, news_tabs=()),
        SourceConfig(
            "youtube.com",
            (),
            (
                "https://www.youtube.com/channel/UCYfdidRxbB8Qhf0Nx7ioOYw/videos",
            ),
        ),
    )
    assert len(feeds) == 1
    assert feeds[0].kind == "news"
    assert feeds[0].tab == "top_stories"
