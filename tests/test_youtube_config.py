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


US_HYPE = (
    "https://www.youtube.com/playlist?list="
    "OLAK5uy_lSbPjCHn-9LSQeRc6T1LWfz-DN8gOqNdw"
)


def test_regional_hype_playlists_preserve_identity_and_default_region(
    tmp_path: Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = ["youtube.com"]\n[youtube.hype_playlists]\n'
        f'us = "{US_HYPE}&gl=GB"',
    )
    settings = load_settings(path)
    assert settings.youtube.hype_playlists == (("US", US_HYPE),)
    feeds = configured_feeds(settings.youtube, settings.sources[0])
    hyped = [feed for feed in feeds if feed.kind == "hype"]
    assert [(feed.url, feed.region) for feed in hyped] == [
        ("https://www.youtube.com/feed/hype", ""), (US_HYPE, "US"),
    ]
    disabled = configured_feeds(
        settings.youtube._replace(hype=False), settings.sources[0],
    )
    assert not any(feed.kind == "hype" for feed in disabled)


def test_explicit_hype_playlist_replaces_matching_default_and_plain_playlist():
    feeds = configured_feeds(
        YouTubeSettings(region="US", news_tabs=(),
                        hype_playlists=(("US", US_HYPE),)),
        SourceConfig("youtube.com", (), (US_HYPE,)),
    )
    assert len(feeds) == 1
    assert feeds[0].kind == "hype"
    assert feeds[0].region == "US"


@pytest.mark.parametrize("table", [
    'hype_playlists = []',
    'hype_playlists = {USA = "' + US_HYPE + '"}',
    'hype_playlists = {US = 5}',
    'hype_playlists = {US = "https://evil.test/playlist?list=abc"}',
    'hype_playlists = {US = "https://www.youtube.com/@channel/videos"}',
    'hype_playlists = {US = "https://www.youtube.com/playlist"}',
    'hype_playlists = {US = "https://www.youtube.com/playlist?list=a&list=b"}',
    'hype_playlists = {US = "' + US_HYPE + '", us = "' + US_HYPE + '"}',
    'hype_playlists = {US = "' + US_HYPE + '", GB = "' + US_HYPE + '"}',
])
def test_hype_playlist_configuration_rejects_ambiguous_destinations(
    tmp_path: Path, table: str,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text("[youtube]\n" + table)

    with pytest.raises(ConfigError, match=r"youtube\.hype_playlists"):
        load_settings(path)


def test_youtube_onepassword_keeps_references_and_normalizes_generated_otp(
    tmp_path: Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        '[youtube.onepassword]\naccount = "my.1password.com"\n'
        'username = "op://Private/Google/username"\n'
        'password = "op://Private/Google/password"\n'
        'otp = "op://Private/Google/one-time password"\n',
    )
    credentials = load_settings(path).youtube.onepassword
    assert credentials is not None
    assert credentials.account == "my.1password.com"
    assert credentials.username == "op://Private/Google/username"
    assert credentials.password == "op://Private/Google/password"
    assert credentials.otp == (
        "op://Private/Google/one-time password?attribute=otp"
    )


@pytest.mark.parametrize("replacement", [
    'account = ""', 'account = 42', 'username = "literal-secret"',
    'password = "op://Private/Google"', 'password = "op://Private//password"',
    'otp = "op://Private/Google/otp?attribute=value"',
    'otp = "otpauth://seed"', 'unexpected = "field"',
])
def test_youtube_onepassword_rejects_invalid_references_and_seed_attributes(
    tmp_path: Path, replacement: str,
) -> None:
    fields = {
        "account": 'account = "my.1password.com"',
        "username": 'username = "op://Private/Google/username"',
        "password": 'password = "op://Private/Google/password"',
    }
    fields[replacement.split(" =")[0]] = replacement
    path = tmp_path / "settings.fixture.toml"
    path.write_text("[youtube.onepassword]\n" + "\n".join(fields.values()))

    with pytest.raises(ConfigError, match=r"youtube\.onepassword"):
        load_settings(path)
