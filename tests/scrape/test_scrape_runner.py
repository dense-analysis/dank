from __future__ import annotations

import pathlib
import re

from dank.config import (
    BrowserSettings,
    ClickHouseSettings,
    LoggingSettings,
    Settings,
    SourceConfig,
    XSettings,
)
from dank.scrape.runner import filter_settings_sources


def _make_settings() -> Settings:
    return Settings(
        clickhouse=ClickHouseSettings(
            host='localhost',
            port=8123,
            database='dank',
            username='default',
            password='',
            secure=False,
            use_http=True,
        ),
        x=XSettings(
            email='x@example.com',
            username='x-user',
            password='secret',
            max_posts=200,
            max_scrolls=20,
            scroll_pause_seconds=1.5,
        ),
        data_dir=pathlib.Path('data'),
        max_asset_bytes=None,
        feed_staleness_days=14,
        sources=(
            SourceConfig(domain='x.com', accounts=('a',)),
            SourceConfig(domain='example.com', accounts=()),
            SourceConfig(domain='news.ycombinator.com', accounts=()),
        ),
        browser=BrowserSettings(
            executable_path=None,
            connection_timeout=None,
            connection_max_tries=None,
        ),
        email=None,
        logging=LoggingSettings(
            file_path=pathlib.Path('dank.log'),
            level='INFO',
        ),
    )


def test_filter_settings_sources_keeps_matching_domains() -> None:
    settings = _make_settings()
    filtered = filter_settings_sources(
        settings,
        re.compile(r'^x\.com$|^example\.com$'),
    )

    assert [source.domain for source in filtered.sources] == [
        'x.com',
        'example.com',
    ]


def test_filter_settings_sources_keeps_no_matches() -> None:
    settings = _make_settings()
    filtered = filter_settings_sources(settings, re.compile(r'^no-match$'))

    assert filtered.sources == ()
