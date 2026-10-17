from __future__ import annotations

import asyncio
import datetime as dt
import json
import pathlib
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from dank.config import SourceConfig
from dank.html_utils import is_page_challenge, remove_page_noise
from dank.media_urls import canonical_media_url
from dank.model import AssetDiscovery
from dank.scrape import page_recovery, rss, runner
from dank.scrape.assets.http import download_file_http
from dank.scrape.http import HTTP_USER_AGENT, http_response
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.page_recovery import (
    PageRecovery,
    RecoveredPage,
    current_page_recovery,
    supports_browser_recovery,
)
from dank.scrape.zendriver import NetworkResponse

ARCHIVE_URL = (
    "https://ia600209.us.archive.org/31/items/dog_factory_1904/"
    "dog_factory_1904_512kb.mp4"
)
ARCHIVE_CANONICAL = (
    "https://archive.org/download/dog_factory_1904/"
    "dog_factory_1904_512kb.mp4"
)
PUBLIC_PAGE = "https://www.etsy.com/codeascraft/example"
ARTICLE = '<html><title>Article</title><article>Saved article</article></html>'
CHALLENGE = (
    '<html><title>etsy.com</title><script '
    'src="https://geo.captcha-delivery.com/a.js"></script></html>'
)


class Response:
    content_length = 5
    status = 200
    url = "https://example.test/image"

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    def raise_for_status(self) -> None:
        pass

    @property
    def content(self) -> Response:
        return self

    async def iter_chunked(self, size: int):
        yield b"image"


async def test_media_identifies_collector_and_sends_origin_only_referrer(
    tmp_path: pathlib.Path,
) -> None:
    client = Mock(get=Mock(return_value=Response()))
    asset = AssetDiscovery(
        "rss", "lobste.rs", "post", "https://shkspr.mobi/favicons/?domain=x",
        "image", None, "https://shkspr.mobi/article?private=value#fragment",
    )
    result = await download_file_http(
        discovery=asset, target_dir=tmp_path, http_client=cast(Any, client),
        max_asset_bytes=100, timestamp=dt.datetime.now(dt.UTC),
    )
    assert result is not None
    assert await asyncio.to_thread(pathlib.Path(result.local_path).read_bytes)
    assert client.get.call_args.kwargs["headers"] == {
        "Referer": "https://shkspr.mobi/", "User-Agent": HTTP_USER_AGENT,
    }


async def test_explicit_user_agent_is_preserved_case_insensitively() -> None:
    client = Mock(get=Mock(return_value=Response()))

    async with http_response(
        cast(Any, client), Response.url, headers={"user-agent": "Example/1"},
    ):
        pass

    assert client.get.call_args.kwargs["headers"] == {
        "user-agent": "Example/1",
    }


@pytest.mark.parametrize("referrer,target", [
    ("https://user:password@example.test/path", "https://cdn.test/image"),
    ("https://example.test/path?secret=x", "http://cdn.test/image"),
    ("file:///private/path", "https://cdn.test/image"),
])
async def test_media_referrer_does_not_leak_credentials_or_downgrade(
    referrer: str, target: str, tmp_path: pathlib.Path,
) -> None:
    client = Mock(get=Mock(return_value=Response()))
    await download_file_http(
        discovery=AssetDiscovery("rss", "a", "b", target, "image", None,
                                 referrer),
        target_dir=tmp_path, http_client=cast(Any, client),
        max_asset_bytes=100, timestamp=dt.datetime.now(dt.UTC),
    )
    assert "Referer" not in client.get.call_args.kwargs["headers"]


def test_stale_archive_links_have_matching_discovery_and_reader_addresses():
    html = f'<video src="{ARCHIVE_URL}"></video>'
    assets = rss._extract_page_assets(  # pyright: ignore[reportPrivateUsage]
        html, domain="example.test", post_id="post",
        base_url="https://example.test/article",
    )
    assert assets[0].url == ARCHIVE_CANONICAL
    assert ARCHIVE_CANONICAL in remove_page_noise(html)
    assert ARCHIVE_URL not in remove_page_noise(html)
    external_host = ARCHIVE_URL.replace(
        "us.archive.org", "us.archive.org.attacker.test",
    )
    assert canonical_media_url(external_host) == external_host
    external = "https://other.test/31/items/a/file.mp4"
    assert canonical_media_url(external) == external


def test_comment_feeds_are_not_automatically_discovered():
    html = (
        '<head><link rel="alternate" type="application/rss+xml" href="/feed/">'
        '<link rel="alternate" type="application/rss+xml" '
        'href="/comments/feed/"></head>'
    )
    assert [feed.url for feed in rss.discover_feed_links(
        html, "https://example.test/",
    )] == ["https://example.test/feed/"]


async def test_cached_comments_filtered_but_explicit_selection_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    comments = "https://example.test/comments/feed/"
    articles = "https://example.test/feed/"
    refresh = AsyncMock()
    load = AsyncMock(return_value=[comments, articles])
    monkeypatch.setattr(runner, "_refresh_site_feeds", refresh)
    monkeypatch.setattr(runner, "_load_site_feed_urls", load)
    select = runner._source_feed_urls  # pyright: ignore[reportPrivateUsage]
    assert await select(cast(Any, None), SourceConfig("example.test", ()),
                        dt.timedelta(days=1)) == [articles]
    refresh.reset_mock()
    assert await select(cast(Any, None), SourceConfig(
        "example.test", (), (comments,),
    ), dt.timedelta(days=1)) == [comments]
    refresh.assert_not_awaited()


@pytest.mark.parametrize("url", [
    "https://lwn.net/Articles/123", "https://nichegamer.com/feed/",
    "https://www.etsy.com/your/account", "https://www.etsy.com:444/codeascraft/x",
    "https://user:pass@www.etsy.com/codeascraft/x",
])
def test_browser_recovery_is_limited_to_verified_public_layouts(url: str):
    assert not supports_browser_recovery(url)


async def test_browser_recovers_network_document_and_rejects_challenge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = SimpleNamespace(get=AsyncMock(), close=AsyncMock())
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    session = SimpleNamespace(get_browser=AsyncMock(return_value=browser))
    capture = SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), drain=AsyncMock(return_value=[
            NetworkResponse(PUBLIC_PAGE, 200, "text/html", CHALLENGE, "1",
                            "Document"),
            NetworkResponse(PUBLIC_PAGE, 200, "text/html", ARTICLE, "2",
                            "Document"),
        ]),
    )
    monkeypatch.setattr(page_recovery, "NetworkCapture", Mock(
        return_value=capture,
    ))
    result = await PageRecovery(cast(Any, session)).fetch(PUBLIC_PAGE)
    assert result == RecoveredPage(ARTICLE, PUBLIC_PAGE)
    capture.stop.assert_awaited_once()
    page.close.assert_awaited_once()


async def test_failed_browser_recovery_does_not_repeat_for_every_article(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = PageRecovery(cast(Any, None))
    fetch = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr(recovery, "_fetch_document", fetch)
    assert await recovery.fetch(PUBLIC_PAGE) is None
    assert await recovery.fetch(PUBLIC_PAGE + "-second") is None
    assert fetch.await_count == 2


async def test_transient_browser_failure_can_recover_on_its_bounded_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = PageRecovery(cast(Any, None))
    fetch = AsyncMock(side_effect=[
        TimeoutError(), RecoveredPage(ARTICLE, PUBLIC_PAGE),
    ])
    monkeypatch.setattr(recovery, "_fetch_document", fetch)
    assert await recovery.fetch(PUBLIC_PAGE) == RecoveredPage(
        ARTICLE, PUBLIC_PAGE,
    )
    assert not recovery.unavailable_hosts


async def test_recovered_capture_records_method_without_terminal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = SimpleNamespace(fetch=AsyncMock(return_value=RecoveredPage(
        ARTICLE, PUBLIC_PAGE,
    )))
    token = current_page_recovery.set(cast(Any, recovery))
    stats = SourceMetrics(1, SourceConfig("www.etsy.com", ()))
    stats_token = current_source.set(stats)
    monkeypatch.setattr(rss, "_fetch_text_with_retries", AsyncMock(
        return_value=rss.FetchedText(CHALLENGE, PUBLIC_PAGE),
    ))

    try:
        fetched = await rss._fetch_text(  # pyright: ignore[reportPrivateUsage]
            cast(Any, None), PUBLIC_PAGE, accept=["text/html"],
        )
    finally:
        current_page_recovery.reset(token)
        current_source.reset(stats_token)

    assert fetched is not None and fetched.method == "browser"
    assert stats.counts["fetch_failures"] == 0
    raw, _ = rss._build_raw_post(  # pyright: ignore[reportPrivateUsage]
        rss.PageDiscovery("www.etsy.com", PUBLIC_PAGE, None, "<item/>"),
        fetched, request_url="https://www.etsy.com/feed",
        feed_final_url="https://www.etsy.com/feed",
        scraped_at=dt.datetime.now(dt.UTC),
    )
    payload = json.loads(raw.payload)
    assert payload["page_fetch_method"] == "browser"
    assert "page_fetch_status" not in payload


async def test_http_denial_recovery_is_not_counted_as_terminal_failure():
    class Denied(Response):
        def raise_for_status(self) -> None:
            raise aiohttp.ClientResponseError(
                cast(Any, SimpleNamespace(real_url=PUBLIC_PAGE)), (),
                status=403, message="Forbidden",
            )

    recovery = SimpleNamespace(fetch=AsyncMock(return_value=RecoveredPage(
        ARTICLE, PUBLIC_PAGE,
    )))
    token = current_page_recovery.set(cast(Any, recovery))
    stats = SourceMetrics(1, SourceConfig("www.etsy.com", ()))
    stats_token = current_source.set(stats)

    try:
        fetched = await rss._fetch_text(  # pyright: ignore[reportPrivateUsage]
            cast(Any, Mock(get=Mock(return_value=Denied()))), PUBLIC_PAGE,
            accept=["text/html"],
        )
    finally:
        current_page_recovery.reset(token)
        current_source.reset(stats_token)

    assert fetched == rss.FetchedText(ARTICLE, PUBLIC_PAGE, "browser")
    assert stats.counts["fetch_failures"] == 0
    recovery.fetch.assert_awaited_once_with(PUBLIC_PAGE)


def test_verification_templates_are_not_mistaken_for_article_content():
    assert is_page_challenge(CHALLENGE)
    assert is_page_challenge(
        '<title>Just a moment...</title><script '
        'src="/cdn-cgi/challenge-platform/a.js"></script>',
    )
    assert not is_page_challenge(ARTICLE)


async def test_cancellation_is_not_cached_as_a_blocked_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = PageRecovery(cast(Any, None))
    monkeypatch.setattr(recovery, "_fetch_document", AsyncMock(
        side_effect=asyncio.CancelledError,
    ))

    with pytest.raises(asyncio.CancelledError):
        await recovery.fetch(PUBLIC_PAGE)

    assert not recovery.unavailable_hosts


async def test_browser_timeout_closes_its_tab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def never_ready(*args: object) -> None:
        await asyncio.Event().wait()

    page = SimpleNamespace(get=never_ready, close=AsyncMock())
    browser = SimpleNamespace(get=AsyncMock(return_value=page))
    session = SimpleNamespace(get_browser=AsyncMock(return_value=browser))
    capture = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    monkeypatch.setattr(page_recovery, "NetworkCapture", Mock(
        return_value=capture,
    ))
    monkeypatch.setattr(page_recovery, "RECOVERY_TIMEOUT_SECONDS", 0.01)
    recovery = PageRecovery(cast(Any, session))
    assert await recovery.fetch(PUBLIC_PAGE) is None
    assert capture.stop.await_count == 2
    assert page.close.await_count == 2
