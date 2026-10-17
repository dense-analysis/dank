from __future__ import annotations

import datetime
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.scrape.__main__ import main
from dank.scrape.runner import (
    _load_x_posts,  # pyright: ignore[reportPrivateUsage]
)


def test_targeted_cli_canonicalizes_and_passes_repeated_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = MagicMock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "scrape",
            "--headless",
            "--x-post",
            "https://twitter.com/writer/status/100?s=20",
            "--x-post",
            "https://x.com/writer/status/101",
        ],
    )
    main()

    assert run.call_args.kwargs["x_posts"] == (
        "https://x.com/writer/status/100",
        "https://x.com/writer/status/101",
    )
    assert run.call_args.kwargs["headless"] is True


@pytest.mark.parametrize(
    "extra",
    [
        ["--domains", "x.com"],
        ["--retry-failed-pages"],
    ],
)
def test_targeted_cli_rejects_ambiguous_source_selection(
    monkeypatch: pytest.MonkeyPatch,
    extra: list[str],
) -> None:
    run = MagicMock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "scrape",
            "--x-post",
            "https://x.com/writer/status/100",
            *extra,
        ],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    run.assert_not_called()


def test_invalid_cli_url_is_rejected_before_loading_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = MagicMock()
    monkeypatch.setattr("dank.scrape.__main__.run_scrape_from_config", run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "scrape",
            "--x-post",
            "https://evil.test/writer/status/100",
        ],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    run.assert_not_called()


async def test_saved_seed_lookup_is_scoped_and_does_not_crawl_accounts() -> (
    None
):
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    row = {
        "domain": "x.com",
        "post_id": "100",
        "url": "old-url",
        "post_created_at": now.isoformat(),
        "scraped_at": now.isoformat(),
        "source": "x",
        "request_url": "old-request",
        "payload": json.dumps(
            {"rest_id": "100", "legacy": {"full_text": "Saved"}},
        ),
    }
    client = MagicMock(
        fetch_json=AsyncMock(
            return_value=SimpleNamespace(rows=[row]),
        ),
    )
    posts = await _load_x_posts(
        cast(Any, client),
        (
            "https://x.com/writer/status/100",
            "https://twitter.com/writer/status/100",
        ),
    )

    assert len(posts) == 1
    assert posts[0].payload == row["payload"]
    assert posts[0].url == "https://x.com/writer/status/100"
    query, parameters = client.fetch_json.call_args.args
    assert "WHERE domain = 'x.com'" in query
    assert "ORDER BY scraped_at DESC LIMIT 1 BY post_id" in query
    assert parameters == {"post_ids": ["100"]}


async def test_targeted_collection_requires_saved_seed() -> None:
    client = MagicMock(
        fetch_json=AsyncMock(
            return_value=SimpleNamespace(rows=[]),
        ),
    )

    with pytest.raises(ValueError, match="must already be saved: 100"):
        await _load_x_posts(
            cast(Any, client), ("https://x.com/writer/status/100",),
        )


async def test_targeted_runner_uses_saved_posts_instead_of_account_crawl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.scrape.runner import (
        _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    )

    settings = MagicMock()
    source = MagicMock(domain="x.com")
    saved = [MagicMock(post_id="100")]
    load = AsyncMock(return_value=saved)
    calls: list[object] = []
    crawl = MagicMock(side_effect=AssertionError("Must not crawl accounts"))

    async def collect(*args: Any) -> Any:
        calls.append(args)
        yield MagicMock(posts=saved, assets=[])

    monkeypatch.setattr("dank.scrape.runner._load_x_posts", load)
    monkeypatch.setattr("dank.scrape.runner.scrape_x_posts", collect)
    monkeypatch.setattr("dank.scrape.runner.scrape_x_accounts", crawl)
    client, http, browser = MagicMock(), MagicMock(), MagicMock()
    urls = ("https://x.com/writer/status/100",)
    batches = [batch async for batch in _discover_source_batches(
        settings, source, client, http, browser,
        feed_staleness=datetime.timedelta(days=1), batch_size=50, x_posts=urls,
    )]

    load.assert_awaited_once_with(client, urls)
    assert calls == [(settings.x, saved, browser)]
    assert batches[0].posts == saved
    crawl.assert_not_called()
