from __future__ import annotations

import asyncio
import io
import json
import subprocess
import sys
from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock

import pytest
import yt_dlp

from dank.scrape.youtube.ytdlp_home import (
    _cookie_jar,  # pyright: ignore[reportPrivateUsage]
    _worker,  # pyright: ignore[reportPrivateUsage]
    classify_entry,
    collect_ytdlp_home,
)

# Saved before the suite blocks subprocesses; only fixed local scripts use it.
_REAL_POPEN = subprocess.Popen

COOKIES: list[dict[str, object]] = [{
    "name": "SAPISID", "value": "synthetic-session-value",
    "domain": ".youtube.com", "path": "/", "secure": True,
    "httpOnly": True, "expires": -1,
}]


@pytest.mark.parametrize(("raw", "kind", "identity"), [
    ({"id": "abcdefghijk", "url": "https://www.youtube.com/watch?v=abcdefghijk",
      "title": "News video", "duration": 10}, "video", "video:abcdefghijk"),
    ({"url": "https://www.youtube.com/shorts/abcdefghijk"},
     "short", "video:abcdefghijk"),
    ({"url": "https://youtu.be/abcdefghijk"}, "video", "video:abcdefghijk"),
    ({"id": "abcdefghijk", "ie_key": "Youtube", "url": "abcdefghijk"},
     "video", "video:abcdefghijk"),
    ({"url": "https://www.youtube.com/post/UgCommunity"},
     "post", "post:UgCommunity"),
    ({"id": "abcdefghijk", "url": "https://www.youtube.com/playables/game"},
     "unknown", ""),
    ({"url": "https://www.youtube.com/playlist?list=abcdefghijk"},
     "unknown", ""),
    ({"url": "https://not-youtube.example/shorts/abcdefghijk"}, "unknown", ""),
])
def test_flat_classification_preserves_original_entry(
    raw: dict[str, Any], kind: str, identity: str,
) -> None:
    item = classify_entry(raw, 7)
    assert item.raw is raw
    assert item.kind == kind
    assert item.identity == identity
    assert item.position == 7


def test_cookie_jar_has_no_file_and_only_provider_domains() -> None:
    jar = _cookie_jar([*COOKIES, {
        "name": "secret", "value": "unrelated", "domain": ".other.example",
    }])
    assert getattr(jar, "filename", None) is None
    cookies = list(jar)
    assert len(cookies) == 1
    assert cookies[0].expires is None
    assert cookies[0].has_nonstandard_attr("HttpOnly")


def _worker_provider(
    monkeypatch: pytest.MonkeyPatch, entries: object, *, limit: int = 10000,
) -> Mock:
    provider = Mock()
    provider.__enter__ = Mock(return_value=provider)
    provider.__exit__ = Mock(return_value=False)
    provider.extract_info.side_effect = [
        {"_type": "url", "url": "https://www.youtube.com/feed/recommended"},
        {"_type": "playlist", "entries": entries},
    ]
    constructor = Mock(return_value=provider)
    monkeypatch.setattr(yt_dlp, "YoutubeDL", constructor)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({
        "cookies": COOKIES, "max_entries": limit, "region": "GB",
    })))

    return constructor


def test_worker_streams_flat_entries_without_resolving_video_urls(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    raw = {"url": "https://www.youtube.com/watch?v=abcdefghijk",
           "id": "abcdefghijk", "title": "News", "duration": 11}
    constructor = _worker_provider(monkeypatch, iter([raw, raw]))
    _worker()
    output = capsys.readouterr().out
    events = [json.loads(line) for line in output.splitlines()]
    assert events == [
        {"event": "entry", "raw": raw}, {"event": "entry", "raw": raw},
        {"event": "done", "outcome": "completed"},
    ]
    assert constructor.return_value.extract_info.call_count == 2
    calls = constructor.return_value.extract_info.call_args_list
    assert all(call.kwargs == {"download": False, "process": False}
               for call in calls)
    assert calls[1].args[0].endswith("?gl=GB&hl=en-GB")
    options = constructor.call_args.args[0]
    assert options["extract_flat"] is True
    assert options["lazy_playlist"] is True
    assert options["skip_download"] is True
    assert options["cachedir"] is False
    assert not options.get("cookiefile")
    assert not options.get("cookiesfrombrowser")


def test_worker_keeps_prior_entries_and_hides_exception_messages(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    def entries() -> Iterator[dict[str, str]]:
        yield {"id": "abcdefghijk", "ie_key": "Youtube"}
        raise RuntimeError("synthetic-session-value must stay private")

    _worker_provider(monkeypatch, entries())
    _worker()
    output = capsys.readouterr().out
    assert "synthetic-session-value" not in output
    assert [json.loads(line)["event"] for line in output.splitlines()] == [
        "entry", "error",
    ]
    assert json.loads(output.splitlines()[-1])["error_type"] == "RuntimeError"


def test_worker_item_guard_does_not_advance_the_generator_again(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    def entries() -> Iterator[dict[str, str]]:
        yield {"id": "abcdefghijk", "ie_key": "Youtube"}
        pytest.fail("The item guard must not request another entry")

    _worker_provider(monkeypatch, entries(), limit=1)
    _worker()
    output = capsys.readouterr().out
    assert json.loads(output.splitlines()[-1]) == {
        "event": "done", "outcome": "item_limit",
    }


def _substitute_worker(
    monkeypatch: pytest.MonkeyPatch, code: str,
) -> tuple[list[asyncio.subprocess.Process], asyncio.Event]:
    spawn = asyncio.create_subprocess_exec

    def local_script_only(args: Any, **kwargs: Any):
        assert tuple(args) == (sys.executable, "-c", code)

        return _REAL_POPEN(args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", local_script_only)
    children: list[asyncio.subprocess.Process] = []
    launched = asyncio.Event()

    async def substitute(*args: Any, **kwargs: Any):
        assert "synthetic-session-value" not in repr(args)
        assert "env" not in kwargs
        child = await spawn(sys.executable, "-c", code, **kwargs)
        children.append(child)
        launched.set()

        return child

    monkeypatch.setattr("asyncio.create_subprocess_exec", substitute)

    return children, launched


async def test_subprocess_timeout_returns_partial_data_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children, _ = _substitute_worker(monkeypatch, '''
import json, sys, time
request = json.load(sys.stdin)
assert request['cookies'][0]['value'] == 'synthetic-session-value'
print(json.dumps({'event':'entry', 'raw':{
    'id':'abcdefghijk', 'ie_key':'Youtube', 'title':'Original metadata'}}),
    flush=True)
time.sleep(30)
''')
    result = await collect_ytdlp_home(COOKIES, timeout_seconds=0.5)
    assert result.outcome == "timeout"
    assert result.error_type == "TimeoutError"
    assert len(result.entries) == 1
    assert result.entries[0].raw["title"] == "Original metadata"
    assert result.first_entry_seconds is not None
    assert result.elapsed_seconds < 2
    assert children[0].returncode is not None


async def test_cancellation_kills_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children, launched = _substitute_worker(monkeypatch, '''
import json, sys, time
json.load(sys.stdin)
time.sleep(30)
''')
    task = asyncio.create_task(collect_ytdlp_home(COOKIES))
    await asyncio.wait_for(launched.wait(), 2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert children[0].returncode is not None


async def test_completed_worker_keeps_duplicates_and_reports_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children, _ = _substitute_worker(monkeypatch, '''
import json, sys
json.load(sys.stdin)
for event in [
    {'event':'entry', 'raw':{'id':'abcdefghijk', 'ie_key':'Youtube'}},
    {'event':'entry', 'raw':{'id':'abcdefghijk', 'ie_key':'Youtube'}},
    {'event':'warning', 'category':'repeated_continuation'},
    {'event':'done', 'outcome':'completed'}]:
    print(json.dumps(event), flush=True)
''')
    result = await collect_ytdlp_home(_cookie_jar(COOKIES))
    assert result.outcome == "completed"
    assert [entry.position for entry in result.entries] == [1, 2]
    assert result.warnings == ("repeated_continuation",)
    assert any("shelf" in limitation for limitation in result.limitations)
    assert children[0].returncode == 0


async def test_invalid_worker_output_returns_failure_without_leaking_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children, _ = _substitute_worker(monkeypatch, '''
import json, sys, time
json.load(sys.stdin)
print('synthetic-session-value malformed', flush=True)
time.sleep(30)
''')
    result = await collect_ytdlp_home(COOKIES)
    assert result.outcome == "failed"
    assert result.error_type == "JSONDecodeError"
    assert "synthetic-session-value" not in repr(result)
    assert children[0].returncode is not None


@pytest.mark.parametrize("kwargs", [
    {"timeout_seconds": 241}, {"timeout_seconds": 0}, {"max_entries": 10001},
])
async def test_invalid_limits_reject_without_spawning(kwargs: dict[str, Any]):
    with pytest.raises(ValueError):
        await collect_ytdlp_home(COOKIES, **kwargs)


async def test_missing_session_never_loads_browser_cookies():
    with pytest.raises(ValueError, match="session snapshot"):
        await collect_ytdlp_home([])


def test_worker_diagnostics_identify_stale_sessions_without_private_text(
    monkeypatch: pytest.MonkeyPatch,
):
    from dank.scrape.youtube import ytdlp_home

    events: list[dict[str, Any]] = []
    monkeypatch.setattr(ytdlp_home, "_emit", events.append)
    logger = ytdlp_home._WorkerLogger()  # pyright: ignore[reportPrivateUsage]
    logger.warning("The provided YouTube account cookies are no longer valid.")
    logger.error("Private details: URL redirected to youtube.com home page")
    assert events == [
        {"event": "warning", "category": "expired_session"},
        {"event": "warning", "category": "feed_redirected_to_home"},
    ]


def test_worker_initialization_errors_never_expose_cookie_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    from dank.scrape.youtube import ytdlp_home

    events: list[dict[str, Any]] = []
    monkeypatch.setattr(ytdlp_home, "_emit", events.append)
    monkeypatch.setattr(
        ytdlp_home, "_run_worker",
        Mock(side_effect=ValueError("private-cookie-value")),
    )
    _worker()
    assert events == [{"event": "error", "error_type": "ValueError"}]


@pytest.mark.parametrize("allow_home_redirect", [False, True])
def test_regional_home_url_needs_the_explicit_redirect_compatibility_option(
    monkeypatch: pytest.MonkeyPatch, *, allow_home_redirect: bool,
) -> None:
    from yt_dlp.extractor.youtube import YoutubeTabIE  # type: ignore
    from yt_dlp.utils import ExtractorError

    data: dict[str, Any] = {
        "contents": {"twoColumnBrowseResultsRenderer": {"tabs": [{
            "tabRenderer": {
                "selected": True, "tabIdentifier": "FEwhat_to_watch",
                "content": {"richGridRenderer": {"contents": []}},
            },
        }]}},
    }
    options: Any = {
        "quiet": True,
        "compat_opts": (
            ["no-youtube-channel-redirect"] if allow_home_redirect else []
        ),
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        extractor: Any = YoutubeTabIE(downloader)
        monkeypatch.setattr(
            extractor, "_extract_webpage", Mock(return_value=("", data)),
        )

        if allow_home_redirect:
            result, _ = extractor._extract_data(
                "https://www.youtube.com/feed/recommended?gl=GB&hl=en-GB",
                "recommended", ytcfg={"LOGGED_IN": True},
            )
            assert result == data
        else:
            with pytest.raises(ExtractorError, match="redirected"):
                extractor._extract_data(
                    "https://www.youtube.com/feed/recommended?gl=GB&hl=en-GB",
                    "recommended", ytcfg={"LOGGED_IN": True},
                )


async def test_renderer_timeout_keeps_shelf_context_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children, _ = _substitute_worker(monkeypatch, '''
import json, sys, time
request = json.load(sys.stdin)
assert request['include_renderers'] is True
print(json.dumps({'event':'renderer', 'key':'reelItemRenderer',
    'raw':{'videoId':'abcdefghijk'}, 'shelf':'Shorts','shelves':['Shorts']}),
    flush=True)
time.sleep(30)
''')
    result = await collect_ytdlp_home(
        COOKIES, timeout_seconds=0.5, include_renderers=True,
    )
    assert result.outcome == "timeout"
    assert result.entries == ()
    assert len(result.renderers) == 1
    assert result.renderers[0].shelves == ("Shorts",)
    assert result.renderers[0].raw["videoId"] == "abcdefghijk"
    assert children[0].returncode is not None
