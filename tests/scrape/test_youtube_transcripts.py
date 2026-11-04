from __future__ import annotations

import asyncio
import datetime as dt
import json
import subprocess
import sys
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.scrape import runner
from dank.scrape.youtube import transcripts
from dank.storage.clickhouse import QueryResult

_REAL_POPEN = subprocess.Popen
VIDEO = "L_6pAVHYoFM"
URL = "https://www.youtube.com/api/timedtext?v=" + VIDEO
BODY = json.dumps(
    {
        "events": [
            {
                "tStartMs": 1200,
                "dDurationMs": 2500,
                "segs": [{"utf8": "Hello &amp; "}, {"utf8": "welcome."}],
            },
            {"tStartMs": 5000, "segs": [{"utf8": "\n"}]},
            {
                "tStartMs": 6000,
                "dDurationMs": 1000,
                "segs": [{"utf8": "<script>literal text</script>"}],
            },
        ],
    },
).encode()


def track(name: str = "English", url: str = URL) -> dict[str, str]:
    return {"ext": "json3", "name": name, "url": url}


@pytest.mark.parametrize(
    "manual,automatic,expected",
    [
        ({"en": [track()]}, {"en": [track()]}, ("en", False)),
        ({"fr": [track("French")]}, {"en": [track()]}, ("en", True)),
        ({"fr": [track("French")]}, {}, ("fr", False)),
        ({}, {"fr-orig": [track("French")]}, ("fr", True)),
        ({}, {"en": [track(url=URL + "&tlang=en")]}, None),
        ({"en": [track(url="https://evil.example/api/timedtext")]}, {}, None),
        (
            {"en": [track(url="https://www.youtube.com:123/api/timedtext")]},
            {},
            None,
        ),
        (
            {"en": [track(url="https://user@www.youtube.com/api/timedtext")]},
            {},
            None,
        ),
    ],
)
def test_track_selection(
    manual: Any,
    automatic: Any,
    expected: tuple[str, bool] | None,
):
    result = transcripts.select_caption_track(
        {
            "subtitles": manual,
            "automatic_captions": automatic,
        },
    )
    assert (
        (result.language, result.generated) if result else None
    ) == expected


def test_segments_preserve_timing_and_literal_text_without_empty_events():
    assert transcripts.parse_caption_segments(BODY) == [
        {"start": 1.2, "duration": 2.5, "text": "Hello & welcome."},
        {
            "start": 6.0,
            "duration": 1.0,
            "text": "<script>literal text</script>",
        },
    ]


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"<html>blocked</html>",
        b"{}",
        b'{"events": []}',
        b'{"events":[{"tStartMs":-1,"segs":[{"utf8":"bad"}]}]}',
        b'{"events":[{"tStartMs":true,"segs":[{"utf8":"bad"}]}]}',
        b'{"events":[{"tStartMs":NaN,"segs":[{"utf8":"bad"}]}]}',
    ],
)
def test_malformed_captions_are_failures_not_unavailable(body: bytes):
    with pytest.raises(ValueError):
        transcripts.parse_caption_segments(body)


class Response:
    def __init__(self, body: bytes = BODY, status: int = 200):
        self.status = status
        self.content = asyncio.StreamReader()
        self.content.feed_data(body)
        self.content.feed_eof()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: Any):
        pass


class HTTP:
    def __init__(self, body: bytes = BODY, status: int = 200):
        self.response = Response(body, status)
        self.calls: list[Any] = []

    def get(self, *args: Any, **kwargs: Any):
        self.calls.append((args, kwargs))

        return self.response


async def test_capture_preserves_raw_payload_and_metadata(monkeypatch: Any):
    metadata = {"id": VIDEO, "subtitles": {"en": [track()]}}
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(return_value=metadata),
    )
    http = HTTP()
    row = await transcripts.fetch_video_transcript(cast(Any, http), VIDEO)
    assert row["status"] == "captured"
    assert row["provider_payload"] == BODY.decode()
    assert json.loads(row["track_payload"]) == metadata
    assert json.loads(row["payload"])["generated"] is False
    assert len(json.loads(row["payload"])["segments"]) == 2
    assert http.calls[0][1]["allow_redirects"] is False


async def test_missing_tracks_are_cached_without_caption_request(
    monkeypatch: Any,
):
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(return_value={"id": VIDEO}),
    )
    http = HTTP()
    row = await transcripts.fetch_video_transcript(cast(Any, http), VIDEO)
    assert row["status"] == "unavailable"
    assert (
        row["retry_after"] - row["observed_at"] == transcripts.UNAVAILABLE_TTL
    )
    assert not http.calls


@pytest.mark.parametrize(
    "body,status",
    [
        (b"rate limited", 429),
        (b"redirect", 302),
        (b"", 200),
        (b"x" * (transcripts.MAX_TRANSCRIPT_BYTES + 1), 200),
    ],
)
async def test_failed_tracks_have_failure_cache(
    monkeypatch: Any,
    body: bytes,
    status: int,
):
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(
            return_value={"id": VIDEO, "subtitles": {"en": [track()]}},
        ),
    )
    row = await transcripts.fetch_video_transcript(
        cast(Any, HTTP(body, status)),
        VIDEO,
    )
    assert row["status"] == "failed"
    assert row["error"]
    assert row["retry_after"] - row["observed_at"] == transcripts.FAILURE_TTL


async def test_extractor_timeout_is_bounded_and_cancellation_propagates(
    monkeypatch: Any,
):
    async def wait(*args: Any):
        await asyncio.Event().wait()

    monkeypatch.setattr(transcripts, "extract_caption_metadata", wait)
    monkeypatch.setattr(transcripts, "TRANSCRIPT_TIMEOUT_SECONDS", 0.01)
    row = await transcripts.fetch_video_transcript(cast(Any, HTTP()), VIDEO)
    assert row["status"] == "failed"
    monkeypatch.setattr(transcripts, "TRANSCRIPT_TIMEOUT_SECONDS", 10)
    task = asyncio.create_task(
        transcripts.fetch_video_transcript(
            cast(Any, HTTP()),
            VIDEO,
        ),
    )
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize(
    "status,days",
    [
        ("captured", -30),
        ("failed", 1),
        ("unavailable", 1),
    ],
)
async def test_saved_success_and_recent_failures_never_refetch(
    monkeypatch: Any,
    status: str,
    days: int,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(
        [
            {
                "status": status,
                "retry_after": dt.datetime.now(dt.UTC)
                + dt.timedelta(days=days),
            },
        ],
    )
    fetch = AsyncMock()
    monkeypatch.setattr(runner, "fetch_video_transcript", fetch)
    row = await runner.collect_youtube_video_transcript(
        client,
        cast(Any, None),
        VIDEO,
    )
    assert row["cached"] is True
    fetch.assert_not_called()
    client.insert_rows.assert_not_called()


async def test_due_failure_retries_and_only_persists_transcript(
    monkeypatch: Any,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(
        [
            {
                "status": "failed",
                "retry_after": dt.datetime(2000, 1, 1, tzinfo=dt.UTC),
            },
        ],
    )
    fetch = AsyncMock(return_value={"status": "captured"})
    monkeypatch.setattr(runner, "fetch_video_transcript", fetch)
    row = await runner.collect_youtube_video_transcript(
        client,
        cast(Any, None),
        VIDEO,
    )
    assert row["cached"] is False
    client.insert_rows.assert_awaited_once_with(
        "youtube_transcripts",
        [{"status": "captured"}],
    )


async def test_scrape_uses_only_current_run_and_stops_after_failure(
    monkeypatch: Any,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult(
        [
            {"video_id": VIDEO},
            {"video_id": "abcdefghijk"},
        ],
    )
    fetch = AsyncMock(return_value={"video_id": VIDEO, "status": "failed"})
    monkeypatch.setattr(runner, "collect_youtube_video_transcript", fetch)
    await runner.collect_run_youtube_transcripts(
        client,
        cast(Any, None),
        "this-run",
    )
    sql, params = client.fetch_json.call_args.args
    assert "d.run_id = %(run)s" in sql
    assert params == {"run": "this-run", "limit": 25}
    assert fetch.await_count == 1


async def test_cancelled_extractor_terminates_and_drains_process(
    monkeypatch: Any,
):
    from types import SimpleNamespace

    stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
    process = SimpleNamespace(
        stdout=stdout,
        stderr=stderr,
        returncode=None,
        pid=123456,
        communicate=AsyncMock(return_value=(b"", b"")),
        wait=AsyncMock(),
    )
    created = asyncio.Event()
    arguments: list[Any] = []
    killed: list[int] = []

    async def spawn(*args: Any, **kwargs: Any):
        arguments.extend(args)
        assert kwargs["start_new_session"] is True
        created.set()

        return process

    def kill(pid: int, sig: Any):
        killed.append(pid)
        process.returncode = -9
        stdout.feed_eof()
        stderr.feed_eof()

    monkeypatch.setattr(transcripts.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(transcripts.os, "killpg", kill)
    task = asyncio.create_task(transcripts.extract_caption_metadata(VIDEO))
    await created.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert killed == [123456]
    process.communicate.assert_awaited_once()
    assert "--skip-download" in arguments
    assert "--ignore-config" in arguments
    assert "--no-cache-dir" in arguments


def dubbed_captions(sources: list[str]) -> dict[str, list[dict[str, str]]]:
    """List tracks as yt-dlp does: each source, then translated into the rest.

    Sizes match `eqYqUAdjsL0`, whose 157 languages and seven formats reach
    about 480 bytes an entry. A language's translations from earlier sources
    come before its own track, which is why selection has to look past them.
    """
    languages = [*sources, *(f"t{number}" for number in range(157))][:157]
    captions: dict[str, list[dict[str, str]]] = {}

    def add(language: str, source: str, translate: str = "") -> None:
        captions.setdefault(language, []).extend(
            {
                "ext": ext,
                "name": language,
                "url": f"{URL}&kind=asr&lang={source}{translate}&fmt={ext}"
                f"&signature={'s' * 330}",
            }
            for ext in ("json3", "srv1", "srv2", "srv3", "ttml", "srt", "vtt")
        )

    for source in sources:
        for language in languages:
            if language != source:
                add(language, source, f"&tlang={language}")

        add(source, source)
        add(f"{source}-orig", source)

    return captions


def substitute_extractor(monkeypatch: Any, code: str) -> None:
    """Run a local script as yt-dlp, past the tests' network guard."""
    spawn = asyncio.create_subprocess_exec

    def local_script_only(args: Any, **kwargs: Any):
        assert tuple(args) == (sys.executable, "-c", code)

        return _REAL_POPEN(args, **kwargs)

    async def substitute(*args: Any, **kwargs: Any):
        return await spawn(sys.executable, "-c", code, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", local_script_only)
    monkeypatch.setattr(
        transcripts.asyncio,
        "create_subprocess_exec",
        substitute,
    )


async def test_dubbed_video_listing_gets_a_transcript(
    monkeypatch: Any,
    tmp_path: Any,
):
    listing = tmp_path / "metadata.json"
    listing.write_text(
        json.dumps(
            {
                "id": VIDEO,
                "automatic_captions": dubbed_captions(
                    ["bn", "en", *(f"s{number}" for number in range(19))],
                ),
                "language": "en-US",
            },
        ),
    )
    # Guards the fixture: this size overflowed the former 8 MiB limit.
    assert listing.stat().st_size > 8 * 1024 * 1024
    substitute_extractor(
        monkeypatch,
        "import shutil, sys\n"
        f"with open({str(listing)!r}) as metadata:\n"
        "    shutil.copyfileobj(metadata, sys.stdout)",
    )
    http = HTTP()

    row = await transcripts.fetch_video_transcript(cast(Any, http), VIDEO)

    assert row["error"] == ""
    assert row["status"] == "captured"
    assert json.loads(row["payload"])["language"] == "en"
    assert json.loads(row["payload"])["generated"] is True
    url = str(http.calls[0][0][0])
    assert "lang=en" in url
    assert "tlang" not in url


async def test_extractor_exit_saves_what_yt_dlp_reported(monkeypatch: Any):
    substitute_extractor(
        monkeypatch,
        "import sys\n"
        "sys.stderr.write('ERROR: [youtube] blocked')\n"
        "sys.exit(1)",
    )

    row = await transcripts.fetch_video_transcript(cast(Any, HTTP()), VIDEO)

    assert row["status"] == "failed"
    assert row["error"] == (
        "YouTube caption discovery failed: ERROR: [youtube] blocked"
    )


async def test_oversized_metadata_saves_the_limit_not_the_task_group(
    monkeypatch: Any,
):
    substitute_extractor(
        monkeypatch,
        "import sys\n"
        f"sys.stdout.write('x' * {transcripts.MAX_METADATA_BYTES + 1})",
    )

    row = await transcripts.fetch_video_transcript(cast(Any, HTTP()), VIDEO)

    assert row["status"] == "failed"
    assert row["retry_after"] - row["observed_at"] == transcripts.FAILURE_TTL
    assert row["error"] == (
        "YouTube caption metadata exceeded the "
        f"{transcripts.MAX_METADATA_BYTES}-byte limit"
    )


async def test_every_cause_in_an_exception_group_is_saved(monkeypatch: Any):
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(
            side_effect=ExceptionGroup(
                "outer",
                [
                    ValueError("first"),
                    ExceptionGroup(
                        "inner",
                        [OSError("second"), TimeoutError()],
                    ),
                ],
            ),
        ),
    )

    row = await transcripts.fetch_video_transcript(cast(Any, HTTP()), VIDEO)

    assert row["status"] == "failed"
    assert row["error"] == "first; second; TimeoutError"


async def test_failure_message_is_bounded(monkeypatch: Any):
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(
            side_effect=ExceptionGroup("outer", [ValueError("x" * 2000)]),
        ),
    )

    row = await transcripts.fetch_video_transcript(cast(Any, HTTP()), VIDEO)

    assert row["error"] == "x" * 1000


async def test_oversized_caption_body_names_what_overflowed(monkeypatch: Any):
    monkeypatch.setattr(
        transcripts,
        "extract_caption_metadata",
        AsyncMock(
            return_value={"id": VIDEO, "subtitles": {"en": [track()]}},
        ),
    )
    body = b"x" * (transcripts.MAX_TRANSCRIPT_BYTES + 1)

    row = await transcripts.fetch_video_transcript(
        cast(Any, HTTP(body)),
        VIDEO,
    )

    assert row["error"] == (
        "YouTube caption response exceeded the "
        f"{transcripts.MAX_TRANSCRIPT_BYTES}-byte limit"
    )


@pytest.mark.parametrize("body", [b"", b"<html>blocked</html>"])
def test_caption_body_that_is_not_json_says_so(body: bytes):
    with pytest.raises(ValueError, match="response was not valid JSON"):
        transcripts.parse_caption_segments(body)
