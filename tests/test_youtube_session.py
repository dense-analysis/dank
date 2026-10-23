from __future__ import annotations

import asyncio
import json
import os
import pathlib
import stat
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from zendriver import cdp

from dank import youtube_session as session


def cookie(**changes: Any) -> cdp.network.Cookie:
    data = {
        "name": "SID", "value": "private-auth-cookie", "domain": ".google.com",
        "path": "/", "size": 19, "httpOnly": True, "secure": True,
        "session": False, "expires": time.time() + 3600,
        "priority": "High", "sourceScheme": "Secure", "sourcePort": 443,
        "sameSite": "None",
    }

    return cdp.network.Cookie.from_json({**data, **changes})


def browser(*cookies: cdp.network.Cookie) -> Any:
    return SimpleNamespace(cookies=SimpleNamespace(
        get_all=AsyncMock(return_value=list(cookies)), set_all=AsyncMock(),
    ))


def write_cache(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps({"version": 1, "cookies": rows}))
    path.chmod(0o600)


def parameters() -> dict[str, Any]:
    return {
        "name": "SID", "value": "private-auth-cookie", "domain": ".google.com",
        "path": "/", "secure": True, "httpOnly": True,
        "expires": time.time() + 3600, "sameSite": "None", "priority": "High",
        "sourceScheme": "Secure", "sourcePort": 443,
    }


async def test_round_trip_preserves_scope_expiry_flags_and_partition(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str],
) -> None:
    expiry = time.time() + 3600
    partition = {"topLevelSite": "https://www.youtube.com",
                 "hasCrossSiteAncestor": True}
    source = browser(
        cookie(expires=expiry),
        cookie(name="__Host-GAPS", domain="accounts.google.com",
               session=True, expires=-1),
        cookie(name="partitioned", domain=".youtube.com",
               partitionKey=partition),
        cookie(name="host-preference", domain="www.youtube.com", secure=False),
        cookie(name="outside", domain=".google.com.evil.test"),
        cookie(name="expired", expires=time.time() - 1),
        cookie(name="opaque", partitionKeyOpaque=True),
    )
    path = tmp_path / "youtube-session.json"
    assert await session.save_youtube_session(source, path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    payload = json.loads(path.read_bytes())
    assert len(payload["cookies"]) == 4
    target = browser()
    assert await session.restore_youtube_session(target, path)
    restored = [
        item.to_json() for item in target.cookies.set_all.call_args.args[0]
    ]
    assert restored[0]["expires"] == expiry
    assert restored[0]["domain"] == ".google.com"
    assert restored[0]["httpOnly"] is True
    assert restored[0]["secure"] is True
    assert restored[0]["sameSite"] == "None"
    assert restored[0]["priority"] == "High"
    assert restored[0]["sourcePort"] == 443
    assert restored[0]["sourceScheme"] == "Secure"
    assert restored[1]["url"] == "https://accounts.google.com/"
    assert "domain" not in restored[1]
    assert "expires" not in restored[1]
    assert restored[2]["partitionKey"] == partition
    assert restored[3]["url"] == "https://www.youtube.com/"
    assert restored[3]["secure"] is False
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("cookies", [(), (cookie(name="PREF"),)])
async def test_empty_or_signed_out_capture_preserves_existing_cache(
    tmp_path: pathlib.Path, cookies: tuple[cdp.network.Cookie, ...],
) -> None:
    path = tmp_path / "youtube-session.json"
    path.write_bytes(b"existing-session")
    path.chmod(0o600)
    assert not await session.save_youtube_session(browser(*cookies), path)
    assert path.read_bytes() == b"existing-session"


async def test_expired_authentication_cannot_restore(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "youtube-session.json"
    write_cache(path, [{**parameters(), "expires": time.time() - 1}])
    target = browser()
    assert not await session.restore_youtube_session(target, path)
    target.cookies.set_all.assert_not_awaited()


@pytest.mark.parametrize("changes", [
    {"domain": ".youtube.com.evil.test"}, {"domain": ".evilgoogle.com"},
    {"domain": "google.com"}, {"url": "https://evil.test/"},
    {"secure": "true"}, {"httpOnly": 1}, {"expires": float("nan")},
    {"expires": -1}, {"sourcePort": 0}, {"sameSite": "Anything"},
    {"partitionKey": {"topLevelSite": "https://evil.test",
                      "hasCrossSiteAncestor": True}},
    {"value": "private\\nvalue".replace("\\n", "\n")},
])
async def test_invalid_cookie_cache_is_rejected_before_browser_mutation(
    tmp_path: pathlib.Path, changes: dict[str, Any],
) -> None:
    path = tmp_path / "youtube-session.json"
    write_cache(path, [{**parameters(), **changes}])
    target = browser()
    assert not await session.restore_youtube_session(target, path)
    target.cookies.set_all.assert_not_awaited()


async def test_missing_public_symlink_and_oversized_caches_are_rejected(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "youtube-session.json"
    target = browser()
    assert not await session.restore_youtube_session(target, path)
    write_cache(path, [parameters()])
    path.chmod(0o644)
    assert not await session.restore_youtube_session(target, path)
    path.chmod(0o600)
    link = tmp_path / "linked.json"
    link.symlink_to(path)
    assert not await session.restore_youtube_session(target, link)
    path.write_bytes(b" " * (session.MAX_CACHE_BYTES + 1))
    assert not await session.restore_youtube_session(target, path)
    target.cookies.set_all.assert_not_awaited()


async def test_atomic_write_failure_preserves_cache_and_removes_temporary_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "youtube-session.json"
    path.write_bytes(b"existing-session")
    path.chmod(0o600)
    monkeypatch.setattr(session.os, "replace", Mock(side_effect=OSError(
        "private-cookie-details",
    )))
    assert not await session.save_youtube_session(browser(cookie()), path)
    assert path.read_bytes() == b"existing-session"
    entries = await asyncio.to_thread(os.listdir, tmp_path)
    assert entries == [path.name]


async def test_cookie_protocol_errors_are_silent_and_cancellation_propagates(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "youtube-session.json"
    target = browser()
    target.cookies.get_all.side_effect = RuntimeError("private-cookie-value")
    assert not await session.save_youtube_session(target, path)
    assert capsys.readouterr().out == ""
    target.cookies.get_all.side_effect = asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await session.save_youtube_session(target, path)


async def test_snapshot_stops_page_scripts_before_reading_current_cookies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
):
    order: list[str] = []

    async def navigate(url: str):
        assert url == "https://www.youtube.com/robots.txt"
        order.append("page stopped")

    async def save(browser: Any, path: pathlib.Path):
        order.append("cookies saved")
        return True

    page = SimpleNamespace(
        evaluate=AsyncMock(return_value=True),
        get=AsyncMock(side_effect=navigate),
    )
    monkeypatch.setattr(session, "save_youtube_session", save)
    assert await session.save_youtube_session_from_page(
        cast(Any, object()), cast(Any, page), tmp_path / "session.json",
    )
    assert order == ["page stopped", "cookies saved"]


async def test_signed_out_page_never_replaces_saved_cookie_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
):
    page = SimpleNamespace(
        evaluate=AsyncMock(return_value=False), get=AsyncMock(),
    )
    save = AsyncMock()
    monkeypatch.setattr(session, "save_youtube_session", save)
    assert not await session.save_youtube_session_from_page(
        cast(Any, object()), cast(Any, page), tmp_path / "session.json",
    )
    page.get.assert_not_called()
    save.assert_not_called()


async def test_cookie_capture_waits_for_committed_static_document(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
):
    page = SimpleNamespace(
        evaluate=AsyncMock(side_effect=[True, 1000.0, False, False, True]),
        get=AsyncMock(),
    )
    save = AsyncMock(return_value=True)
    monkeypatch.setattr(session, "save_youtube_session", save)
    assert await session.save_youtube_session_from_page(
        cast(Any, object()), cast(Any, page), tmp_path / "session.json",
    )
    assert page.evaluate.await_count == 5
    save.assert_awaited_once()


async def test_cookie_capture_preserves_cache_if_static_page_never_commits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
):
    answers = iter([True, 1000.0])

    async def evaluate(_script: str):
        return next(answers, False)

    page = SimpleNamespace(evaluate=evaluate, get=AsyncMock())
    save = AsyncMock()
    monkeypatch.setattr(session, "save_youtube_session", save)
    monkeypatch.setattr(session, "COOKIE_CAPTURE_TIMEOUT_SECONDS", 0.01)
    assert not await session.save_youtube_session_from_page(
        cast(Any, object()), cast(Any, page), tmp_path / "session.json",
    )
    save.assert_not_awaited()
