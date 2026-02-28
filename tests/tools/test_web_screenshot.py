from __future__ import annotations

import pathlib
from typing import Any, cast

import pytest

from dank.tools import web_screenshot


def test_build_config_normalizes_host_route_and_page_size() -> None:
    config = web_screenshot._build_config(  # pyright: ignore[reportPrivateUsage]
        config_path="config.toml",
        host="0.0.0.0",
        port=8800,
        route="?q=dank",
        output="data/view.png",
        width=1200,
        height=900,
        wait=0.2,
        page_size=999,
        full_page=True,
    )

    assert config.bind_host == "0.0.0.0"
    assert config.navigate_host == "127.0.0.1"
    assert config.route == "/?q=dank"
    assert config.page_size == web_screenshot.MAX_PAGE_SIZE


def test_build_config_rejects_non_local_host() -> None:
    with pytest.raises(ValueError, match="--host"):
        web_screenshot._build_config(  # pyright: ignore[reportPrivateUsage]
            config_path="config.toml",
            host="x.com",
            port=8800,
            route="/",
            output="data/view.png",
            width=1200,
            height=900,
            wait=0.2,
            page_size=50,
            full_page=False,
        )


def test_is_allowed_request_url_only_allows_loopback_urls() -> None:
    allowed = frozenset({"127.0.0.1", "localhost", "::1"})

    assert web_screenshot._is_allowed_request_url(  # pyright: ignore[reportPrivateUsage]
        "http://127.0.0.1:8799/",
        allowed_hosts=allowed,
        allowed_port=8799,
    )
    assert web_screenshot._is_allowed_request_url(  # pyright: ignore[reportPrivateUsage]
        "data:image/svg+xml;base64,PHN2Zz4=",
        allowed_hosts=allowed,
        allowed_port=8799,
    )
    assert not web_screenshot._is_allowed_request_url(  # pyright: ignore[reportPrivateUsage]
        "https://x.com/home",
        allowed_hosts=allowed,
        allowed_port=8799,
    )


def test_main_runs_screenshot_command(
    monkeypatch: Any,
    capsys: Any,
    tmp_path: pathlib.Path,
) -> None:
    captured: list[web_screenshot.WebScreenshotConfig] = []
    output_path = tmp_path / "capture.png"

    def fake_run(
        config: web_screenshot.WebScreenshotConfig,
    ) -> pathlib.Path:
        captured.append(config)

        return output_path

    monkeypatch.setattr(
        web_screenshot,
        "run_web_view_screenshot",
        cast(Any, fake_run),
    )

    web_screenshot.main(
        [
            "--host",
            "127.0.0.1",
            "--port",
            "8788",
            "--route",
            "/?q=signal",
            "--output",
            str(output_path),
        ],
    )

    output = capsys.readouterr().out

    assert captured
    assert captured[0].port == 8788
    assert captured[0].route == "/?q=signal"
    assert str(output_path) in output
