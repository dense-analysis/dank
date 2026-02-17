from typing import Any

from dank.web import (
    __main__ as web_main,  # pyright: ignore[reportPrivateUsage]
)


def test_terminal_print_wraps_http_links_for_terminals(capsys: Any) -> None:
    text = "Running on http://127.0.0.1:8080"

    web_main._terminal_print(text)  # pyright: ignore[reportPrivateUsage]

    result = capsys.readouterr().out.strip()

    expected_link = (
        "\x1b]8;;http://127.0.0.1:8080\x1b\\"
        "http://127.0.0.1:8080"
        "\x1b]8;;\x1b\\"
    )
    assert result == f"Running on {expected_link}"


def test_linkify_urls_leaves_non_links_unchanged(capsys: Any) -> None:
    text = "(Press CTRL+C to quit)"

    web_main._terminal_print(text)  # pyright: ignore[reportPrivateUsage]

    result = capsys.readouterr().out.strip()

    assert result == text
