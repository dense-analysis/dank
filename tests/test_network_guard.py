import io
import pathlib
import socket
import sqlite3
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from socket import socket as original_socket
from subprocess import Popen

import clickhouse_connect
import psycopg
import pytest
from psycopg import connect as original_postgres_connect

DATABASE_BLOCKED = "Database connections are disabled"


def test_urllib_rejects_external_requests() -> None:
    url = "http://network.test"

    with pytest.raises(RuntimeError, match="Network is disabled"):
        urllib.request.urlopen(url)

    with pytest.raises(RuntimeError, match="Network is disabled"):
        urllib.request.build_opener().open(url)


def test_socket_rejects_connection_to_unlisted_endpoint() -> None:
    with (
        pytest.raises(RuntimeError, match="Network is disabled"),
        socket.socket() as client,
    ):
        client.connect(("192.0.2.1", 80))


def test_socket_blocks_reverse_dns() -> None:
    with pytest.raises(RuntimeError, match="Network is disabled"):
        socket.gethostbyaddr("192.0.2.1")


def test_socket_constructor_reference_cannot_bypass_guard() -> None:
    with pytest.raises(RuntimeError, match="Network is disabled"):
        original_socket(socket.AF_UNIX)


def test_application_socket_pairs_are_blocked() -> None:
    with pytest.raises(RuntimeError, match="Network is disabled"):
        socket.socketpair()


def test_clickhouse_client_is_blocked() -> None:
    with pytest.raises(RuntimeError, match=DATABASE_BLOCKED):
        clickhouse_connect.get_client(  # pyright: ignore[reportUnknownMemberType]
            host="127.0.0.1",
        )


def test_postgres_client_is_blocked() -> None:
    with pytest.raises(RuntimeError, match=DATABASE_BLOCKED):
        psycopg.connect(host="127.0.0.1")


def test_postgres_alias_cannot_bypass_guard() -> None:
    with pytest.raises(RuntimeError, match=DATABASE_BLOCKED):
        original_postgres_connect(hostaddr="127.0.0.1")


def test_sqlite_constructor_is_blocked() -> None:
    with pytest.raises(RuntimeError, match=DATABASE_BLOCKED):
        sqlite3.Connection(":memory:")


def test_subprocess_alias_cannot_bypass_guard() -> None:
    with pytest.raises(RuntimeError, match="Subprocesses are disabled"):
        Popen(["true"])


def test_file_opener_captured_before_setup_cannot_read_the_checkout() -> None:
    opener = _captured_opener()

    with pytest.raises(RuntimeError, match="Real filesystem I/O is disabled"):
        opener("Makefile").close()

    with (
        ThreadPoolExecutor(1) as pool,
        pytest.raises(RuntimeError, match="Real filesystem I/O is disabled"),
    ):
        pool.submit(opener, "Makefile").result().close()


def _captured_opener(
    opener: Callable[[str], io.TextIOBase] = io.open,
) -> Callable[[str], io.TextIOBase]:
    return opener


def test_writes_use_a_fresh_in_memory_filesystem(
    memory_path: pathlib.Path,
) -> None:
    path = memory_path / "written-by-test"
    assert not path.exists()

    path.write_bytes(b"contents")

    assert path.read_bytes() == b"contents"
    assert not (pathlib.Path.cwd() / "AGENTS.md").exists()
