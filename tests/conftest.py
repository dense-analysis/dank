from __future__ import annotations

import ipaddress
import socket
import subprocess
from collections.abc import Container, Iterator
from typing import Any, Never, Self
from urllib.parse import urlsplit

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--clickhouse-url",
        default="",
        help=(
            "Run tests marked clickhouse against this loopback server, "
            "e.g. http://default:@127.0.0.1:18123"
        ),
    )


def pytest_report_teststatus(report: Any, config: Any) -> Any:
    if report.passed and report.when == "call":
        return report.outcome, "", report.outcome.upper()


class GuardedSocket:
    __slots__ = ('_allowed', '_s')
    _s: socket.socket
    _allowed: frozenset[tuple[str, int]]

    def __init__(
        self,
        s: socket.socket,
        allowed: frozenset[tuple[str, int]] = frozenset(),
    ):
        object.__setattr__(self, '_s', s)
        object.__setattr__(self, '_allowed', allowed)

    def connect(self, address: socket._Address) -> None:  # type: ignore
        # AF_UNIX connects use a path string.
        if self._permits(address):
            return self._s.connect(address)

        raise RuntimeError('Network is disabled in tests')

    def connect_ex(self, address: socket._Address) -> int:  # type: ignore
        # AF_UNIX connects use a path string.
        if self._permits(address):
            return self._s.connect_ex(address)

        raise RuntimeError('Network is disabled in tests')

    def _permits(self, address: Any) -> bool:
        return isinstance(address, str) or tuple(address[:2]) in self._allowed

    def __getattr__(self, name: str) -> Any:
        return getattr(self._s, name)

    def __enter__(self) -> Self:
        self._s.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any):
        return self._s.__exit__(exc_type, exc, tb)


def loopback_addresses(url: str) -> frozenset[tuple[str, int]]:
    """The addresses a --clickhouse-url may connect to; loopback only."""
    if not url:
        return frozenset()

    parts = urlsplit(url)
    port = parts.port or 8123
    addresses = frozenset(
        (str(info[4][0]), port)
        for info in socket.getaddrinfo(parts.hostname, port)
    )

    if not all(
        ipaddress.ip_address(host).is_loopback for host, _ in addresses
    ):
        raise pytest.UsageError('--clickhouse-url must be a loopback address')

    return addresses


@pytest.fixture(scope='session', autouse=True)
def block_all_network(request: pytest.FixtureRequest) -> Iterator[None]:
    """
    This fixture blocks all network traffic during test runs so running
    pytest never accidentally dials network connections at any point.

    This keeps tests predictable and efficient. The one exception is the
    loopback ClickHouse server named by --clickhouse-url, if any.
    """
    url = str(request.config.getoption('clickhouse_url'))
    allowed = loopback_addresses(url)
    # The ClickHouse driver resolves the URL's host name before connecting.
    allowed_hosts: Container[object] = {
        urlsplit(url).hostname, *(host for host, _ in allowed),
    } if allowed else frozenset[str]()

    ###
    ### Save originals
    ###
    real_socket = socket.socket
    real_create_connection = socket.create_connection
    real_socketpair = socket.socketpair
    real_getaddrinfo = socket.getaddrinfo
    real_gethostbyname = socket.gethostbyname
    real_gethostbyname_ex = socket.gethostbyname_ex
    real_popen = subprocess.Popen

    ###
    ### Create mock functions
    ###
    def blocked(*args: Any, **kwargs: Any) -> Never:
        raise RuntimeError('Network is disabled in tests')

    def guarded_socketpair(*args: Any, **kwargs: Any) -> tuple[
        GuardedSocket,
        GuardedSocket,
    ]:
        a, b = real_socketpair(*args, **kwargs)

        return GuardedSocket(a), GuardedSocket(b)

    def guarded_socket(
        family: socket.AddressFamily | int=-1,
        type: socket.SocketKind=socket.SOCK_STREAM,  # noqa
        proto: int=-1,
        fileno: int | None=None,
    ):
        # Allow UNIX sockets for local IPC (asyncio uses these via socketpair).
        if family == socket.AF_UNIX:
            return (
                GuardedSocket(real_socket(family, type, proto, fileno))
                if fileno is not None else
                GuardedSocket(real_socket(family, type, proto))
            )

        # TCP sockets may only connect to the --clickhouse-url server.
        if allowed and fileno is None:
            return GuardedSocket(real_socket(family, type, proto), allowed)

        # Block all other networking.
        blocked()

    def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        if host in allowed_hosts:
            return real_getaddrinfo(host, *args, **kwargs)

        blocked()

    ###
    ### Patch functions
    ###
    socket.socket = guarded_socket
    socket.create_connection = blocked
    socket.socketpair = guarded_socketpair
    socket.getaddrinfo = guarded_getaddrinfo
    socket.gethostbyname = blocked
    socket.gethostbyname_ex = blocked
    subprocess.Popen = blocked

    try:
        yield
    finally:
        ###
        ### Restore functions
        ###
        socket.socket = real_socket
        socket.create_connection = real_create_connection
        socket.socketpair = real_socketpair
        socket.getaddrinfo = real_getaddrinfo
        socket.gethostbyname = real_gethostbyname
        socket.gethostbyname_ex = real_gethostbyname_ex
        subprocess.Popen = real_popen
