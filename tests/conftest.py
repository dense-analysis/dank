from __future__ import annotations

import socket
import subprocess
from collections.abc import Iterator
from typing import Any, Never, Self

import pytest


def pytest_report_teststatus(report: Any, config: Any) -> Any:
    if report.passed and report.when == "call":
        return report.outcome, "", report.outcome.upper()


class GuardedSocket:
    __slots__ = ('_s',)
    _s: socket.socket

    def __init__(self, s: socket.socket):
        object.__setattr__(self, '_s', s)

    def connect(self, address: socket._Address) -> None:  # type: ignore
        # AF_UNIX connects use a path string.
        if isinstance(address, str):
            return self._s.connect(address)

        raise RuntimeError('Network is disabled in tests')

    def connect_ex(self, address: socket._Address) -> int:  # type: ignore
        # AF_UNIX connects use a path string.
        if isinstance(address, str):
            return self._s.connect_ex(address)

        raise RuntimeError('Network is disabled in tests')

    def __getattr__(self, name: str) -> Any:
        return getattr(self._s, name)

    def __enter__(self) -> Self:
        self._s.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any):
        return self._s.__exit__(exc_type, exc, tb)


@pytest.fixture(scope='session', autouse=True)
def block_all_network() -> Iterator[None]:
    """
    This fixture blocks all network traffic during test runs so running
    pytest never accidentally dials network connections at any point.

    This keeps tests predictable and efficient.
    """
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

        # Block all other networking.
        blocked()

    ###
    ### Patch functions
    ###
    socket.socket = guarded_socket
    socket.create_connection = blocked
    socket.socketpair = guarded_socketpair
    socket.getaddrinfo = blocked
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
