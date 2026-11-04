from __future__ import annotations

import asyncio
import ipaddress
import pathlib
import socket
import subprocess
import uuid
from collections.abc import AsyncIterator, Container, Iterator
from typing import Any, Never, Self
from urllib.parse import urlsplit

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import Session

from dank.config import PostgresSettings
from dank.db.atlas import atlas_url, find_atlas, upgrade
from dank.db.engine import async_engine, sync_engine
from tests.postgres import (
    create_database,
    drop_database,
    server_settings,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--clickhouse-url",
        default="",
        help=(
            "Run tests marked clickhouse against this loopback server, "
            "e.g. http://default:@127.0.0.1:18123"
        ),
    )
    parser.addoption(
        "--postgres-url",
        default="",
        help=(
            "Run tests marked postgres against this loopback server, "
            "e.g. postgresql://postgres:test@127.0.0.1:15433/postgres"
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


def loopback_addresses(
    url: str,
    *,
    option: str = '--clickhouse-url',
    default_port: int = 8123,
) -> frozenset[tuple[str, int]]:
    """The addresses a test server URL may connect to; loopback only."""
    if not url:
        return frozenset()

    parts = urlsplit(url)
    port = parts.port or default_port
    addresses = frozenset(
        (str(info[4][0]), port)
        for info in socket.getaddrinfo(parts.hostname, port)
    )

    if not all(
        ipaddress.ip_address(host).is_loopback for host, _ in addresses
    ):
        raise pytest.UsageError(f'{option} must be a loopback address')

    return addresses


@pytest.fixture(scope='session', autouse=True)
def block_all_network(request: pytest.FixtureRequest) -> Iterator[None]:
    """
    This fixture blocks all network traffic during test runs so running
    pytest never accidentally dials network connections at any point.

    This keeps tests predictable and efficient. The exceptions are the
    loopback servers named by --clickhouse-url and --postgres-url, if any,
    and the Atlas binary that the Postgres lane runs.
    """
    clickhouse_url = str(request.config.getoption('clickhouse_url'))
    postgres_url = str(request.config.getoption('postgres_url'))
    allowed = loopback_addresses(clickhouse_url) | loopback_addresses(
        postgres_url,
        option='--postgres-url',
        default_port=5432,
    )
    # The drivers resolve the URL's host name before connecting.
    allowed_hosts: Container[object] = {
        urlsplit(url).hostname
        for url in (clickhouse_url, postgres_url)
        if url
    } | {host for host, _ in allowed}

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

    def guarded_popen(args: Any, *rest: Any, **kwargs: Any) -> Any:
        # asyncio starts subprocesses through this name, passing a tuple.
        if postgres_url and pathlib.Path(str(args[0])).name == 'atlas':
            return real_popen(args, *rest, **kwargs)

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
    subprocess.Popen = guarded_popen

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


@pytest.fixture(scope='session')
def postgres_server(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> PostgresSettings:
    """The --postgres-url server. Tests using it skip when it's missing."""
    url = str(request.config.getoption('postgres_url'))

    if not url:
        pytest.skip('needs --postgres-url; see make test-postgres')

    if find_atlas() is None:
        pytest.skip('needs Atlas; run uv run dank-db install-atlas')

    return server_settings(url, tmp_path_factory.mktemp('postgres'))


@pytest.fixture(scope='session')
def postgres_dev_url(postgres_server: PostgresSettings) -> Iterator[str]:
    """An empty database for Atlas to compare schemas in."""
    settings = postgres_server._replace(database=f'dev_{uuid.uuid4().hex}')
    create_database(settings)

    try:
        yield atlas_url(settings, settings.password_file.read_text())
    finally:
        drop_database(settings)


@pytest.fixture
def fresh_postgres(postgres_server: PostgresSettings) -> Iterator[
    PostgresSettings
]:
    """A new, empty database for one test, such as a migration test."""
    settings = postgres_server._replace(database=f'test_{uuid.uuid4().hex}')
    create_database(settings)

    try:
        yield settings
    finally:
        drop_database(settings)


@pytest.fixture(scope='session')
def postgres_settings(postgres_server: PostgresSettings) -> Iterator[
    PostgresSettings
]:
    """A database migrated to head, shared by every test in the session."""
    settings = postgres_server._replace(database=f'test_{uuid.uuid4().hex}')
    create_database(settings)

    try:
        asyncio.run(upgrade(settings))
        yield settings
    finally:
        drop_database(settings)


@pytest.fixture
def postgres_session(postgres_settings: PostgresSettings) -> Iterator[Session]:
    """A session whose work rolls back after the test, even commits."""
    engine = sync_engine(postgres_settings)

    with engine.connect() as connection, connection.begin() as transaction:
        with Session(
            bind=connection,
            join_transaction_mode='create_savepoint',
        ) as session:
            yield session

        transaction.rollback()

    engine.dispose()


@pytest.fixture
async def async_postgres_session(
    postgres_settings: PostgresSettings,
) -> AsyncIterator[AsyncSession]:
    """The same for the web app's async sessions."""
    engine = async_engine(postgres_settings)
    connection: AsyncConnection

    async with engine.connect() as connection, connection.begin() as outer:
        async with AsyncSession(
            bind=connection,
            join_transaction_mode='create_savepoint',
            expire_on_commit=False,
        ) as session:
            yield session

        await outer.rollback()

    await engine.dispose()
