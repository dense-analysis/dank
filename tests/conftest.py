from __future__ import annotations

import asyncio
import fcntl
import os
import pathlib
import sqlite3
import subprocess
import sys
import threading
import urllib.request
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager, nullcontext
from typing import Any, Never, TextIO

import clickhouse_connect
import psycopg
import pytest
from psycopg import generators
from pyfakefs import pytest_plugin as fakefs
from pyfakefs.fake_file import FakeDirectory, FakeFile
from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import Patcher, Pause

# Keep pyfakefs's fixtures while replacing its reporting hooks.
fs = fakefs.fs
fs_class = fakefs.fs_class
fs_module = fakefs.fs_module
fs_session = fakefs.fs_session

_LIVE_IO_MARKERS = ('clickhouse', 'shell_io', 'embeddings')
_ROOT = pathlib.Path(__file__).resolve().parents[1]
_CHECKOUT_PARENTS = _ROOT.parts[1:-1]
_TEST_DIRECTORY = str(_ROOT / 'tests') + os.sep
# Pytest reads these public inputs once, alongside the test modules.
_CHECKOUT_INPUTS = {
    path.relative_to(_ROOT): path.read_bytes()
    for path in (
        *(_ROOT / name for name in (
            '.gitignore', 'Makefile', 'README.md', 'pyproject.toml',
            'schema.sql', 'compose.yaml',
            'reader/openapi.json', 'src/dank/container_entrypoint.sh',
            'src/dank/db/migrations/atlas.sum',
        )),
        *(_ROOT / 'src/dank/db/migrations').glob('*.sql'),
        _ROOT / 'config.example.toml',
        _ROOT / 'config.native.example.toml',
        *(_ROOT / 'src/dank/storage').glob('*.sql'),
        _ROOT / 'src/dank/scrape/history.sql',
        *(_ROOT / 'tests/fixtures').glob('*.html'),
        *(_ROOT / 'tests/fixtures').glob('*.json'),
    )
}

_CHECKOUT_DIRS = sorted({
    parent for path in _CHECKOUT_INPUTS for parent in path.parents
    if parent.parts
}, key=lambda path: len(path.parts))


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item],
) -> None:
    # Retain the live cases while their assertions are converted to unit tests.
    deferred = [
        item for item in items
        if any(item.get_closest_marker(marker) for marker in _LIVE_IO_MARKERS)
    ]

    if deferred:
        config.hook.pytest_deselected(items=deferred)
        excluded = set(deferred)
        items[:] = [item for item in items if item not in excluded]

        # The compact forkserver summary counts only the items it received.
        reporter: Any = config.pluginmanager.get_plugin('terminalreporter')

        if reporter is not None and not hasattr(config, 'workerinput'):
            reporter.write_line(
                f'{len(deferred)} cases deselected '
                '(live ClickHouse, shell or embedding model)',
            )
            reporter.flush()


def pytest_report_teststatus(report: Any, config: Any) -> Any:
    if report.passed and report.when == 'call':
        return report.outcome, '', report.outcome.upper()


@pytest.hookimpl(trylast=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    # Cache pytest's paths while all of its Path objects are still real.
    for item in session.items:
        _ = item.location


def _report_filesystem(*, failed: bool) -> Pause | nullcontext[None]:
    patcher = Patcher.PATCHER

    if failed and patcher is not None and patcher.is_patching:
        return Pause(patcher)

    return nullcontext()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(
    call: pytest.CallInfo[None],
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    # pyfakefs normally restores and reapplies the mocks around every report.
    # Tracebacks need the checkout. Passing reports already have their paths.
    with _report_filesystem(failed=call.excinfo is not None):
        return (yield)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_logreport(report: pytest.TestReport) -> Iterator[None]:
    with _report_filesystem(failed=not report.passed):
        yield


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish() -> None:
    Patcher.clear_fs_cache()


class CheckoutDirectory(FakeDirectory):
    """Mount the collected checkout inputs only when a test looks them up."""

    def __init__(self, name: str, filesystem: FakeFilesystem) -> None:
        super().__init__(name, filesystem=filesystem)
        self._populated = False
        self._mount_lock = threading.RLock()

    def _populate(self) -> None:
        # add_entry also consults entries. Keep other threads out while the
        # mounting thread fills the directory.
        with self._mount_lock:
            if self._populated:
                return

            self._populated = True
            filesystem = self.filesystem
            directories: dict[pathlib.PurePath, FakeDirectory] = {
                pathlib.PurePath('.'): self,
            }

            for path in _CHECKOUT_DIRS:
                child = FakeDirectory(path.name, filesystem=filesystem)
                directories[path.parent].add_entry(child)
                directories[path] = child

            for path, contents in _CHECKOUT_INPUTS.items():
                directories[path.parent].add_entry(FakeFile(
                    os.fsencode(path.name), contents=contents,
                    filesystem=filesystem,
                ))

    @property
    def entries(self) -> dict[str, FakeFile]:
        self._populate()

        return super().entries

    @property
    def ordered_dirs(self) -> list[str]:
        self._populate()

        return super().ordered_dirs


@pytest.fixture(autouse=True)
def in_memory_files(fs_session: FakeFilesystem) -> None:
    fs_session.reset(init_pathlib=False)
    parent = fs_session.root_dir

    if (
        fs_session.is_case_sensitive and _CHECKOUT_PARENTS
        and _CHECKOUT_PARENTS[0] not in parent.entries
    ):
        # The entire path is new. Insert it one component at a time.
        perm_bits = 0o777 & ~fs_session.umask

        for name in _CHECKOUT_PARENTS:
            child = FakeDirectory(
                name, perm_bits=perm_bits, filesystem=fs_session,
            )
            parent.add_entry(child)
            parent = child
    else:
        parent_path = pathlib.Path(_ROOT.parent)
        parent_path.mkdir(parents=True, exist_ok=True)
        parent = fs_session.resolve(str(parent_path))

        assert isinstance(parent, FakeDirectory)

    parent.add_entry(CheckoutDirectory(_ROOT.name, filesystem=fs_session))
    fs_session.cwd = str(_ROOT)


@pytest.fixture
def memory_path() -> pathlib.Path:
    path = pathlib.Path('/fixture')
    path.mkdir()

    return path


@pytest.fixture
def tmp_path(
    memory_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> pathlib.Path:
    # pyfakefs treats flock as a no-op. Collection and browser profile locks
    # belong to an open handle and are released when that handle is closed.
    locks: dict[str, list[tuple[TextIO, int]]] = {}

    def flock(handle: TextIO, operation: int) -> None:
        mode = operation & ~fcntl.LOCK_NB
        others = [
            (owner, lock) for owner, lock in locks.get(handle.name, [])
            if owner is not handle and not owner.closed
        ]

        if mode != fcntl.LOCK_UN:
            if any(fcntl.LOCK_EX in (mode, lock) for _, lock in others):
                raise BlockingIOError(handle.name)

            others.append((handle, mode))

        locks[handle.name] = others

    monkeypatch.setattr(fcntl, 'flock', flock)

    return memory_path


def _network_disabled(*args: Any, **kwargs: Any) -> Never:
    raise RuntimeError('Network is disabled in tests')


def _database_disabled(*args: Any, **kwargs: Any) -> Never:
    raise RuntimeError('Database connections are disabled in tests')


def _subprocess_disabled(*args: Any, **kwargs: Any) -> Never:
    raise RuntimeError('Subprocesses are disabled in tests')


_SOCKET_EVENTS = frozenset({
    'socket.__new__', 'socket.bind', 'socket.connect', 'socket.getaddrinfo',
    'socket.gethostbyaddr', 'socket.gethostbyname', 'socket.getnameinfo',
    'socket.sendmsg', 'socket.sendto',
})
_SOCKETPAIR_EVENTS = {'socket.__new__', 'socket.bind', 'socket.connect'}
_PROCESS_EVENTS = frozenset({
    'subprocess.Popen', '_posixsubprocess.fork_exec', 'os.exec', 'os.fork',
    'os.forkpty', 'os.kill', 'os.killpg', 'os.posix_spawn', 'os.spawn',
    'os.startfile', 'os.startfile/2', 'os.system',
})
_AUDIT_BLOCKERS = {
    **dict.fromkeys(_SOCKET_EVENTS, _network_disabled),
    **dict.fromkeys(_PROCESS_EVENTS, _subprocess_disabled),
    'sqlite3.connect': _database_disabled,
    'urllib.Request': _network_disabled,
}


_FILE_EVENTS = frozenset({
    'open', 'os.chdir', 'os.chmod', 'os.chown', 'os.link', 'os.listdir',
    'os.mkdir', 'os.remove', 'os.rename', 'os.rmdir', 'os.scandir',
    'os.symlink', 'os.truncate', 'os.utime',
})


def _check_real_file_access(event: str) -> None:
    # Imports and traceback formatting still need Python source. Walk the
    # stack only for a syscall that escaped the in-memory filesystem.
    frame = sys._getframe(2)  # pyright: ignore[reportPrivateUsage]

    while frame is not None:
        module = frame.f_globals.get('__name__', '')

        if module in {'importlib._bootstrap_external', 'linecache',
                      'pytest_fast'}:
            return

        if module.startswith(('_pytest.', 'pluggy.', 'xdist.', 'execnet.',
                              'coverage.')):
            return

        if (module.startswith(('dank.', 'tests.'))
                or frame.f_code.co_filename.startswith(_TEST_DIRECTORY)):
            break

        frame = frame.f_back

    # A callable submitted directly to a thread pool has no test frames.
    raise RuntimeError(f'Real filesystem I/O is disabled in tests ({event})')


@contextmanager
def block_audited_io(*, files: bool = False) -> Generator[pytest.MonkeyPatch]:
    active = True
    local = threading.local()
    make_self_pipe: Callable[[asyncio.SelectorEventLoop], None] = getattr(  # noqa: B009
        asyncio.SelectorEventLoop, '_make_self_pipe',
    )

    def audit(event: str, _args: tuple[object, ...]) -> None:
        if active and files and event in _FILE_EVENTS:
            _check_real_file_access(event)

        blocked = _AUDIT_BLOCKERS.get(event) if active else None

        if blocked is None:
            return

        if (
            event in _SOCKETPAIR_EVENTS
            and getattr(local, 'in_socketpair', False)
        ):
            return

        blocked()

    def local_wakeup_pipe(loop: asyncio.SelectorEventLoop) -> None:
        # asyncio uses a connected socket pair for waking its event loop.
        local.in_socketpair = True

        try:
            make_self_pipe(loop)
        finally:
            local.in_socketpair = False

    sys.addaudithook(audit)

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                asyncio.SelectorEventLoop,
                '_make_self_pipe',
                local_wakeup_pipe,
            )
            # libpq opens connections without calling Python's socket module.
            patch.setattr(generators, 'connect', _database_disabled)
            yield patch
    finally:
        # Python audit hooks remain registered for the process lifetime.
        active = False


@pytest.hookimpl(wrapper=True)
def pytest_collection() -> Iterator[object]:
    with block_audited_io():
        yield


@pytest.fixture(scope='session', autouse=True)
def block_external_io() -> Iterator[None]:
    with block_audited_io(files=True) as patch:
        patch.setattr(urllib.request, 'urlopen', _network_disabled)
        patch.setattr(urllib.request.OpenerDirector, 'open', _network_disabled)
        patch.setattr(subprocess, 'Popen', _subprocess_disabled)

        for target in (psycopg, psycopg.Connection, psycopg.AsyncConnection):
            patch.setattr(target, 'connect', _database_disabled)

        for name in ('get_client', 'get_async_client', 'create_client'):
            patch.setattr(clickhouse_connect, name, _database_disabled)

        patch.setattr(sqlite3, 'connect', _database_disabled)
        yield
