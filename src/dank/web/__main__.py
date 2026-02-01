from __future__ import annotations

import argparse
import os
import pathlib
import sys
import threading
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:

    class _InotifyEvent:
        wd: int
        mask: int
        name: str

    class _Flags:
        MODIFY: int
        CLOSE_WRITE: int
        MOVED_TO: int
        CREATE: int
        DELETE: int
        MOVE_SELF: int
        DELETE_SELF: int
        ISDIR: int

        def from_mask(self, mask: int) -> list[int]: ...

    class INotify:
        def add_watch(self, path: str, mask: int) -> int: ...

        def read(self) -> list[_InotifyEvent]: ...

    flags: _Flags
else:
    from inotify_simple import (  # type: ignore[reportMissingTypeStubs]
        INotify,
        flags,
    )

from dank.config import load_settings
from dank.web.app import (
    DEFAULT_HOST,
    DEFAULT_PAGE_SIZE,
    DEFAULT_PORT,
    MAX_PAGE_SIZE,
    create_app,
)


def main() -> None:
    parser = argparse.ArgumentParser(prog="dank.web")
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config.toml",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="Host to bind (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port to bind",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Posts per page",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the server when files change",
    )
    args = parser.parse_args()
    settings = load_settings(args.config)
    page_size = args.limit

    if page_size <= 0:
        page_size = DEFAULT_PAGE_SIZE

    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    if args.reload:
        _start_reloader()

    app = create_app(settings, page_size=page_size)
    web.run_app(app, host=args.host, port=args.port)


def _start_reloader() -> None:
    watch_paths = _watch_paths()
    extensions = {".py", ".css", ".html", ".js", ".svg"}
    inotify = INotify()
    watch_map: dict[int, pathlib.Path] = {}
    watched: set[pathlib.Path] = set()
    mask = _watch_mask()
    timer: threading.Timer | None = None

    for path in watch_paths:
        _add_watch_tree(inotify, watch_map, watched, path, mask)

    def _watch() -> None:
        nonlocal timer

        while True:
            for event in inotify.read():
                base = watch_map.get(event.wd)

                if base is None:
                    continue

                event_flags = set(flags.from_mask(event.mask))
                name = event.name
                path = base / name if name else base

                if flags.ISDIR in event_flags:
                    if (
                        flags.CREATE in event_flags
                        or flags.MOVED_TO in event_flags
                    ):
                        _add_watch_tree(
                            inotify,
                            watch_map,
                            watched,
                            path,
                            mask,
                        )
                    continue

                if path.suffix in extensions:
                    if timer is not None:
                        timer.cancel()

                    timer = threading.Timer(0.4, _restart_process)
                    timer.daemon = True
                    timer.start()

    thread = threading.Thread(target=_watch, daemon=True)
    thread.start()


def _watch_paths() -> list[pathlib.Path]:
    root = pathlib.Path(__file__).resolve().parents[3]

    return [root / "src", root / "static"]


def _watch_mask() -> int:
    return (
        flags.MODIFY
        | flags.CLOSE_WRITE
        | flags.MOVED_TO
        | flags.CREATE
        | flags.DELETE
        | flags.MOVE_SELF
        | flags.DELETE_SELF
    )


def _add_watch_tree(
    inotify: INotify,
    watch_map: dict[int, pathlib.Path],
    watched: set[pathlib.Path],
    path: pathlib.Path,
    mask: int,
) -> None:
    if not path.exists():
        return

    resolved = path.resolve()

    if resolved in watched:
        return

    if resolved.is_dir():
        wd = inotify.add_watch(str(resolved), mask)
        watch_map[wd] = resolved
        watched.add(resolved)

        for entry in resolved.rglob("*"):
            if entry.is_dir():
                _add_watch_tree(inotify, watch_map, watched, entry, mask)


def _restart_process() -> None:
    args = ["-m", "dank.web", *sys.argv[1:]]
    os.execv(sys.executable, [sys.executable, *args])


if __name__ == "__main__":
    main()
