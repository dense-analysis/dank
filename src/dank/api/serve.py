"""`web --server fastapi`: uvicorn, with watchfiles for reloading."""

import pathlib
from typing import NamedTuple

import uvicorn
import watchfiles

from dank.api.app import create_app
from dank.config import load_settings

SRC_DIR = pathlib.Path(__file__).parents[1]
# In-flight requests get this long at shutdown. The lifespan then waits for
# refresh workers (about 95 s), which keeps the total inside compose.yaml's
# 110 s stop_grace_period.
REQUEST_DRAIN_SECONDS = 10


class ServeOptions(NamedTuple):
    config: str
    host: str
    port: int


def serve(options: ServeOptions, *, reload: bool) -> None:
    if reload:
        # watchfiles restarts serve_once in a child process and hands it
        # the options as arguments, so no environment variable carries
        # configuration. It works on macOS, unlike inotify.
        watchfiles.run_process(SRC_DIR, target=serve_once, args=(options,))
    else:
        serve_once(options)


def serve_once(options: ServeOptions) -> None:
    settings = load_settings(options.config)
    app = create_app(settings)
    uvicorn.run(
        app,
        host=options.host,
        port=options.port,
        # Forwarded headers stay off: see same_origin in guards.py.
        proxy_headers=False,
        timeout_graceful_shutdown=REQUEST_DRAIN_SECONDS,
    )
