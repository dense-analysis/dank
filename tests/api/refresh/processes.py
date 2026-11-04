"""Targets for the real processes these tests spawn.

They import only the standard library, so each starts quickly and none loads
the network guard, which belongs to the pytest process.
"""

from __future__ import annotations

import http.server
import os
import pathlib
import signal
import subprocess
import threading
import time
from multiprocessing.connection import Connection
from types import FrameType

EMPTY_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>Empty</title><link>http://feed.test/</link>
<description>Nothing yet</description>
</channel></rss>
"""


def stubborn_group_leader(ready: Connection) -> None:
    """Like a worker with a browser that won't close."""
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    browser = subprocess.Popen(["sleep", "60"])
    ready.send(browser.pid)
    time.sleep(60)


def stubborn_in_servers_group(ready: Connection) -> None:
    """Like a worker killed before it could call setsid."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ready.send(os.getpid())
    time.sleep(60)


def cooperative(ready: Connection, marker: str) -> None:
    def clean_up(signum: int, frame: FrameType | None) -> None:
        # Longer than a poll, so a stop that doesn't wait would miss the file.
        time.sleep(0.5)
        pathlib.Path(marker).write_text("cleaned up")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, clean_up)
    ready.send(os.getpid())
    time.sleep(60)


def serve_feeds(ready: Connection, entered: str) -> None:
    """A local feed server: /empty.xml answers, /hang.xml never does."""
    forever = threading.Event()

    class Feeds(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/hang.xml":
                pathlib.Path(entered).write_text("entered")
                forever.wait()

            self.send_response(200)
            self.send_header("Content-Type", "application/rss+xml")
            self.send_header("Content-Length", str(len(EMPTY_FEED)))
            self.end_headers()
            self.wfile.write(EMPTY_FEED)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Feeds)
    ready.send(server.server_address[1])
    server.serve_forever()
