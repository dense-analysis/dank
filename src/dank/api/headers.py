"""Response headers every route gets, and HEAD for every GET."""

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

NO_CACHE = "no-store, no-cache, must-revalidate, max-age=0"
# Their mounts answer HEAD themselves. Run as a GET, a HEAD on a video would
# read the whole file only to throw it away.
FILE_PATHS = ("/assets/", "/static/", "/reader/")


class ApiHeaders:
    """Pure ASGI middleware, so streamed files pass through untouched.

    * /api/* gets "Cache-Control: no-store" unless the route set its own,
      which is how source icons keep their day-long cache.
    * /static/* and HTML get today's no-cache headers.
    * HEAD runs the GET route and drops the body. FastAPI, unlike aiohttp,
      doesn't answer HEAD on its own, though the file mounts do.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send,
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)

            return

        head = scope["method"] == "HEAD"
        path: str = scope["path"]

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                html = headers.get("content-type", "").startswith("text/html")

                if path.startswith("/api/"):
                    headers.setdefault("cache-control", "no-store")
                elif path.startswith("/static/") or html:
                    headers["cache-control"] = NO_CACHE
                    headers["pragma"] = "no-cache"
                    headers["expires"] = "0"
            elif head:
                message = {**message, "body": b""}

            await send(message)

        if head and not path.startswith(FILE_PATHS):
            scope = {**scope, "method": "GET"}

        await self.app(scope, receive, send_with_headers)
