"""Response headers every route gets, and HEAD for every GET."""

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

NO_CACHE = "no-store, no-cache, must-revalidate, max-age=0"
# The only routes. Everything else is a file mount, which answers HEAD itself:
# run as a GET, a HEAD on a video would read the whole file only to throw it
# away.
ROUTE_PATHS = ("/api/", "/reader")


class ApiHeaders:
    """Pure ASGI middleware, so streamed files pass through untouched.

    * /api/* gets "Cache-Control: no-store" unless the route set its own,
      which is how source icons keep their day-long cache.
    * HTML gets no-cache headers.
    * HEAD runs the GET route and drops the body. FastAPI doesn't answer
      HEAD on its own, though the file mounts do.
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
                elif html:
                    headers["cache-control"] = NO_CACHE
                    headers["pragma"] = "no-cache"
                    headers["expires"] = "0"
            elif head:
                message = {**message, "body": b""}

            await send(message)

        if head and path.startswith(ROUTE_PATHS):
            scope = {**scope, "method": "GET"}

        await self.app(scope, receive, send_with_headers)
