"""A 413 for request bodies over 1 MiB, before they are read into memory."""

from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dank.api.errors import CODE_BY_STATUS, envelope
from dank.api.guards import SAFE_METHODS

# aiohttp's default client_max_size, which the API had until DEN-51 replaced
# aiohttp with FastAPI. FastAPI has no limit of its own.
MAX_BODY_BYTES = 1024 * 1024
TOO_LARGE = "The request is larger than 1 MiB."


class BodyLimit:
    """Pure ASGI middleware, because FastAPI reads the whole body before any
    dependency runs, so a route can't check the size in time.

    * Unsafe /api/* requests whose Content-Length is over the limit answer 413
      without reading anything.
    * Otherwise bytes are counted as they arrive, which covers chunked bodies
      and a Content-Length that undercounts. Reading stops at the first chunk
      over the limit, and the error handlers answer 413.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send,
    ) -> None:
        if (
            scope["type"] != "http"
            or scope["method"] in SAFE_METHODS
            or not scope["path"].startswith("/api/")
        ):
            await self.app(scope, receive, send)

            return

        length = Headers(scope=scope).get("content-length", "")

        if length.isdigit() and int(length) > MAX_BODY_BYTES:
            response = envelope(413, TOO_LARGE, CODE_BY_STATUS[413])
            await response(scope, receive, send)

            return

        received = 0

        async def counted_receive() -> Message:
            nonlocal received
            message = await receive()

            if message["type"] == "http.request":
                received += len(message.get("body", b""))

                if received > MAX_BODY_BYTES:
                    raise HTTPException(413, TOO_LARGE)

            return message

        await self.app(scope, counted_receive, send)
