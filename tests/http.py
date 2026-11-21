import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from http import HTTPStatus

import aiohttp
from aiohttp.client_proto import ResponseHandler
from aiohttp.tracing import Trace


def http_reply(
    status: HTTPStatus, body: bytes = b"", /, **headers: str,
) -> bytes:
    lines = [
        f"HTTP/1.1 {status.value} {status.phrase}",
        f"Content-Length: {len(body)}",
        *(f"{name}: {value}" for name, value in headers.items()),
    ]

    return "\r\n".join([*lines, "", ""]).encode("ascii") + body


class MemoryHttpTransport(asyncio.Transport):
    def __init__(
        self, protocol: ResponseHandler, respond: Callable[[str], bytes],
    ) -> None:
        super().__init__()
        self.protocol = protocol
        self.respond = respond
        self.loop = asyncio.get_running_loop()
        self.closing = False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        request_line = bytes(data).split(b"\r\n", 1)[0]
        _, path, _ = request_line.decode("ascii").split(" ")
        # Let aiohttp install the response parser before delivering the bytes.
        self.loop.call_soon(self.protocol.data_received, self.respond(path))

    def is_closing(self) -> bool:
        return self.closing

    def close(self) -> None:
        if not self.closing:
            self.closing = True
            self.loop.call_soon(self.protocol.connection_lost, None)

    def abort(self) -> None:
        self.close()


class MemoryHttpConnector(aiohttp.BaseConnector):
    def __init__(self, respond: Callable[[str], bytes]) -> None:
        super().__init__()
        self.respond = respond

    async def _create_connection(
        self, req: aiohttp.ClientRequest, traces: list[Trace],
        timeout: aiohttp.ClientTimeout,  # noqa: ASYNC109
    ) -> ResponseHandler:
        protocol = ResponseHandler(asyncio.get_running_loop())
        protocol.connection_made(MemoryHttpTransport(protocol, self.respond))

        return protocol


@asynccontextmanager
async def local_http_client(
    respond: Callable[[str], bytes],
) -> AsyncGenerator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession(
        connector=MemoryHttpConnector(respond),
    ) as client:
        yield client
