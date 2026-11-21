"""In-memory streams for exercising worker protocols without a subprocess."""

import asyncio
import json
import signal
from collections.abc import Awaitable, Callable

type Worker = Callable[
    [asyncio.StreamReader, asyncio.StreamReader], Awaitable[int],
]


def write_message(stream: asyncio.StreamReader, message: object) -> None:
    stream.feed_data(json.dumps(message).encode() + b"\n")


class PipeTransport(asyncio.Transport):
    def __init__(self, reader: asyncio.StreamReader) -> None:
        super().__init__()
        self.reader = reader
        self.closed = False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        assert not self.closed
        self.reader.feed_data(bytes(data))

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True
        self.reader.feed_eof()


class MemoryProcess:
    def __init__(self, worker: Worker) -> None:
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        self.stdin = asyncio.StreamWriter(
            PipeTransport(reader), asyncio.StreamReaderProtocol(reader),
            None, loop,
        )
        self.stdout = asyncio.StreamReader()
        self.returncode: int | None = None
        self.task = asyncio.create_task(self.run(worker, reader))

    async def run(self, worker: Worker, reader: asyncio.StreamReader) -> None:
        try:
            self.returncode = await worker(reader, self.stdout)
        finally:
            self.stdout.feed_eof()

    def kill(self) -> None:
        assert self.returncode is None
        self.returncode = -signal.SIGKILL
        self.task.cancel()
        self.stdout.feed_eof()

    async def wait(self) -> int:
        outcome, = await asyncio.gather(self.task, return_exceptions=True)

        if isinstance(outcome, BaseException) and not self.task.cancelled():
            raise outcome

        assert self.returncode is not None

        return self.returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        self.stdin.close()
        await self.wait()

        return await self.stdout.read(), b""
