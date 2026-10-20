"""A TCP relay that delays each direction by a fixed one-way latency.

It stands between a remote node's agentd and the broker, so everything that node
exchanges with Core (tasks, telemetry, control commands and confirmations) pays
the same network delay, as on the testbed's WAN links. Byte order is preserved:
each direction is a FIFO released at ``arrival + delay``. Used where ``tc
netem`` is unavailable; it shapes only connections the scenario routes through
it.

    python -m e2e.containment.relay --listen 4300 --upstream 4222 --delay-ms 25
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import time


async def _pump(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float
) -> None:
    queue: asyncio.Queue[tuple[float, bytes]] = asyncio.Queue()

    async def release() -> None:
        while True:
            due, data = await queue.get()
            if not data:
                writer.close()
                return
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            writer.write(data)
            await writer.drain()

    sender = asyncio.create_task(release())
    try:
        while data := await reader.read(65536):
            queue.put_nowait((time.monotonic() + delay, data))
    finally:
        queue.put_nowait((time.monotonic() + delay, b""))
        with contextlib.suppress(ConnectionError):
            await sender


async def serve(listen: int, upstream: int, delay: float) -> None:
    async def accept(reader, writer) -> None:
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                "127.0.0.1", upstream
            )
        except OSError:
            writer.close()
            return
        await asyncio.gather(
            _pump(reader, remote_writer, delay),
            _pump(remote_reader, writer, delay),
            return_exceptions=True,
        )

    server = await asyncio.start_server(accept, "127.0.0.1", listen)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    async with server:
        await stop.wait()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream", type=int, required=True)
    parser.add_argument("--delay-ms", type=float, required=True)
    args = parser.parse_args()
    asyncio.run(serve(args.listen, args.upstream, args.delay_ms / 1000))


if __name__ == "__main__":
    main()
