"""Owned TCP relay for severing only a fixture's Core/Leaf connection."""

import asyncio
from contextlib import suppress


class CuttableLink:
    def __init__(self, upstream_port):
        self.upstream_port = upstream_port
        self.port = 0
        self.server = None
        self.connections = set()

    async def start(self):
        assert self.server is None
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)
        self.port = self.server.sockets[0].getsockname()[1]

    def _accept(self, reader, writer):
        task = asyncio.create_task(self._relay(reader, writer))
        self.connections.add(task)
        task.add_done_callback(self.connections.discard)

    async def _relay(self, reader, writer):
        upstream = None
        pumps = []
        try:
            remote, upstream = await asyncio.open_connection(
                "127.0.0.1", self.upstream_port
            )

            async def pump(source, target):
                while data := await source.read(65536):
                    target.write(data)
                    await target.drain()

            pumps = [
                asyncio.create_task(pump(reader, upstream)),
                asyncio.create_task(pump(remote, writer)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except OSError:
            pass  # The owned upstream may be intentionally unavailable.
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for stream in (writer, upstream):
                if stream is not None:
                    stream.close()
                    with suppress(OSError):
                        await stream.wait_closed()

    async def cut(self):
        server, self.server = self.server, None
        if server is not None:
            server.close()
        await asyncio.sleep(0)
        tasks = list(self.connections)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert not self.connections
        if server is not None:
            # Server closure waits for active transports: close relay connections
            # first, otherwise the test waits forever before injecting the cut.
            await server.wait_closed()
