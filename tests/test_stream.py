import asyncio
import json

import pytest
from websockets.asyncio.server import serve

import oddssea
from oddssea import _stream
from oddssea._stream import Stream


class Connection:
    """The server's side of one connection. It numbers the messages as the API does."""

    def __init__(self, ws):
        self.ws = ws
        self.seq = 0

    async def send(self, type, **fields):
        self.seq += 1
        await self.ws.send(json.dumps({"type": type, "seq": self.seq, "sentAt": 0, **fields}))

    async def snapshot(self, *events):
        for e in events:
            await self.send("snapshot", sport="soccer", event=e)
        await self.send("ready")

    async def update(self, e):
        await self.send("update", sport="soccer", event=e)

    async def error(self, code):
        await self.ws.send(json.dumps({"type": "error", "code": code, "message": "no"}))
        await self.ws.close(4003)


class Server:
    """A local stream server. script(connection, n) plays the n-th connection, from 0."""

    def __init__(self, script):
        self.script = script
        self.connections = 0

    async def __aenter__(self):
        self._server = await serve(self._handle, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc_info):
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, ws):
        n = self.connections
        self.connections += 1
        await ws.recv()     # the subscribe
        await self.script(Connection(ws), n)


def event(event_id, price):
    return {"id": event_id, "sportId": 29, "markets": [{"prices": {"home": price}}]}


def summary(message):
    """A message as (type, event id, price), shorter to compare."""
    if message["type"] in ("snapshot", "update"):
        e = message["event"]
        return message["type"], e["id"], e["markets"][0]["prices"]["home"]
    if message["type"] == "remove":
        return "remove", message["eventId"]
    return (message["type"],)


async def read(stream, count):
    async def take():
        return [summary(await anext(stream)) for _ in range(count)]

    return await asyncio.wait_for(take(), 5)


async def nothing_more(stream):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(stream), 0.2)


def play(script, body):
    """Run body(stream, server) against a server playing script."""

    async def main():
        async with Server(script) as server, Stream(server.url, {}, {"op": "subscribe"}) as stream:
            await body(stream, server)

    asyncio.run(main())


@pytest.fixture(autouse=True)
def quick_reconnects(monkeypatch):
    monkeypatch.setattr(_stream, "_backoff", lambda failures: 0.01)


def test_snapshot_then_changes():
    async def script(conn, n):
        await conn.snapshot(event(1, 2.0), event(2, 3.0))
        await conn.update(event(1, 2.1))
        await conn.send("remove", sportId=29, eventId=2)
        await conn.send("limit", eventId=1, changes=[])
        await conn.ws.wait_closed()

    async def body(stream, server):
        assert not stream.connected
        assert await read(stream, 3) == [("snapshot", 1, 2.0), ("snapshot", 2, 3.0), ("ready",)]
        assert stream.connected
        assert await read(stream, 3) == [("update", 1, 2.1), ("remove", 2), ("limit",)]
        assert stream.events == {1: event(1, 2.1)}

    play(script, body)


def test_lost_connection_gives_what_changed():
    async def script(conn, n):
        if n == 0:
            await conn.snapshot(event(1, 2.0), event(2, 3.0), event(3, 4.0))
            await conn.ws.close(1011)
        else:
            await conn.snapshot(event(1, 2.0), event(2, 3.5), event(4, 5.0))
            await conn.ws.wait_closed()

    async def body(stream, server):
        await read(stream, 4)
        assert await read(stream, 1) == [("disconnect",)]
        assert not stream.connected
        assert await read(stream, 4) == [("update", 2, 3.5), ("update", 4, 5.0), ("remove", 3), ("ready",)]
        assert stream.connected
        assert sorted(stream.events) == [1, 2, 4]

    play(script, body)


def test_missing_message_reconnects():
    async def script(conn, n):
        await conn.snapshot(event(1, 2.0))
        if n == 0:
            conn.seq += 1       # as if one message never arrived
            await conn.update(event(1, 9.9))
        await conn.ws.wait_closed()

    async def body(stream, server):
        assert await read(stream, 3) == [("snapshot", 1, 2.0), ("ready",), ("disconnect",)]
        assert await read(stream, 1) == [("ready",)]

    play(script, body)


def test_handover_is_not_noticed():
    """The server announces a close: the stream moves to a new connection and repeats nothing."""
    old_sent = asyncio.Event()
    new_connected = asyncio.Event()

    async def script(conn, n):
        if n == 0:
            await conn.snapshot(event(1, 2.0), event(2, 3.0))
            await conn.send("reconnect", closesAt=0, message="closing soon")
            await new_connected.wait()
            await conn.update(event(2, 3.1))
            await conn.send("opened", eventId=2, markets=[])
            await conn.update(event(3, 6.0))
            old_sent.set()
            await conn.ws.wait_closed()
        else:
            # its snapshot is from before the old connection's last messages
            new_connected.set()
            await old_sent.wait()
            await asyncio.sleep(0.05)
            await conn.snapshot(event(1, 2.0), event(2, 3.0))
            await conn.update(event(2, 3.1))
            await conn.send("opened", eventId=2, markets=[])
            await conn.update(event(3, 6.0))
            await conn.update(event(1, 2.2))
            await conn.ws.wait_closed()

    async def body(stream, server):
        await read(stream, 3)
        assert await read(stream, 4) == [("update", 2, 3.1), ("opened",), ("update", 3, 6.0), ("update", 1, 2.2)]
        assert stream.connected
        assert server.connections == 2
        assert stream.events == {1: event(1, 2.2), 2: event(2, 3.1), 3: event(3, 6.0)}
        await nothing_more(stream)

    play(script, body)


def test_handover_passes_on_what_the_old_connection_missed():
    async def script(conn, n):
        if n == 0:
            await conn.snapshot(event(1, 2.0), event(2, 3.0))
            await conn.send("reconnect", closesAt=0, message="closing soon")
            await conn.ws.wait_closed()
        else:
            await conn.snapshot(event(1, 2.4))
            await conn.ws.wait_closed()

    async def body(stream, server):
        await read(stream, 3)
        assert await read(stream, 2) == [("update", 1, 2.4), ("remove", 2)]
        await nothing_more(stream)

    play(script, body)


def test_failed_handover_keeps_the_old_connection():
    async def script(conn, n):
        if n == 0:
            await conn.snapshot(event(1, 2.0))
            await conn.send("reconnect", closesAt=0, message="closing soon")
            await asyncio.sleep(0.1)
            await conn.update(event(1, 2.1))
            await conn.ws.wait_closed()
        else:
            await conn.ws.close(1013)

    async def body(stream, server):
        assert await read(stream, 3) == [("snapshot", 1, 2.0), ("ready",), ("update", 1, 2.1)]
        assert stream.connected

    play(script, body)


def test_key_not_allowed_raises():
    async def script(conn, n):
        await conn.error("PLAN_REQUIRED")

    async def body(stream, server):
        with pytest.raises(oddssea.APIError) as caught:
            await read(stream, 1)
        assert caught.value.code == "PLAN_REQUIRED"
        assert caught.value.status is None

    play(script, body)


def test_connections_in_use_is_tried_again():
    async def script(conn, n):
        if n == 0:
            await conn.error("TOO_MANY_CONNECTIONS")
        else:
            await conn.snapshot(event(1, 2.0))
            await conn.ws.wait_closed()

    async def body(stream, server):
        assert await read(stream, 2) == [("snapshot", 1, 2.0), ("ready",)]

    play(script, body)


def test_gives_up_when_it_never_connects():
    async def script(conn, n):
        await conn.error("TOO_MANY_CONNECTIONS")

    async def body(stream, server):
        with pytest.raises(oddssea.APIError):
            await read(stream, 1)
        assert server.connections == _stream.FIRST_ATTEMPTS

    play(script, body)


def test_unreachable_server(monkeypatch):
    monkeypatch.setattr(_stream, "FIRST_ATTEMPTS", 1)     # a refused connection takes seconds on Windows

    async def main():
        async with Stream("ws://127.0.0.1:1", {}, {"op": "subscribe"}) as stream:
            with pytest.raises(oddssea.NetworkError):
                await read(stream, 1)

    asyncio.run(main())


def test_sync_stream():
    async def script(conn, n):
        await conn.snapshot(event(1, 2.0))
        await conn.update(event(1, 2.1))
        await conn.ws.wait_closed()

    with oddssea.Client("test-key") as client:
        server = client._wait(Server(script).__aenter__())
        client._async._base_url = server.url.replace("ws", "http")
        with client.stream("soccer") as stream:
            messages = [summary(next(stream)) for _ in range(3)]
            assert messages == [("snapshot", 1, 2.0), ("ready",), ("update", 1, 2.1)]
            assert stream.events == {1: event(1, 2.1)}
        client._wait(server.__aexit__())
