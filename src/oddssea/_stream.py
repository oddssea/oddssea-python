from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import AsyncGenerator, Iterator
from typing import Self

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from ._version import USER_AGENT
from .errors import APIError, NetworkError
from .types import Event, Message

log = logging.getLogger("oddssea")

# Errors that connecting again can't fix.
FATAL = {"UNAUTHORIZED", "PLAN_REQUIRED", "INVALID_REQUEST_DATA"}

FIRST_ATTEMPTS = 3      # tries before giving up on a stream that has never worked
MAX_BACKOFF = 30.0
REPLAY_S = 5.0          # how long after a handover the new connection may repeat the old one
INBOX_SIZE = 1000


class Stream:
    """A stream subscription: iterate it for the API's messages.

        async with client.stream("soccer") as stream:
            async for message in stream:
                ...

    Messages are the ones at https://oddssea.dev/docs#messages. The first
    connection gives a "snapshot" per event and then "ready"; after that come
    "update", "remove" and, if asked for, "opened" and "limit".

    The stream reconnects by itself. When the connection is lost you get
    {"type": "disconnect"}. Once it's back you get what changed meanwhile, as
    "update" and "remove" messages, and then "ready". When the server announces
    that it will close the connection, the stream moves to a new one before that
    happens and you get nothing extra.

    events holds the current events by id, as of the messages you've read.
    connected is False between "disconnect" and the next "ready".
    """

    def __init__(self, url: str, headers: dict[str, str], subscription: dict):
        self.events: dict[int, Event] = {}
        self.connected = False
        self._url = url
        self._headers = headers
        self._subscription = json.dumps(subscription)
        self._inbox: asyncio.Queue = asyncio.Queue(INBOX_SIZE)
        self._tasks: set[asyncio.Task] = set()
        self._synced = False                    # a first "ready" has been given
        self._overlap: _Overlap | None = None
        self._messages = self._run()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Message:
        return await self._messages.__anext__()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def close(self) -> None:
        await self._messages.aclose()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.connected = False

    async def _run(self) -> AsyncGenerator[Message, None]:
        # Each connection is a task that puts what it receives, or the error that
        # ended it, in the inbox. live is the one being passed on, starting is
        # one on its way: its messages are held back until its "ready".
        live: asyncio.Task | None = None
        starting: asyncio.Task | None = self._connect()
        held: list[Message] = []
        failures = 0

        while True:
            source, item = await self._inbox.get()

            if isinstance(item, Exception):
                if source is live:
                    live = None
                    if starting:
                        continue        # closed before its replacement was ready: wait for that
                elif source is starting:
                    starting, held = None, []
                    if live:
                        self._overlap = None
                        continue        # the old connection still works, until the server closes it
                else:
                    continue

                failures += 1
                if not _temporary(item) or (not self._synced and failures == FIRST_ATTEMPTS):
                    raise _for_caller(item)
                if self.connected:
                    self.connected = False
                    self._overlap = None
                    yield {"type": "disconnect"}
                wait = _backoff(failures)
                log.info("stream connection lost (%s), connecting again in %.1f s", item, wait)
                await asyncio.sleep(wait)
                starting = self._connect()

            elif source is starting:
                held.append(item)
                if item["type"] == "ready":
                    for message in self._switch(held):
                        yield message
                    if live:
                        live.cancel()
                    live, starting, held, failures = starting, None, [], 0

            elif source is live:
                if item["type"] == "reconnect":
                    if not starting:
                        self._overlap = _Overlap(self.events)
                        starting = self._connect()
                elif self._apply(item):
                    yield item

    def _connect(self) -> asyncio.Task:
        task = asyncio.create_task(self._receive())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _receive(self) -> None:
        """One connection, from the subscribe to whatever ends it."""
        me = asyncio.current_task()
        try:
            async with connect(self._url, additional_headers=self._headers, user_agent_header=USER_AGENT) as ws:
                await ws.send(self._subscription)
                seq = None
                async for raw in ws:
                    message = json.loads(raw)
                    if message["type"] == "error":
                        raise APIError(None, message["code"], message["message"])
                    if seq is not None and message["seq"] != seq + 1:
                        raise ConnectionError("a message went missing")
                    seq = message["seq"]
                    await self._inbox.put((me, message))
                raise ConnectionError("closed by the server")
        except Exception as exc:
            await self._inbox.put((me, exc))

    def _switch(self, held: list[Message]) -> Iterator[Message]:
        """Go over to a connection that has sent its snapshot, passing on what's new in it."""
        overlap = self._overlap if self.connected else None
        fresh = set()
        for message in held:
            if message["type"] == "snapshot":
                event = message["event"]
                fresh.add(event["id"])
                if self.events.get(event["id"]) == event or (overlap and overlap.saw(event)):
                    continue
                self.events[event["id"]] = event
                yield message if not self._synced else {**message, "type": "update"}
            elif message["type"] != "ready":
                yield message

        for event_id in self.events.keys() - fresh:
            if overlap and overlap.touched(event_id):
                continue        # appeared after the snapshot was taken
            event = self.events.pop(event_id)
            yield {"type": "remove", "sportId": event["sportId"], "eventId": event_id}

        if overlap:
            overlap.ended = time.monotonic()
        else:
            self.connected = self._synced = True
            yield held[-1]

    def _apply(self, message: Message) -> bool:
        """Bring events up to date with a message. False if the caller already has it."""
        overlap = self._overlap
        if overlap:
            if overlap.ended is None:
                overlap.note(message)
            elif time.monotonic() - overlap.ended > REPLAY_S:
                self._overlap = None
            elif overlap.gave(message):
                return False

        if message["type"] == "update":
            self.events[message["event"]["id"]] = message["event"]
        elif message["type"] == "remove":
            return self.events.pop(message["eventId"], None) is not None
        return True


class _Overlap:
    """What the caller was given while two connections were open.

    Before the server closes a connection it says so, and the stream opens a new
    one while the old one keeps delivering. The new connection's snapshot is a
    little older than the old connection's latest messages, and it sends
    everything after its snapshot again. This remembers what was passed on from
    the old one so those repeats can be left out.
    """

    def __init__(self, events: dict[int, Event]):
        self.before = dict(events)
        self.given: list[Message] = []
        self.ended: float | None = None     # when the new connection took over

    def note(self, message: Message) -> None:
        self.given.append(_content(message))

    def gave(self, message: Message) -> bool:
        return _content(message) in self.given

    def saw(self, event: Event) -> bool:
        if self.before.get(event["id"]) == event:
            return True
        return any(m.get("event") == event for m in self.given)

    def touched(self, event_id: int) -> bool:
        return any(m["type"] == "update" and m["event"]["id"] == event_id for m in self.given)


def _content(message: Message) -> Message:
    """A message without what differs between two connections sending the same thing."""
    return {key: value for key, value in message.items() if key not in ("seq", "sentAt")}


def _temporary(error: Exception) -> bool:
    if isinstance(error, APIError):
        return error.code not in FATAL
    return isinstance(error, (OSError, WebSocketException))


def _for_caller(error: Exception) -> Exception:
    if isinstance(error, (OSError, WebSocketException)):
        wrapped = NetworkError(str(error) or type(error).__name__)
        wrapped.__cause__ = error
        return wrapped
    return error


def _backoff(failures: int) -> float:
    return min(0.5 * 2 ** (failures - 1), MAX_BACKOFF) * random.uniform(0.5, 1)
