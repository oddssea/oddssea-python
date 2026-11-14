from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterator
from typing import Any, Concatenate, ParamSpec, Self, TypeVar

from ._client import BASE_URL, AsyncClient
from ._stream import Stream
from .types import Event, Message

P = ParamSpec("P")
T = TypeVar("T")

AsyncMethod = Callable[Concatenate[AsyncClient, P], T]
SyncMethod = Callable[Concatenate["Client", P], T]


# Client's methods are AsyncClient's, each wrapped to wait for its answer.
# The wrappers keep the parameters and swap the return type, so editors and
# type checkers see the same signatures on both clients.

def _blocking(method: AsyncMethod[P, Coroutine[Any, Any, T]]) -> SyncMethod[P, T]:
    @functools.wraps(method)
    def call(self: Client, *args: P.args, **kwargs: P.kwargs) -> T:
        return self._wait(method(self._async, *args, **kwargs))

    return call


def _blocking_iter(method: AsyncMethod[P, AsyncGenerator[T, None]]) -> SyncMethod[P, Iterator[T]]:
    @functools.wraps(method)
    def call(self: Client, *args: P.args, **kwargs: P.kwargs) -> Iterator[T]:
        items = method(self._async, *args, **kwargs)
        while True:
            try:
                yield self._wait(items.__anext__())
            except StopAsyncIteration:
                return

    return call


def _blocking_stream(method: AsyncMethod[P, Stream]) -> SyncMethod[P, SyncStream]:
    @functools.wraps(method)
    def call(self: Client, *args: P.args, **kwargs: P.kwargs) -> SyncStream:
        return SyncStream(method(self._async, *args, **kwargs), self._wait)

    return call


class Client:
    """The oddssea API, without asyncio.

    The same methods as AsyncClient, called without await. The work happens on
    an event loop in a background thread, which close() stops.
    """

    def __init__(self, api_key: str | None = None, *, base_url: str = BASE_URL, timeout: float = 30.0,
                 max_retries: int = 2, max_retry_wait: float = 5.0):
        self._async = AsyncClient(api_key, base_url=base_url, timeout=timeout, max_retries=max_retries,
                                  max_retry_wait=max_retry_wait)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="oddssea", daemon=True)
        self._thread.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        if self._loop.is_closed():
            return
        self._wait(self._async.aclose())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    @property
    def plan(self) -> str | None:
        return self._async.plan

    @property
    def plan_expires(self) -> str | None:
        return self._async.plan_expires

    sports = _blocking(AsyncClient.sports)
    leagues = _blocking(AsyncClient.leagues)
    events = _blocking(AsyncClient.events)
    event = _blocking(AsyncClient.event)
    match = _blocking(AsyncClient.match)
    search = _blocking(AsyncClient.search)
    history = _blocking(AsyncClient.history)
    timeline = _blocking(AsyncClient.timeline)
    history_events = _blocking_iter(AsyncClient.history_events)
    opening_closing = _blocking(AsyncClient.opening_closing)
    drops = _blocking(AsyncClient.drops)
    stream = _blocking_stream(AsyncClient.stream)

    def _wait(self, coroutine: Coroutine[Any, Any, T]) -> T:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result()
        except KeyboardInterrupt:
            future.cancel()
            raise


class SyncStream:
    """Stream for a plain for loop. See Stream for the messages.

        with client.stream("soccer") as stream:
            for message in stream:
                ...
    """

    def __init__(self, stream: Stream, wait: Callable[[Coroutine[Any, Any, Any]], Any]):
        self._stream = stream
        self._wait = wait

    @property
    def events(self) -> dict[int, Event]:
        return self._stream.events

    @property
    def connected(self) -> bool:
        return self._stream.connected

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Message:
        try:
            return self._wait(self._stream.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self._wait(self._stream.close())
